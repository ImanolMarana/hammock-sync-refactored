/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.hammock.sync.internal.query;

import static org.hammock.sync.internal.query.QueryConstants.AND;
import static org.hammock.sync.internal.query.QueryConstants.EQ;
import static org.hammock.sync.internal.query.QueryConstants.EXISTS;
import static org.hammock.sync.internal.query.QueryConstants.GT;
import static org.hammock.sync.internal.query.QueryConstants.GTE;
import static org.hammock.sync.internal.query.QueryConstants.IN;
import static org.hammock.sync.internal.query.QueryConstants.LT;
import static org.hammock.sync.internal.query.QueryConstants.LTE;
import static org.hammock.sync.internal.query.QueryConstants.MOD;
import static org.hammock.sync.internal.query.QueryConstants.NOT;
import static org.hammock.sync.internal.query.QueryConstants.OR;
import static org.hammock.sync.internal.query.QueryConstants.SEARCH;
import static org.hammock.sync.internal.query.QueryConstants.SIZE;
import static org.hammock.sync.internal.query.QueryConstants.TEXT;

import org.hammock.sync.query.FieldSort;
import org.hammock.sync.query.Index;
import org.hammock.sync.query.IndexType;
import org.hammock.sync.internal.util.Misc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  This class translates Cloudant query selectors into the SQL we need to use
 *  to query our indexes.
 *
 *  It creates a tree structure which contains AND/OR nodes, along with the SQL which
 *  needs to be used to get a list of document IDs for each level. This tree structure
 *  is passed back out of the translation process for execution by an interpreter which
 *  can perform the needed AND and OR operations between the document ID sets returned
 *  by the SQL queries.
 *
 *  This merging of results in code allows us to make more intelligent use of indexes
 *  within the SQLite database. As SQLite allows us to use just a single index per query,
 *  performing several queries over indexes and then using set operations works out
 *  more flexible and likely more efficient.
 *
 *  The SQL must be executed separately so we can do it in a transaction so we're doing
 *  it over a consistent view of the index.
 *
 *  The translator is a simple depth-first, recursive decent parser over the selector
 *  map(dictionary).
 *
 *  Some examples:
 *
 *  AND : [ { x: X }, { y: Y } ]
 *
 *  This can be represented by a single SQL query and AND tree node:
 *
 *  AND
 *   |
 *  sql
 *
 *
 *  OR : [ { x: X }, { y: Y } ]
 *
 *  This is a single OR node and two SQL queries:
 *
 *     OR
 *     /\
 *  sql sql
 *
 *  The interpreter then unions the results.
 *
 *
 *  OR : [ { AND : [ { x: X }, { y: Y } ] }, { y: Y } ]
 *
 *  This requires a more complex tree:
 *
 *      OR
 *     / \
 *  AND  sql
 *   |
 *  sql
 *
 *  We could collapse out the extra AND node.
 *
 *
 *  AND : [ { OR : [ { x: X }, { y: Y } ] }, { y: Y } ]
 *
 *  This is really the most complex situation:
 *
 *       AND
 *       / \
 *     OR  sql
 *     /\
 *  sql sql
 *
 *  These basic patterns can be composed into more complicate structures.
 */
class QuerySqlTranslator {

    private static final Logger logger = Logger.getLogger(QuerySqlTranslator.class.getName());

    public static QueryNode translateQuery(Map<String, Object> query,
                                           List<Index> indexes,
                                           Boolean[] indexesCoverQuery) {
        TranslatorState state = new TranslatorState();
        QueryNode node = translateQuery(query, indexes, state);

        Misc.checkState(!state.textIndexMissing, "No text index defined, cannot execute query containing a text search.");
        Misc.checkState(!(state.textIndexRequired && state.atLeastOneIndexMissing), String.format("query %s contains a text search but is missing \"json\"" +
                                       " index(es).  All indexes must exist in order to execute a" +
                                       " query containing a text search.  Create all necessary" +
                                       " indexes for the query and re-execute.",
                                       query.toString()));
        if (!state.textIndexRequired &&
                      (!state.atLeastOneIndexUsed || state.atLeastOneORIndexMissing)) {
            // If we haven't used a single index or an OR clause is missing an index,
            // we need to return every document ID, so that the post-hoc matcher can
            // run over every document to manually carry out the query.
            SqlQueryNode sqlNode = new SqlQueryNode();
            Set<String> neededFields = new HashSet<String>(Collections.singletonList("_id"));
            String allDocsIndex = chooseIndexForFields(neededFields, indexes);

            if (allDocsIndex != null && !allDocsIndex.isEmpty()) {
                String tableName = QueryImpl.tableNameForIndex(allDocsIndex);
                String sql = String.format(Locale.ENGLISH, "SELECT _id FROM \"%s\"", tableName);
                sqlNode.sql = SqlParts.partsForSql(sql, new String[]{});
            }

            AndQueryNode root = new AndQueryNode();
            root.children.add(sqlNode);

            indexesCoverQuery[0] = false;
            return root;
        } else {
            indexesCoverQuery[0] = !state.atLeastOneIndexMissing;
            return node;
        }
    }

    @SuppressWarnings("unchecked")
    private static QueryNode translateQuery(Map<String, Object> query,
                                           List<Index> indexes,
                                           TranslatorState state) {
        // At this point we will have a root compound predicate, AND or OR, and
        // the query will be reduced to a single entry:
        // { "$and": [ ... predicates (possibly compound) ... ] }
        // { "$or": [ ... predicates (possibly compound) ... ] }

        ChildrenQueryNode root = null;
        List<Object> clauses = new ArrayList<Object>();

        if (query.get(AND) != null) {
            clauses = (ArrayList<Object>) query.get(AND);
            root = new AndQueryNode();
        } else if (query.get(OR) != null) {
            clauses = (ArrayList<Object>) query.get(OR);
            root = new OrQueryNode();
        }

        // Compile a list of simple clauses to be handled below. If a text clause is
        // encountered, store it separately from the simple clauses since it will be
        // handled later on its own.
        List<Object> basicClauses = extractBasicClauses(clauses);
        Object textClause = extractTextClause(clauses);

        // Handle the simple "field": { "$operator": "value" } clauses.
        root = handleBasicClauses(query, indexes, state, root, basicClauses);

        // A text clause such as { "$text" : { "$search" : "foo bar baz" } }
        // by nature uses its own text index. It is therefor handled
        // separately from other simple clauses.
        root = handleTextClause(indexes, state, root, textClause);

        // AND and OR subclauses are handled identically whatever the parent is.
        // We go through the query twice to order the OR clauses before the AND
        // clauses, for predictability.

        root = addSubclauses(clauses, indexes, state, root, OR);
        root = addSubclauses(clauses, indexes, state, root, AND);

        return root;
    }

    private static List<Object> extractBasicClauses(List<Object> clauses) {
        List<Object> basicClauses = null;
        for (Object rawClause : clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (!field.startsWith("$")) {
                if (basicClauses == null) {
                    basicClauses = new ArrayList<Object>();
                }
                basicClauses.add(rawClause);
            }
        }
        return basicClauses;
    }

    private static Object extractTextClause(List<Object> clauses) {
        Object textClause = null;
        for (Object rawClause : clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (field.equalsIgnoreCase(TEXT)) {
                textClause = rawClause;
            }
        }
        return textClause;
    }

    private static ChildrenQueryNode handleBasicClauses(Map<String, Object> query, List<Index> indexes, TranslatorState state, ChildrenQueryNode root, List<Object> basicClauses) {
        if (basicClauses != null) {
            if (query.get(AND) != null) {
                root = handleAndBasicClauses(indexes, state, root, basicClauses);
            } else if (query.get(OR) != null) {
                root = handleOrBasicClauses(indexes, state, root, basicClauses);
            }
        }
        return root;
    }

    private static ChildrenQueryNode handleAndBasicClauses(List<Index> indexes, TranslatorState state, ChildrenQueryNode root, List<Object> basicClauses) {
        // For an AND query, we require a single compound index and we generate a
        // single SQL statement to use that index to satisfy the clauses.
        String chosenIndex = chooseIndexForAndClause(basicClauses, indexes);
        if (chosenIndex == null || chosenIndex.isEmpty()) {
            state.atLeastOneIndexMissing = true;
            String msg = String.format("No single index contains all of %s; %s",
                    basicClauses.toString(),
                    "add index for these fields to query efficiently.");
            logger.log(Level.WARNING, msg);
        } else {
            state.atLeastOneIndexUsed = true;

            // Execute SQL on that index with appropriate values
            SqlParts select = selectStatementForAndClause(basicClauses, chosenIndex);

            SqlQueryNode sqlNode = new SqlQueryNode();
            sqlNode.sql = select;

            if (root != null) {
                root.children.add(sqlNode);
            }
        }
        return root;
    }

    private static ChildrenQueryNode handleOrBasicClauses(List<Index> indexes, TranslatorState state, ChildrenQueryNode root, List<Object> basicClauses) {
        // OR nodes require a query for each clause.
        //
        // We want to allow OR clauses to use separate indexes, unlike for AND, to allow
        // users to query over multiple indexes during a single query. This prevents users
        // having to create a single huge index just because one query in their application
        // requires it, slowing execution of all the other queries down.
        //
        // We could optimise for OR parts where we have an appropriate compound index,
        // but we don't for now.

        for (Object basicClause : basicClauses) {
            List<Object> wrappedClause = Arrays.asList(basicClause);
            String chosenIndex = chooseIndexForAndClause(wrappedClause, indexes);
            if (chosenIndex == null || chosenIndex.isEmpty()) {
                state.atLeastOneIndexMissing = true;
                state.atLeastOneORIndexMissing = true;
                String msg = String.format("No single index contains all of %s; %s",
                        basicClauses.toString(),
                        "add index for these fields to query efficiently.");
                logger.log(Level.WARNING, msg);
            } else {
                state.atLeastOneIndexUsed = true;

                // Execute SQL on that index with appropriate values
                SqlParts select = selectStatementForAndClause(wrappedClause, chosenIndex);

                SqlQueryNode sqlNode = new SqlQueryNode();
                sqlNode.sql = select;

                if (root != null) {
                    root.children.add(sqlNode);
                }
            }
        }
        return root;
    }

    private static ChildrenQueryNode handleTextClause(List<Index> indexes, TranslatorState state, ChildrenQueryNode root, Object textClause) {
        if (textClause != null) {
            state.textIndexRequired = true;
            String textIndex = getTextIndex(indexes);
            if (textIndex == null || textIndex.isEmpty()) {
                state.textIndexMissing = true;
            } else {
                SqlParts select = selectStatementForTextClause(textClause, textIndex);

                SqlQueryNode sqlNode = new SqlQueryNode();
                sqlNode.sql = select;

                if (root != null) {
                    root.children.add(sqlNode);
                }
            }
        }
        return root;
    }

    private static ChildrenQueryNode addSubclauses(List<Object> clauses, List<Index> indexes, TranslatorState state, ChildrenQueryNode root, String clauseType) {
        for (Object rawClause : clauses) {
            Map<String, Object> clause = (Map<String, Object>) rawClause;
            String field = (String) clause.keySet().toArray()[0];
            if (field.equals(clauseType)) {
                QueryNode subNode = translateQuery(clause, indexes, state);
                if (root != null) {
                    root.children.add(subNode);
                }
            }
        }
        return root;
    }
    
    private static List<String> fieldsForAndClause(List<Object> clause) {
        Misc.checkNotNull(clause, "clause");

        List<String> fieldNames = new ArrayList<String>();
        for (Object rawTerm: clause) {
            @SuppressWarnings("unchecked")
            Map<String, Object> term = (Map<String, Object>) rawTerm;
            if (term.size() == 1) {
                fieldNames.add((String) term.keySet().toArray()[0]);
            }
        }

        return fieldNames;
    }

    /*
     * Checks for the existence of an operator in a query clause list
     */
    protected static boolean isOperatorFoundInClause(String operator, List<Object> clause) {
        // first check for the existence of $not so that we can find negated operators:
        // if we find $not then recurse with the value associated with it in the query map
        boolean found = isOperatorFoundInClauseWithNot(operator, clause);

        // if we didn't find $not then look directly for the operator
        if (!found) {
            found = isOperatorFoundDirectly(operator, clause);
        }

        return found;
    }

    private static boolean isOperatorFoundInClauseWithNot(String operator, List<Object> clause) {
        for (Object rawTerm : clause) {
            if (rawTerm instanceof Map) {
                Map term = (Map) rawTerm;
                if (term.size() == 1 && term.values().toArray()[0] instanceof Map) {
                    Map predicate = (Map) term.values().toArray()[0];
                    if (predicate.get(NOT) != null && predicate.get(NOT) instanceof Map) {
                        return isOperatorFoundInClause(operator, Collections
                                .<Object>singletonList(predicate));
                    }
                }
            }
        }
        return false;
    }

    private static boolean isOperatorFoundDirectly(String operator, List<Object> clause) {
        for (Object rawTerm : clause) {
            if (rawTerm instanceof Map) {
                Map term = (Map) rawTerm;
                if (term.size() == 1 && term.values().toArray()[0] instanceof Map) {
                    Map predicate = (Map) term.values().toArray()[0];
                    if (predicate.get(operator) != null) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected static String chooseIndexForAndClause(List<Object> clause,
                                                    List<Index>indexes) {

        if (clause == null || clause.isEmpty()) {
            return null;
        }

        if (indexes == null || indexes.isEmpty()) {
            return null;
        }

        // NB this is not an error condition, but no index will be used
        if (isOperatorFoundInClause(SIZE, clause)) {
            String msg = String.format("$size operator found in clause %s.  " +
                    "Indexes are not used with $size operations.", clause);
            logger.log(Level.INFO, msg);
            return null;
        }

        List<String> fieldList = fieldsForAndClause(clause);
        Set<String> neededFields = new HashSet<String>(fieldList);

        Misc.checkState(!neededFields.isEmpty(), String.format("Invalid clauses in $and clause %s.", clause.toString()));

        return chooseIndexForFields(neededFields, indexes);
    }

    protected static String chooseIndexForFields(Set<String> neededFields,
                                                 List<Index> indexes) {
        String chosenIndex = null;
        for (Index index : indexes) {

            // Don't choose a text index for a non-text query clause
            IndexType indexType = index.indexType;
            if (indexType == IndexType.TEXT) {
                continue;
            }

            Set<String> providedFields = new HashSet<String>();
            for (FieldSort f : index.fieldNames) {
                providedFields.add(f.field);
            }

            if (providedFields.containsAll(neededFields)) {
                chosenIndex = index.indexName;
                break;
            }
        }

        return chosenIndex;
    }

    private static String getTextIndex(List<Index> indexes) {
        String textIndex = null;
        for (Index index : indexes) {
            IndexType indexType = index.indexType;
            if (indexType == IndexType.TEXT) {
                textIndex = index.indexName;
            }
        }

        return textIndex;
    }

    protected static SqlParts selectStatementForAndClause(List<Object> clause,
                                                          String indexName) {

        Misc.checkArgument(!(clause == null || clause.isEmpty()), "clause cannot be null or empty");

        Misc.checkNotNullOrEmpty(indexName, "indexName");

        SqlParts where = whereSqlForAndClause(clause, indexName);

        Misc.checkNotNull(where, "where");

        String tableName = QueryImpl.tableNameForIndex(indexName);

        String sql = String.format(Locale.ENGLISH,
                                   "SELECT _id FROM \"%s\" WHERE %s",
                                   tableName,
                                   where.sqlWithPlaceHolders);
        return SqlParts.partsForSql(sql, where.placeHolderValues);
    }

    @SuppressWarnings("unchecked")
    protected static SqlParts selectStatementForTextClause(Object clause,
                                                           String indexName) {

        Misc.checkNotNull(clause, "clause");

        Misc.checkNotNullOrEmpty(indexName, "indexName");

        Misc.checkArgument(clause instanceof Map, "clause must be a Map");

        Map<String, Object> textClause = (Map<String, Object>) clause;
        Map<String, String> searchClause = (Map<String, String>) textClause.get(TEXT);

        String tableName = QueryImpl.tableNameForIndex(indexName);
        String search = searchClause.get(SEARCH);
        search = search.replace("'", "''");

        String sql = String.format(Locale.ENGLISH, "SELECT _id FROM \"%s\" WHERE \"%s\" MATCH ?", tableName, tableName);
        return SqlParts.partsForSql(sql, new String[]{ search });
    }

    @SuppressWarnings("unchecked")
    protected static SqlParts whereSqlForAndClause(List<Object> clause, String indexName) {
    Misc.checkArgument(!(clause == null || clause.isEmpty()), "clause cannot be null or empty");

    List<String> whereClauses = new ArrayList<>();
    List<Object> sqlParameters = new ArrayList<>() {
        @Override
        public boolean add(Object o) {
            return o instanceof Boolean ? super.add(((Boolean) o) ? "1" : "0") : super.add(String.valueOf(o));
        }
    };

    Map<String, String> operatorMap = new HashMap<>();
    operatorMap.put(EQ, "=");
    operatorMap.put(GT, ">");
    operatorMap.put(GTE, ">=");
    operatorMap.put(LT, "<");
    operatorMap.put(LTE, "<=");
    operatorMap.put(IN, "IN");
    operatorMap.put(MOD, "%");

    for (Object rawComponent : clause) {
        Map<String, Object> component = (Map<String, Object>) rawComponent;
        Misc.checkState(component.size() == 1, String.format("Expected single predicate per clause map, got %s", component));

        String fieldName = (String) component.keySet().toArray()[0];
        Map<String, Object> predicate = (Map<String, Object>) component.get(fieldName);

        Misc.checkState(predicate.size() == 1, String.format("Expected single operator per predicate map, got %s", predicate));

        String operator = (String) predicate.keySet().toArray()[0];

        if (operator.equals(NOT)) {
            processNotOperator(predicate, whereClauses, sqlParameters, indexName, operatorMap);
        } else {
            processOperator(predicate, whereClauses, sqlParameters, fieldName, operator, operatorMap);
        }
    }

    String where = Misc.join(" AND ", whereClauses);
    String[] parameterArray = sqlParameters.stream().map(String::valueOf).toArray(String[]::new);

    return SqlParts.partsForSql(where, parameterArray);
}

private static void processNotOperator(Map<String, Object> predicate, List<String> whereClauses, 
                                       List<Object> sqlParameters, String indexName,
                                       Map<String, String> operatorMap) {
    Map<String, Object> negatedPredicate = (Map<String, Object>) predicate.get(NOT);
    Misc.checkState(negatedPredicate.size() == 1, String.format("Expected single operator per predicate map, got %s", predicate));
    String operator = (String) negatedPredicate.keySet().toArray()[0];

    if (operator.equals(EXISTS)) {
        boolean exists = !((Boolean) negatedPredicate.get(operator));
        whereClauses.add(convertExistsToSqlClauseForFieldName(((String) predicate.keySet().toArray()[0]), exists));
    } else {
        String whereClause;
        String sqlOperator = operatorMap.get(operator);
        String tableName = QueryImpl.tableNameForIndex(indexName);
        String placeholder = createPlaceholder(negatedPredicate.get(operator), sqlParameters, operatorMap);
        String fieldName = (String) predicate.keySet().toArray()[0];
        whereClause = whereClauseForNot(fieldName, sqlOperator, tableName, placeholder);
        whereClauses.add(whereClause);
    }
}

private static void processOperator(Map<String, Object> predicate, List<String> whereClauses, List<Object> sqlParameters,
                                    String fieldName, String operator, Map<String, String> operatorMap) {
    if (operator.equals(EXISTS)) {
        boolean exists = (Boolean) predicate.get(operator);
        whereClauses.add(convertExistsToSqlClauseForFieldName(fieldName, exists));
    } else {
        String sqlOperator = operatorMap.get(operator);
        String placeholder = createPlaceholder(predicate.get(operator), sqlParameters, operatorMap);

        String whereClause = String.format("\"%s\" %s %s", fieldName, sqlOperator, placeholder);
        whereClauses.add(whereClause);
    }
}


private static String createPlaceholder(Object value, List<Object> sqlParameters, Map<String, String> operatorMap) {
    if (IN.equals(operatorMap.get(IN)) && value instanceof List) {
        return placeholdersForInList((List<Object>) value, sqlParameters);
    } else if (MOD.equals(operatorMap.get(MOD)) && value instanceof List) {
         List<Integer> modulus = (List<Integer>) value;
        String placeholder = String.format("? %s CAST(? AS INTEGER)", operatorMap.get(EQ));
        sqlParameters.add(modulus.get(0));
        sqlParameters.add(modulus.get(1));
        return placeholder;
    } else {
        sqlParameters.add(value);
        return "?";
    }
}



// ... other methods (whereClauseForNot, convertExistsToSqlClauseForFieldName, placeholdersForInList) remain unchanged


//Refactoring end

    private static String placeholdersForInList(List<Object> values, List<Object> sqlParameters) {
        List<String> inOperands = new ArrayList<String>();
        for (Object value : values) {
            inOperands.add("?");
            sqlParameters.add(String.valueOf(value));
        }

        return String.format("( %s )", Misc.join(", ", inOperands));
    }

    /**
     * WHERE clause representation of $not must be handled by using a
     * sub-SELECT statement of the operator which is then applied to
     * _id NOT IN (...).  This is because this process is the only
     * way that we can ensure that documents that contain arrays are
     * handled correctly.
     *
     * @param fieldName the field to be NOT-ted
     * @param sqlOperator the SQL operator used in the sub-SELECT
     * @param tableName the chosen table index
     * @return the NOT-ted WHERE clause for the fieldName and sqlOperator
     */
    private static String whereClauseForNot(String fieldName,
                                            String sqlOperator,
                                            String tableName,
                                            String operand) {
        String whereForSubSelect = String.format(Locale.ENGLISH, "\"%s\" %s %s", fieldName, sqlOperator, operand);
        String subSelect = String.format(Locale.ENGLISH, "SELECT _id FROM \"%s\" WHERE %s",
                                         tableName,
                                         whereForSubSelect);

        return String.format("_id NOT IN (%s)", subSelect);
    }

    private static String convertExistsToSqlClauseForFieldName(String fieldName, boolean exists) {
        String sqlClause;
        if (exists) {
            // so this field needs to exist
            sqlClause = String.format("(\"%s\" IS NOT NULL)", fieldName);
        } else {
            // must not exist
            sqlClause = String.format("(\"%s\" IS NULL)", fieldName);
        }

        return sqlClause;
    }

}
