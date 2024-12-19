/*
 * Copyright © 2014, 2017 IBM Corp. All rights reserved.
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

import org.hammock.sync.documentstore.Database;
import org.hammock.sync.internal.documentstore.DatabaseImpl;
import org.hammock.sync.internal.query.callables.CreateIndexCallable;
import org.hammock.sync.internal.query.callables.ListIndexesCallable;
import org.hammock.sync.internal.sqlite.SQLDatabaseFactory;
import org.hammock.sync.internal.sqlite.SQLDatabaseQueue;
import org.hammock.sync.internal.util.Misc;
import org.hammock.sync.query.FieldSort;
import org.hammock.sync.query.Index;
import org.hammock.sync.query.IndexType;
import org.hammock.sync.query.QueryException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Handles creating indexes for a given DocumentStore.
 */
class IndexCreator {

    private final static String GENERATED_INDEX_NAME_PREFIX = "com.cloudant.sync.query.GeneratedIndexName.";

    private final Database database;

    private final SQLDatabaseQueue queue;

    private static final Logger logger = Logger.getLogger(IndexCreator.class.getName());

    public IndexCreator(Database database, SQLDatabaseQueue queue) {
        this.database = database;
        this.queue = queue;
    }

    protected static Index ensureIndexed(Index index,
                                          Database database,
                                          SQLDatabaseQueue queue) throws QueryException {
        IndexCreator executor = new IndexCreator(database, queue);

        return executor.ensureIndexed(index);
    }

    /**
     *  Add a single, possibly compound index for the given field names and ensure all indexing
     *  constraints are met.
     *
     *  This function generates a name for the new index.
     *
     *  @param proposedIndex The object that defines an index.  Includes field list, name, type and options.
     *  @return name of created index
     */
    private Index ensureIndexed(Index proposedIndex) throws QueryException {

        validateFieldSort(proposedIndex);
        validateIndexType(proposedIndex);
    
        List<FieldSort> fieldNamesList = prepareFieldNames(proposedIndex.fieldNames);
        proposedIndex = generateIndexNameIfNeeded(proposedIndex);
    
        List<Index> existingIndexes = getExistingIndexes();
        return checkExistingIndexesAndCreate(proposedIndex, fieldNamesList, existingIndexes);
    }
    
    private void validateFieldSort(Index proposedIndex) {
        for (FieldSort fs : proposedIndex.fieldNames) {
            if (fs.sort == FieldSort.Direction.DESCENDING) {
                throw new UnsupportedOperationException("Indexes with Direction.DESCENDING are " +
                        "not supported. To return data in descending order, create an index with " +
                        "Direction.ASCENDING fields and execute the subsequent query with " +
                        "Direction.DESCENDING fields as required.");
            }
        }
    }
    
    private void validateIndexType(Index proposedIndex) throws QueryException {
        if (proposedIndex.indexType == IndexType.TEXT && !SQLDatabaseFactory.FTS_AVAILABLE) {
            String message = "Text search not supported.  To add support for text " +
                    "search, enable FTS compile options in SQLite.";
            logger.log(Level.SEVERE, message);
            throw new QueryException(message);
        }
    }
    
    private List<FieldSort> prepareFieldNames(List<FieldSort> fieldNamesList) {
        Set<String> uniqueNames = new HashSet<String>();
        for (FieldSort fieldName : fieldNamesList) {
            uniqueNames.add(fieldName.field);
            Misc.checkArgument(validFieldName(fieldName.field), "Field " + fieldName.field + " is not valid");
        }
    
        // Check there are no duplicate field names in the array
        Misc.checkArgument(uniqueNames.size() == fieldNamesList.size(), String.format("Cannot create index with duplicated field names %s"
                , fieldNamesList));
    
    
        if (!uniqueNames.contains("_rev")) {
            fieldNamesList.add(0, new FieldSort("_rev"));
        }
    
        if (!uniqueNames.contains("_id")) {
            fieldNamesList.add(0, new FieldSort("_id"));
        }
        return fieldNamesList;
    }
    
    
    
    private Index generateIndexNameIfNeeded(Index proposedIndex) {
        if (proposedIndex.indexName == null) {
            // generate a name for the index.
            String indexName = GENERATED_INDEX_NAME_PREFIX + proposedIndex.toString();
            // copy over definition of existing proposed index and create it with this name
            proposedIndex = new Index(proposedIndex.fieldNames,
                    indexName,
                    proposedIndex.indexType,
                    proposedIndex.tokenizer);
        }
        return proposedIndex;
    }
    
    
    private List<Index> getExistingIndexes() throws QueryException {
        try {
            return DatabaseImpl.get(this.queue.submit(new ListIndexesCallable()));
        } catch (ExecutionException e) {
            String msg = "Failed to list indexes";
            logger.log(Level.SEVERE, msg, e);
            throw new QueryException(msg, e);
        }
    }
    
    private Index checkExistingIndexesAndCreate(Index proposedIndex, List<FieldSort> fieldNamesList, List<Index> existingIndexes) throws QueryException {
        for (Index existingIndex : existingIndexes) {
            if (proposedIndex.indexType == IndexType.TEXT && existingIndex.indexType == IndexType.TEXT) {
                String msg = String.format("Text index limit reached. There is a limit of one " +
                                "text index per database. There is an existing text index in this " +
                                "database called \"%s\".",
                        existingIndex.indexName);
                logger.log(Level.SEVERE, msg, existingIndex.indexName);
                throw new QueryException(msg);
            }
    
            if (existingIndex.indexName.equals(proposedIndex.indexName)) {
                return handleExistingIndexSameName(proposedIndex, existingIndex, fieldNamesList);
    
            }
    
            Index compare = new Index(proposedIndex.fieldNames, existingIndex.indexName, proposedIndex.indexType, proposedIndex.tokenizer);
            if (compare.equals(existingIndex)) {
                 return handleExistingIndexSameDefinition(existingIndex, fieldNamesList);
            }
        }
    
        return createNewIndex(proposedIndex, fieldNamesList);
    }
    
    
    private Index handleExistingIndexSameName(Index proposedIndex, Index existingIndex, List<FieldSort> fieldNamesList) throws QueryException {
        if (existingIndex.equals(proposedIndex)) {
            logger.fine(String.format("Index with name \"%s\" already exists with same " +
                    "definition", proposedIndex.indexName));
    
            IndexUpdater.updateIndex(existingIndex.indexName, fieldNamesList, database, queue);
            return existingIndex;
        } else {
            throw new QueryException(String.format("Index with name \"%s\" already exists" +
                    " but has different definition to requested index", proposedIndex.indexName));
        }
    }
    
    
    private Index handleExistingIndexSameDefinition(Index existingIndex, List<FieldSort> fieldNamesList) throws QueryException{
            logger.fine(String.format("Index with name \"%s\" exists which has same " +
                    "definition of requested index \"%s\"",
                    existingIndex.indexName));
    
            IndexUpdater.updateIndex(existingIndex.indexName, fieldNamesList, database, queue);
            return existingIndex;
    }
    
    private Index createNewIndex(Index index, List<FieldSort> fieldNamesList) throws QueryException {
        Future<Void> result = queue.submitTransaction(new CreateIndexCallable(fieldNamesList, index));
    
        try {
            result.get();
        } catch (ExecutionException e) {
            String message = "Execution error encountered whilst inserting index metadata";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        } catch (InterruptedException e) {
            String message = "Execution interrupted error encountered whilst inserting index metadata";
            logger.log(Level.SEVERE, message, e);
            throw new QueryException(message, e);
        }
    
        IndexUpdater.updateIndex(index.indexName, fieldNamesList, database, queue);
        return index;
    
    }
    /**
     *  Validate the field name string is usable.
     *
     *  The only restriction so far is that the parts don't start with
     *  a $ sign, as this makes the query language ambiguous.
     */
    protected static boolean validFieldName(String fieldName) {
        String[] parts = fieldName.split("\\.");
        for (String part: parts) {
            if (part.startsWith("$")) {
                String msg = String.format("Field names cannot start with a $ in field %s", part);
                logger.log(Level.SEVERE, msg);
                return false;
            }
        }

        return true;
    }

}
