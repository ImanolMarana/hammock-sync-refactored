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

class SqlParts {

    public String sqlWithPlaceHolders;
    public String[] placeHolderValues;

    public static SqlParts partsForSql(String sql, String[] parameters) {
        SqlParts parts = new SqlParts();
        parts.sqlWithPlaceHolders = sql;
        parts.placeHolderValues = parameters;

        return parts;
    }

}
