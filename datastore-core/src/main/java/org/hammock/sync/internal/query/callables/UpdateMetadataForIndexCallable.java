/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 */

package org.hammock.sync.internal.query.callables;

import org.hammock.sync.internal.android.ContentValues;
import org.hammock.sync.internal.query.QueryConstants;
import org.hammock.sync.internal.sqlite.SQLCallable;
import org.hammock.sync.internal.sqlite.SQLDatabase;
import org.hammock.sync.query.QueryException;

/**
 * Updates the Metadata for an index in the database.
 */
public class UpdateMetadataForIndexCallable implements SQLCallable<Void> {
    private final long lastSequence;
    private final String indexName;

    public UpdateMetadataForIndexCallable(long lastSequence, String indexName) {
        this.lastSequence = lastSequence;
        this.indexName = indexName;
    }

    @Override
    public Void call(SQLDatabase database) throws QueryException {
        ContentValues v = new ContentValues();
        v.put("last_sequence", lastSequence);
        int row = database.update(QueryConstants.INDEX_METADATA_TABLE_NAME,
                                  v,
                                  " index_name = ? ",
                                  new String[]{indexName});
        if (row <= 0) {
            throw new QueryException("Failed to update index metadata for index " + indexName);
        }
        return null;
    }
}
