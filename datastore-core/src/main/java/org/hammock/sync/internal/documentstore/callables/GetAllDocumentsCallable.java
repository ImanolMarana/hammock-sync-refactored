/*
 * Copyright © 2016, 2017 IBM Corp. All rights reserved.
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

package org.hammock.sync.internal.documentstore.callables;

import org.hammock.sync.documentstore.DocumentRevision;
import org.hammock.sync.internal.documentstore.AttachmentStreamFactory;
import org.hammock.sync.internal.documentstore.helpers.GetRevisionsFromRawQuery;
import org.hammock.sync.internal.sqlite.SQLCallable;
import org.hammock.sync.internal.sqlite.SQLDatabase;

import java.util.ArrayList;
import java.util.List;

/**
 * Get all non-deleted winning Revisions of Documents, ordered by Document ID, starting from
 * `offset` and with maximum `limit` results.
 */
public class GetAllDocumentsCallable implements SQLCallable<List<DocumentRevision>> {

    private int offset;
    private int limit;
    private boolean descending;

    private String attachmentsDir;
    private AttachmentStreamFactory attachmentStreamFactory;

    public GetAllDocumentsCallable(int offset, int limit, boolean descending, String
            attachmentsDir, AttachmentStreamFactory attachmentStreamFactory) {
        this.offset = offset;
        this.limit = limit;
        this.descending = descending;
        this.attachmentsDir = attachmentsDir;
        this.attachmentStreamFactory = attachmentStreamFactory;
    }

    @Override
    public List<DocumentRevision> call(SQLDatabase db) throws Exception {
        // Generate the SELECT statement, based on the options:
        String sql = String.format("SELECT " + CallableSQLConstants.FULL_DOCUMENT_COLS +
                " FROM revs, docs WHERE deleted = 0 AND current = 1 AND docs.doc_id = revs.doc_id " +
                " ORDER BY docs.doc_id %1$s, revid DESC LIMIT %2$s OFFSET %3$s ",
                (descending ? "DESC" : "ASC"), limit, offset);

        return new ArrayList<DocumentRevision>(GetRevisionsFromRawQuery.get(db, sql,
                new String[]{}, attachmentsDir, attachmentStreamFactory));

    }
}
