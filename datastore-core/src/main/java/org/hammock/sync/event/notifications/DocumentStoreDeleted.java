/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2015 Cloudant, Inc. All rights reserved.
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

package org.hammock.sync.event.notifications;

import org.hammock.sync.documentstore.DocumentStore;

/**
 * <p>Event for database deleted.</p>
 *
 * <p>This event is posted by
 * {@link DocumentStore#delete()}</p>
 */
public class DocumentStoreDeleted extends DocumentStoreModified {

    /**
     * Event for DocumentStore deleted.
     * 
     * @param dbName
     *            The name of the DocumentStore that was deleted
     */
    public DocumentStoreDeleted(String dbName) {
        super(dbName);
    }
}
