/**
 * Copyright (C) 2012 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.messaging.bookkeeper.ledger;

import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.AddEntryCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.CloseCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenCursorCallback;

/**
 * A ManagedLedger it's a superset of a BookKeeper ledger concept.
 * <p>
 * It mimics the concept of an appender log that:
 * 
 * <ul>
 * <li>has a unique name (chosen by clients) by which it can be
 * created/opened/deleted</li>
 * <li>is always writable: if a writer process crashes, a new writer can re-open
 * the ManagedLedger and continue writing into it</li>
 * <li>has multiple persisted consumers (see {@link ManagedCursor}), each of
 * them with an associated position</li>
 * <li>when all the consumers have processed all the entries contained in a
 * Bookkeeper ledger, the ledger is deleted</li>
 * </ul>
 * <p>
 * Caveats:
 * <ul>
 * <li>A single ManagedLedger can only be open once at any time. Implementation
 * can protect double access from the same VM, but accesses from different
 * machines to the same ManagedLedger need to be avoided through an external
 * source of coordination.</li>
 * </ul>
 */
public interface ManagedLedger {

    /**
     * @return the unique name of this ManagedLedger
     */
    public String getName();

    /**
     * Append a new entry to the end of a managed ledger.
     * 
     * @param data
     *            data entry to be persisted
     * @return the Position at which the entry has been inserted
     * @throws Exception
     */
    public Position addEntry(byte[] data) throws Exception;

    /**
     * Append a new entry asynchronously
     * 
     * @see #addEntry(byte[])
     * @param data
     *            data entry to be persisted
     * 
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncAddEntry(byte[] data, AddEntryCallback callback, Object ctx);

    /**
     * Open a ManagedCursor in this ManagedLedger.
     * <p>
     * If the cursors doesn't exist, a new one will be created and its position
     * will be at the end of the ManagedLedger.
     * 
     * @param name
     *            the name associated with the ManagedCursor
     * @return the ManagedCursor
     * @throws Exception
     */
    public ManagedCursor openCursor(String name) throws Exception;

    /**
     * Open a ManagedCursor asynchronously.
     * 
     * @see #openCursor(String)
     * @param name
     *            the name associated with the ManagedCursor
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncOpenCursor(String name, OpenCursorCallback callback, Object ctx);

    /**
     * Get the total number of entries for this managed ledger.
     * <p>
     * This is defined by the number of entries in all the BookKeeper ledgers
     * that are being maintained by this ManagedLedger.
     * <p>
     * This method is non-blocking.
     * 
     * @return the number of entries
     */
    public long getNumberOfEntries();

    /**
     * Get the total sizes in bytes of the managed ledger, without accounting
     * for replicas.
     * <p>
     * This is defined by the sizes of all the BookKeeper ledgers that are being
     * maintained by this ManagedLedger.
     * <p>
     * This method is non-blocking.
     * 
     * @return total size in bytes
     */
    public long getTotalSize();

    /**
     * Close the ManagedLedger.
     * <p>
     * This will close all the underlying BookKeeper ledgers. All the
     * ManagedCursors associated will be invalidated.
     * 
     * @throws Exception
     */
    public void close() throws Exception;

    /**
     * Close the ManagedLedger asynchronously.
     * 
     * @see #close()
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncClose(CloseCallback callback, Object ctx);
}
