package com.yahoo.messaging.bookkeeper.ledger;

import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.AddEntryCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.CloseCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenCursorCallback;

/**
 * A ManagedLedger it's a superset of a BookKeeper ledger concept. These are the
 * differences :
 * 
 * <ul>
 * <li>ManagedLedger has a unique name by which it can be created/reopened</li>
 * <li>xxx</li>
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
     *            to be added to the managed ledger
     */
    public void addEntry(byte[] data) throws Exception;

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

    public void asyncOpenCursor(String name, OpenCursorCallback callback, Object ctx);

    /**
     * Get the total number of entries for this managed ledger.
     * <p>
     * This is defined by the number of entries in all the BookKeeper ledgers
     * that are being maintained by this ManagedLedger.
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
     * 
     * @return total size in bytes
     */
    public long getTotalSize();

    /**
     * Close the current virtual ledger.
     * <p>
     * This will close all the underlying BookKeeper ledgers. All the
     * ManagedCursors associated will be invalidated.
     * 
     */
    public void close() throws Exception;

    public void asyncClose(CloseCallback callback, Object ctx);
}
