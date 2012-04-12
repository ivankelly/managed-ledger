package com.yahoo.messaging.bookkeeper.ledger;

import java.util.List;

/**
 * A ManangedCursor is a persisted cursor
 * 
 * If the cursor is not marking entries
 * 
 */
public interface ManagedCursor {

    /**
     * 
     * 
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @return the list of entries
     * @throws Exception
     */
    public List<Entry> readEntries(int numberOfEntriesToRead) throws Exception;

    /**
     * Tells whether this cursor has already consumed all the available entries.
     * 
     * @return true if there are pending entries to read, false otherwise
     */
    public boolean hasMoreEntries();

    /**
     * 
     * This signals that the reader is done with all the entries up to "entry"
     * (included). This can potentially trigger a ledger deletion, if all the
     * other cursors are done too with the underlying ledger.
     * 
     * @param entry the last entry that has been successfully processed
     * @throws Exception
     */
    public void markDelete(Entry entry) throws Exception;

}
