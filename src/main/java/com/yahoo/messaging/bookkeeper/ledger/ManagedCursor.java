package com.yahoo.messaging.bookkeeper.ledger;

import java.util.List;

import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.MarkDeleteCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.ReadEntriesCallback;

/**
 * A ManangedCursor is a persisted cursor inside a ManagedLedger.
 * <p>
 * The ManagedCursor is used to read from the ManagedLedger and to signal when
 * the consumer is done with the messages that it has read before.
 */
public interface ManagedCursor {

    /**
     * Read entries from the ManagedLedger, up to the specified number. The
     * returned list can be smaller.
     * 
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @return the list of entries
     * @throws Exception
     */
    public List<Entry> readEntries(int numberOfEntriesToRead) throws Exception;

    /**
     * Asynchronously read entries from the ManagedLedger.
     * 
     * @see #readEntries(int)
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx);

    /**
     * Tells whether this cursor has already consumed all the available entries.
     * <p>
     * This method is not blocking.
     * 
     * @return true if there are pending entries to read, false otherwise
     */
    public boolean hasMoreEntries();

    /**
     * This signals that the reader is done with all the entries up to
     * "position" (included). This can potentially trigger a ledger deletion, if
     * all the other cursors are done too with the underlying ledger.
     * 
     * @param position
     *            the last position that have been successfully consumed
     * @throws Exception
     */
    public void markDelete(Position position) throws Exception;

    /**
     * Asynchronous mark delete
     * 
     * @see #markDelete(Position)
     * @param position
     *            the last position that have been successfully consumed
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx);

}
