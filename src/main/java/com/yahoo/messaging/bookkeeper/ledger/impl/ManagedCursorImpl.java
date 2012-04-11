/**
 * 
 */
package com.yahoo.messaging.bookkeeper.ledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.apache.bookkeeper.client.LedgerEntry;

import com.google.common.base.Objects;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.Position;
import com.yahoo.messaging.bookkeeper.ledger.util.Pair;

/**
 */
class ManagedCursorImpl implements ManagedCursor {

    private final ManagedLedgerImpl ledger;
    private final MetaStore store;
    private final String name;

    private Position acknowledgedPosition;
    private Position readPosition;

    ManagedCursorImpl(ManagedLedgerImpl ledger, MetaStore store, String name, Position position) {
        this.ledger = ledger;
        this.store = store;
        this.name = name;
        this.acknowledgedPosition = position;
        this.readPosition = position;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#readEntries(int)
     */
    @Override
    public List<LedgerEntry> readEntries(int numberOfEntriesToRead) throws Exception {
        checkArgument(numberOfEntriesToRead > 0);

        Pair<List<LedgerEntry>, Position> pair = ledger.readEntries(readPosition,
                numberOfEntriesToRead);
        readPosition = pair.second;
        return pair.first;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#hasMoreEntries()
     */
    @Override
    public boolean hasMoreEntries() {
        return ledger.hasMoreEntries(readPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#acknowledge(org.
     * apache.bookkeeper.client.LedgerEntry)
     */
    @Override
    public void markDelete(LedgerEntry entry) throws Exception {
        checkNotNull(entry);

        acknowledgedPosition = new Position(entry);
        store.updateConsumer(ledger.getName(), name, acknowledgedPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("name", name).add("ackPos", acknowledgedPosition)
                .add("readPos", readPosition).toString();
    }

}
