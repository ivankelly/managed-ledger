/**
 * 
 */
package com.yahoo.messaging.bookkeeper.ledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.yahoo.messaging.bookkeeper.ledger.util.VarArgs.va;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.MarkDeleteCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.ReadEntriesCallback;
import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.Position;
import com.yahoo.messaging.bookkeeper.ledger.util.Pair;

/**
 */
class ManagedCursorImpl implements ManagedCursor {

    private final ManagedLedgerImpl ledger;
    private final String name;

    private Position acknowledgedPosition;
    private Position readPosition;

    ManagedCursorImpl(ManagedLedgerImpl ledger, String name, Position position) throws Exception {
        this.ledger = ledger;
        this.name = name;
        this.acknowledgedPosition = position;
        this.readPosition = new Position(position.getLedgerId(), position.getEntryId() + 1);

        ledger.getStore().updateConsumer(ledger.getName(), name, acknowledgedPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#readEntries(int)
     */
    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws Exception {
        checkArgument(numberOfEntriesToRead > 0);

        Pair<List<Entry>, Position> pair = ledger.readEntries(readPosition, numberOfEntriesToRead);
        readPosition = pair.second;
        return pair.first;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#asyncReadEntries(int,
     * com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.ReadEntriesCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncReadEntries(final int numberOfEntriesToRead,
            final ReadEntriesCallback callback, final Object ctx) {
        ledger.getExecutor().execute(new Runnable() {
            public void run() {
                Exception error = null;
                List<Entry> entries = null;

                try {
                    entries = readEntries(numberOfEntriesToRead);
                } catch (Exception e) {
                    log.warn("[{}] Got exception when reading from cursor: {} {}",
                            va(ledger.getName(), name, e));
                    error = e;
                }

                callback.readEntriesComplete(error, entries, ctx);
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#hasMoreEntries()
     */
    @Override
    public boolean hasMoreEntries() {
        return ledger.hasMoreEntries(readPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#acknowledge(Entry)
     */
    @Override
    public void markDelete(Entry entry) throws Exception {
        checkNotNull(entry);

        log.debug("[{}] Mark delete up to position: {}", ledger.getName(), entry.getPosition());
        acknowledgedPosition = entry.getPosition();
        ledger.getStore().updateConsumer(ledger.getName(), name, acknowledgedPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedCursor#asyncMarkDelete(com
     * .yahoo.messaging.bookkeeper.ledger.Entry,
     * com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.MarkDeleteCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncMarkDelete(final Entry entry, final MarkDeleteCallback callback,
            final Object ctx) {
        ledger.getExecutor().execute(new Runnable() {
            public void run() {
                Exception error = null;

                try {
                    markDelete(entry);
                } catch (Exception e) {
                    log.warn("[{}] Got exception when mark deleting entry: {} {}",
                            va(ledger.getName(), name, e));
                    error = e;
                }

                callback.markDeleteComplete(error, ctx);
            }
        });
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

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorImpl.class);
}
