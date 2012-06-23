/**
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
/**
 * 
 */
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.util.VarArgs.va;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 */
@ThreadSafe
class ManagedCursorImpl implements ManagedCursor {

    private final ManagedLedgerImpl ledger;
    private final String name;

    private final AtomicReference<Position> acknowledgedPosition = new AtomicReference<Position>();
    private final AtomicReference<Position> readPosition = new AtomicReference<Position>();

    ManagedCursorImpl(ManagedLedgerImpl ledger, String name, Position position) throws InterruptedException,
            ManagedLedgerException {
        this.ledger = ledger;
        this.name = name;
        this.acknowledgedPosition.set(position);

        // The read position has to ahead of the acknowledged position, by at
        // least 1, since it refers to the next entry that has to be read.
        this.readPosition.set(new Position(position.getLedgerId(), position.getEntryId() + 1));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#readEntries(int)
     */
    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        checkArgument(numberOfEntriesToRead > 0);

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException status;
            List<Entry> entries;
        }

        final Result result = new Result();

        asyncReadEntries(numberOfEntriesToRead, new ReadEntriesCallback() {
            public void readEntriesComplete(Throwable status, List<Entry> entries, Object ctx) {
                // TODO: Remove casting
                result.status = (ManagedLedgerException) status;
                result.entries = entries;
                counter.countDown();
            }
        }, null);

        counter.await();

        if (result.status != null)
            throw result.status;

        return result.entries;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#asyncReadEntries(int,
     * org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncReadEntries(final int numberOfEntriesToRead, final ReadEntriesCallback callback, final Object ctx) {
        checkArgument(numberOfEntriesToRead > 0);

        OpReadEntry op = new OpReadEntry(this, readPosition.get(), numberOfEntriesToRead, callback, ctx);
        ledger.asyncReadEntries(op);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#hasMoreEntries()
     */
    @Override
    public boolean hasMoreEntries() {
        return ledger.hasMoreEntries(readPosition.get());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#getNumberOfEntries()
     */
    @Override
    public long getNumberOfEntries() {
        return ledger.getNumberOfEntries(readPosition.get());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#acknowledge(Position)
     */
    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        checkNotNull(position);

        log.debug("[{}] Mark delete up to position: {}", ledger.getName(), position);
        ledger.updateCursor(this, position);
    }

    protected void setAcknowledgedPosition(Position newPosition) {
        acknowledgedPosition.set(newPosition);

        Position currentRead = readPosition.get();
        if (newPosition.compareTo(currentRead) >= 0) {
            // If the position that is markdeleted is past the read position, it
            // means that the client has skipped some entries. We need to move
            // read position forward
            readPosition.compareAndSet(currentRead, new Position(newPosition.getLedgerId(),
                    newPosition.getEntryId() + 1));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#asyncMarkDelete(com
     * .yahoo.messaging.bookkeeper.ledger.Position,
     * org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncMarkDelete(final Position position, final MarkDeleteCallback callback, final Object ctx) {
        ledger.getExecutor().execute(new Runnable() {
            public void run() {
                Exception error = null;

                try {
                    markDelete(position);
                } catch (Exception e) {
                    log.warn("[{}] Got exception when mark deleting entry: {} {}", va(ledger.getName(), name, e));
                    error = e;
                }

                callback.markDeleteComplete(error, ctx);
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public synchronized String toString() {
        return Objects.toStringHelper(this).add("name", name).add("ackPos", acknowledgedPosition)
                .add("readPos", readPosition).toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#getName()
     */
    @Override
    public String getName() {
        return name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#getReadPosition()
     */
    @Override
    public Position getReadPosition() {
        return readPosition.get();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#getMarkDeletedPosition
     * ()
     */
    @Override
    public Position getMarkDeletedPosition() {
        return acknowledgedPosition.get();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#skip(int)
     */
    @Override
    public void skip(int entries) {
        checkArgument(entries > 0);
        readPosition.set(ledger.skipEntries(readPosition.get(), entries));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#seek(com.yahoo.messaging
     * .bookkeeper.ledger.Position)
     */
    @Override
    public void seek(Position newReadPosition) {
        checkArgument(newReadPosition.compareTo(acknowledgedPosition.get()) > 0,
                "new read position must be greater than or equal to the mark deleted position for this cursor");

        checkArgument(ledger.isValidPosition(newReadPosition), "new read position is not valid for this managed ledger");
        readPosition.set(newReadPosition);
    }

    /**
     * Internal version of seek that doesn't do the validation check
     * 
     * @param newReadPosition
     */
    protected void setReadPosition(Position newReadPosition) {
        checkArgument(newReadPosition != null);
        readPosition.set(newReadPosition);
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorImpl.class);
}
