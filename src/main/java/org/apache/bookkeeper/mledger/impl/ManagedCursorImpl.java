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
/**
 * 
 */
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.util.VarArgs.va;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 */
@ThreadSafe
class ManagedCursorImpl implements ManagedCursor {

    private final ManagedLedgerImpl ledger;
    private final String name;

    private Position acknowledgedPosition;
    private Position readPosition;

    ManagedCursorImpl(ManagedLedgerImpl ledger, String name, Position position) throws Exception {
        this.ledger = ledger;
        this.name = name;
        this.acknowledgedPosition = position;

        // The read position has to ahead of the acknowledged position, by at
        // least 1, since it refers to the next entry that has to be read.
        this.readPosition = new Position(position.getLedgerId(), position.getEntryId() + 1);

        ledger.getStore().updateConsumer(ledger.getName(), name, acknowledgedPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#readEntries(int)
     */
    @Override
    public synchronized List<Entry> readEntries(int numberOfEntriesToRead) throws Exception {
        checkArgument(numberOfEntriesToRead > 0);

        Position current = readPosition;
        Pair<List<Entry>, Position> pair = ledger.readEntries(current, numberOfEntriesToRead);

        Position newPosition = pair.second;
        readPosition = newPosition;
        return pair.first;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.mledger.ManagedCursor#asyncReadEntries(int,
     * org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncReadEntries(final int numberOfEntriesToRead, final ReadEntriesCallback callback, final Object ctx) {
        ledger.getExecutor().execute(new Runnable() {
            public void run() {
                Exception error = null;
                List<Entry> entries = null;

                try {
                    entries = readEntries(numberOfEntriesToRead);
                } catch (Exception e) {
                    log.warn("[{}] Got exception when reading from cursor: {} {}", va(ledger.getName(), name, e));
                    error = e;
                }

                callback.readEntriesComplete(error, entries, ctx);
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#hasMoreEntries()
     */
    @Override
    public synchronized boolean hasMoreEntries() {
        return ledger.hasMoreEntries(readPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.mledger.ManagedCursor#getNumberOfEntries()
     */
    @Override
    public synchronized long getNumberOfEntries() {
        return ledger.getNumberOfEntries(readPosition);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.mledger.ManagedCursor#acknowledge(Position)
     */
    @Override
    public synchronized void markDelete(Position position) throws Exception {
        checkNotNull(position);

        log.debug("[{}] Mark delete up to position: {}", ledger.getName(), position);
        ledger.updateCursor(this, position);
    }

    protected synchronized void setAcknowledgedPosition(Position newPosition) {
        acknowledgedPosition = newPosition;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.mledger.ManagedCursor#asyncMarkDelete(com
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
     * @see
     * org.apache.bookkeeper.mledger.ManagedCursor#getReadPosition()
     */
    @Override
    public synchronized Position getReadPosition() {
        return readPosition;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.mledger.ManagedCursor#getMarkDeletedPosition
     * ()
     */
    @Override
    public synchronized Position getMarkDeletedPosition() {
        return acknowledgedPosition;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedCursor#skip(int)
     */
    @Override
    public synchronized void skip(int entries) {
        checkArgument(entries > 0);
        readPosition = ledger.skipEntries(readPosition, entries);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.mledger.ManagedCursor#seek(com.yahoo.messaging
     * .bookkeeper.ledger.Position)
     */
    @Override
    public synchronized void seek(Position newReadPosition) {
        checkArgument(newReadPosition.compareTo(acknowledgedPosition) >= 0,
                "new read position must be greater than or equal to the mark deleted position for this cursor");

        checkArgument(ledger.isValidPosition(newReadPosition), "new read position is not valid for this managed ledger");
        readPosition = newReadPosition;
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorImpl.class);
}
