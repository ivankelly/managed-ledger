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
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static org.apache.bookkeeper.mledger.util.VarArgs.va;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ManagedLedgerImpl implements ManagedLedger, CreateCallback {

    private final static long MegaByte = 1024 * 1024;

    private final BookKeeper bookKeeper;
    private final String name;

    private final ManagedLedgerConfig config;
    private final MetaStore store;

    private final Cache<Long, LedgerHandle> ledgerCache;
    protected final TreeMap<Long, LedgerStat> ledgers = Maps.newTreeMap();

    private final ManagedCursorContainer cursors = new ManagedCursorContainer();

    protected AtomicLong numberOfEntries = new AtomicLong(0);
    protected AtomicLong totalSize = new AtomicLong(0);

    private LedgerHandle currentLedger;
    private boolean currentLedgerIsClosed;
    private boolean newLedgerIsBeingCreated;
    private long currentLedgerEntries = 0;
    private long currentLedgerSize = 0;

    private final Executor executor;
    private final ManagedLedgerFactoryImpl factory;

    /**
     * Queue of pending entries to be added to the managed ledger. Typically
     * entries are queued when a new ledger is created asynchronously and hence
     * there is no ready ledger to write into.
     */
    private final Queue<ManagedLedgerAddEntryOp> pendingAddEntries = Lists.newLinkedList();

    // //////////////////////////////////////////////////////////////////////

    public ManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
            ManagedLedgerConfig config, Executor executor, final String name) {
        this.factory = factory;
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.store = store;
        this.name = name;
        this.executor = executor;
        this.currentLedger = null;
        this.currentLedgerIsClosed = false;
        this.newLedgerIsBeingCreated = false;

        RemovalListener<Long, LedgerHandle> removalListener = new RemovalListener<Long, LedgerHandle>() {
            public void onRemoval(RemovalNotification<Long, LedgerHandle> entry) {
                LedgerHandle ledger = entry.getValue();
                log.debug("[{}] Closing ledger: {} cause={}", va(name, ledger.getId(), entry.getCause()));
                try {
                    ledger.close();
                } catch (Exception e) {
                    log.error("[{}] Error closing ledger {}", name, ledger.getId());
                    log.error("Exception: ", e);
                }
            }
        };
        this.ledgerCache = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.SECONDS)
                .removalListener(removalListener).build();
    }

    protected synchronized void initialize() throws InterruptedException, ManagedLedgerException {
        log.info("Opening managed ledger {}", name);

        // Fetch the list of existing ledgers in the managed ledger
        for (LedgerStat ls : store.getLedgerIds(name)) {
            ledgers.put(ls.getLedgerId(), ls);
        }

        try {
            // Last ledger stat may be zeroed, we must update it
            if (ledgers.size() > 0) {
                long id = ledgers.lastKey();
                LedgerHandle handle = bookKeeper.openLedger(id, config.getDigestType(), config.getPassword());
                ledgers.put(id, new LedgerStat(id, handle.getLastAddConfirmed() + 1, handle.getLength()));

                handle.close();
            }
        } catch (BKException e) {
            throw new ManagedLedgerException(e);
        }

        log.debug("[{}] Contains: {}", name, ledgers);

        // Create a new ledger to start writing
        try {
            currentLedger = bookKeeper.createLedger(config.getDigestType(), config.getPassword());
            currentLedgerIsClosed = false;
            ledgers.put(currentLedger.getId(), new LedgerStat(currentLedger.getId(), 0, 0));
        } catch (BKException e) {
            throw new ManagedLedgerException(e);
        }

        // Save it back to ensure all nodes exist
        store.updateLedgersIds(name, ledgers.values());

        // Load existing cursors
        for (Pair<String, Position> pair : store.getConsumers(name)) {
            log.debug("[{}] Loading cursor {}", name, pair);
            cursors.add(new ManagedCursorImpl(this, pair.first, pair.second));
        }

        // Calculate total entries and size
        for (LedgerStat ls : ledgers.values()) {
            this.numberOfEntries.addAndGet(ls.getEntriesCount());
            this.totalSize.addAndGet(ls.getSize());
        }
    }

    @Override
    public String getName() {
        return name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedger#addEntry(byte[])
     */
    public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        // Result list will contain the status exception and the resulting
        // position
        final List<Object> results = Lists.newArrayList();

        asyncAddEntry(data, new AddEntryCallback() {
            public void addComplete(Throwable status, Position position, Object ctx) {
                results.add(status);
                results.add(position);
                counter.countDown();
            }
        }, null);

        counter.await();
        ManagedLedgerException status = (ManagedLedgerException) results.get(0);
        Position position = (Position) results.get(1);
        if (status != null) {
            log.error("Error adding entry", status);
            throw status;
        }

        return position;
    }

    @Override
    public synchronized void asyncAddEntry(final byte[] data, final AddEntryCallback callback, final Object ctx) {
        log.debug("[{}] asyncAddEntry size={}", name, data.length);
        ManagedLedgerAddEntryOp addOperation = new ManagedLedgerAddEntryOp(this, data, callback, ctx);

        if (currentLedgerIsClosed) {
            // We don't have a ready ledger to write into
            if (newLedgerIsBeingCreated) {
                // We are waiting for a new ledger to be created
                log.debug("[{}] Queue addEntry request", name);
                pendingAddEntries.add(addOperation);
            } else {
                // No ledger and no pending operations. A ledger.create()
                // probably failed before. Retrying here to avoid staleness
                pendingAddEntries.add(addOperation);
                log.debug("[{}] Creating a new ledger", name);
                bookKeeper.asyncCreateLedger(config.getEnsembleSize(), config.getQuorumSize(), config.getDigestType(),
                        config.getPassword(), this, ctx);
            }
        } else if (!currentLedgerIsFull()) {
            // Write into lastLedger
            log.debug("[{}] Write into current ledger lh={}", name, currentLedger.getId());
            addOperation.setLedger(currentLedger);

            ++currentLedgerEntries;
            currentLedgerSize += data.length;
            if (currentLedgerIsFull()) {
                // This entry will be the last added to current ledger
                addOperation.setCloseWhenDone(true);
                currentLedgerIsClosed = true;
            }

            addOperation.initiate();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedger#openCursor(java.
     * lang.String)
     */
    @Override
    public synchronized ManagedCursor openCursor(String cursorName) throws InterruptedException, ManagedLedgerException {
        ManagedCursor cursor = cursors.get(cursorName);

        if (cursor == null) {
            // Create a new one and persist it
            Position position = new Position(currentLedger.getId(), currentLedger.getLastAddConfirmed());

            cursor = new ManagedCursorImpl(this, cursorName, position);
            store.updateConsumer(name, cursorName, position);
            cursors.add(cursor);
        }

        log.debug("[{}] Opened new cursor: {}", this.name, cursor);
        return cursor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedger#asyncOpenCursor(java
     * .lang.String,
     * org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncOpenCursor(final String name, final OpenCursorCallback callback, final Object ctx) {
        executor.execute(new Runnable() {
            public void run() {
                Exception error = null;
                ManagedCursor cursor = null;

                try {
                    cursor = openCursor(name);
                } catch (Exception e) {
                    log.warn("Got exception when adding entry: {}", e);
                    error = e;
                }

                callback.openCursorComplete(error, cursor, ctx);
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedger#getNumberOfEntries()
     */
    @Override
    public long getNumberOfEntries() {
        return numberOfEntries.get();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedger#getTotalSize()
     */
    @Override
    public long getTotalSize() {
        return totalSize.get();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedger#close()
     */
    @Override
    public synchronized void close() throws InterruptedException, ManagedLedgerException {
        for (LedgerHandle ledger : ledgerCache.asMap().values()) {
            log.debug("Closing ledger: {}", ledger.getId());
            try {
                ledger.close();
            } catch (BKException e) {
                throw new ManagedLedgerException(e);
            }
        }

        ledgerCache.invalidateAll();
        log.info("Invalidated {} ledgers in cache", ledgerCache.size());
        factory.close(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedger#asyncClose(com.yahoo
     * .messaging.bookkeeper.ledger.AsyncCallbacks.CloseCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncClose(final CloseCallback callback, final Object ctx) {
        executor.execute(new Runnable() {
            public void run() {
                Exception error = null;

                try {
                    close();
                } catch (Exception e) {
                    log.warn("[{}] Got exception when closin managed ledger: {}", name, e);
                    error = e;
                }

                callback.closeComplete(error, ctx);
            }
        });
    }

    // //////////////////////////////////////////////////////////////////////
    // Callbacks

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.client.AsyncCallback.CreateCallback#createComplete
     * (int, org.apache.bookkeeper.client.LedgerHandle, java.lang.Object)
     */
    @Override
    public synchronized void createComplete(int rc, LedgerHandle lh, Object ctx) {
        log.debug("[{}] createComplete rc={}", va(name, rc));
        newLedgerIsBeingCreated = false;

        if (rc != BKException.Code.OK) {
            log.error("[{}] Error creating ledger rc={} {}", va(name, rc, BKException.getMessage(rc)));
            BKException status = BKException.create(rc);

            // Empty the list of pending requests and make all of them fail
            while (!pendingAddEntries.isEmpty()) {
                pendingAddEntries.poll().failed(status);
            }
        } else {
            log.debug("[{}] Successfully created new ledger {}", name, lh.getId());
            ledgers.put(lh.getId(), new LedgerStat(lh.getId(), 0, 0));
            currentLedger = lh;
            currentLedgerEntries = 0;
            currentLedgerSize = 0;
            currentLedgerIsClosed = false;

            // TODO: (Matteo) update the meta store async
            try {
                store.updateLedgersIds(name, ledgers.values());
                log.debug("Updated meta store");
            } catch (MetaStoreException e) {
                log.warn("Error updating meta data with the new list of ledgers");
                while (!pendingAddEntries.isEmpty()) {
                    pendingAddEntries.poll().failed(e);
                }
            }

            // Process all the pending addEntry requests
            while (!pendingAddEntries.isEmpty()) {
                ManagedLedgerAddEntryOp op = pendingAddEntries.poll();

                op.setLedger(lh);
                ++currentLedgerEntries;
                currentLedgerSize += op.data.length;

                if (currentLedgerIsFull()) {
                    currentLedgerIsClosed = true;
                    op.setCloseWhenDone(true);
                    op.initiate();
                    break;
                } else {
                    op.initiate();
                }
            }
        }
    }

    // //////////////////////////////////////////////////////////////////////
    // Private helpers

    protected synchronized void ledgerClosed(LedgerHandle ledger) {
        log.debug("[{}] Ledger has been closed id={} entries={}",
                va(name, ledger.getId(), ledger.getLastAddConfirmed() + 1));

        ledgers.put(ledger.getId(),
                new LedgerStat(ledger.getId(), ledger.getLastAddConfirmed() + 1, ledger.getLength()));

        bookKeeper.asyncCreateLedger(config.getEnsembleSize(), config.getQuorumSize(), config.getDigestType(),
                config.getPassword(), this, null);
        newLedgerIsBeingCreated = true;
    }

    protected synchronized Pair<List<Entry>, Position> readEntries(Position position, int count)
            throws InterruptedException, ManagedLedgerException {

        LedgerHandle ledger = null;

        if (position.getLedgerId() == -1) {
            if (ledgers.isEmpty()) {
                // The ManagedLedger is completely empty
                return new Pair<List<Entry>, Position>(new ArrayList<Entry>(), position);
            }

            // Initialize the position on the first entry for the first ledger
            // in the set
            position = new Position(ledgers.firstKey(), 0);
        }

        long id = position.getLedgerId();

        if (id == currentLedger.getId()) {
            // Current writing ledger is not in the cache (since we don't want
            // it to be automatically evicted), and we cannot use 2 different
            // ledger handles (read & write)for the same ledger.
            ledger = currentLedger;
        } else {
            ledger = ledgerCache.getIfPresent(id);
        }

        if (ledger == null) {
            // Ledger is not already open, verify that id is valid and try to
            // open it
            checkArgument(ledgers.containsKey(id), "[%s] Ledger id is not assigned to this managed ledger id=%s", name,
                    id);

            // Open the ledger and cache the handle
            log.debug("[{}] Opening ledger {} for read", name, id);
            try {
                ledger = bookKeeper.openLedger(id, config.getDigestType(), config.getPassword());
                log.debug("ok 1");
            } catch (BKException e) {
                log.debug("error");
                throw new ManagedLedgerException(e);
            }

            log.debug("ok");
        }

        // Perform the read
        long firstEntry = position.getEntryId();

        if (firstEntry > ledger.getLastAddConfirmed()) {
            log.debug("[{}] No more messages to read from ledger={} lastEntry={} readEntry={}",
                    va(name, ledger.getId(), ledger.getLastAddConfirmed(), firstEntry));
            if (ledger.getId() != currentLedger.getId()) {
                // Cursor was placed past the end of one ledger, move it to the
                // beginning of the next ledger
                Long nextLedgerId = ledgers.ceilingKey(ledger.getId() + 1);
                return new Pair<List<Entry>, Position>(new ArrayList<Entry>(), new Position(nextLedgerId, 0));
            } else {
                // We reached the end of the entries stream
                return new Pair<List<Entry>, Position>(new ArrayList<Entry>(), position);
            }
        }

        long lastEntry = min(firstEntry + count - 1, ledger.getLastAddConfirmed());

        log.debug("[{}] Reading entries from ledger {} - first={} last={}", va(name, id, firstEntry, lastEntry));

        Enumeration<LedgerEntry> entriesEnum = null;
        try {
            entriesEnum = ledger.readEntries(firstEntry, lastEntry);
        } catch (BKException e) {
            throw new ManagedLedgerException(e);
        }
        long expectedEntries = lastEntry - firstEntry + 1;
        List<Entry> entries = Lists.newArrayListWithExpectedSize((int) expectedEntries);

        while (entriesEnum.hasMoreElements())
            entries.add(new EntryImpl(entriesEnum.nextElement()));

        // Get the "next read position", we need to advance the position taking
        // care of ledgers boundaries
        Position newPosition;
        if (lastEntry < ledger.getLastAddConfirmed()) {
            newPosition = new Position(id, lastEntry + 1);
        } else {
            // Move to next ledger
            Long nextLedgerId = ledgers.ceilingKey(id + 1);
            if (nextLedgerId == null) {
                // We are already in the last ledger
                newPosition = new Position(id, lastEntry + 1);
            } else {
                newPosition = new Position(nextLedgerId, 0);
            }
        }

        return Pair.create(entries, newPosition);
    }

    protected synchronized boolean hasMoreEntries(Position position) {
        if (position.getLedgerId() == currentLedger.getId()) {
            // If we are reading from the last ledger ensure, use the
            // LedgerHandle metadata
            return position.getEntryId() <= currentLedger.getLastAddConfirmed();
        } else if (currentLedger.getLastAddConfirmed() >= 0) {
            // We have entries in the last ledger and we are reading in an
            // older ledger
            return true;
        } else {
            // At this point, currentLedger is empty, we need to check in the
            // older ledgers for entries past the current position
            LedgerStat ls = ledgers.get(position.getLedgerId());
            if (ls == null) {
                // The cursor haven't been initialized yet
                checkArgument(position.getLedgerId() == -1);
                return true;
            } else if (position.getEntryId() < ls.getEntriesCount()) {
                // There are still entries to read in the current reading ledger
                return true;
            } else {
                for (LedgerStat stat : ledgers.tailMap(position.getLedgerId(), false).values()) {
                    if (stat.getEntriesCount() > 0)
                        return true;
                }

                return false;
            }
        }
    }

    protected synchronized void updateCursor(ManagedCursorImpl cursor, Position newPosition)
            throws InterruptedException, ManagedLedgerException {
        // First update the metadata store, so that if we don't succeed we have
        // not changed any other state
        store.updateConsumer(name, cursor.getName(), newPosition);
        cursor.setAcknowledgedPosition(newPosition);
        cursors.cursorUpdated(cursor);

        trimConsumedLedgers();
    }

    /**
     * Checks whether there are ledger that have been fully consumed and deletes
     * them
     * 
     * @throws Exception
     */
    protected void trimConsumedLedgers() throws InterruptedException, ManagedLedgerException {
        long slowestReaderLedgerId = -1;
        if (cursors.isEmpty() && currentLedger != null) {
            // At this point the lastLedger will be pointing to the ledger that
            // has just been closed, therefore the +1 to include lastLedger in
            // the trimming.
            slowestReaderLedgerId = currentLedger.getId() + 1;
        } else {
            slowestReaderLedgerId = cursors.getSlowestReaderPosition().getLedgerId();
        }

        while (!ledgers.isEmpty() && ledgers.firstKey() < slowestReaderLedgerId) {
            // Delete ledger from BookKeeper
            LedgerStat ledgerToDelete = ledgers.firstEntry().getValue();

            ledgerCache.invalidate(ledgerToDelete.getLedgerId());

            log.info("[{}] Removing ledger {}", name, ledgerToDelete.getLedgerId());
            try {
                bookKeeper.deleteLedger(ledgerToDelete.getLedgerId());
            } catch (BKException e) {
                throw new ManagedLedgerException(e);
            }

            // Update metadata
            store.updateLedgersIds(name, ledgers.values());

            ledgers.remove(ledgerToDelete.getLedgerId());
            numberOfEntries.addAndGet(-ledgerToDelete.getEntriesCount());
            totalSize.addAndGet(-ledgerToDelete.getSize());
        }
    }

    /**
     * Delete this ManagedLedger completely from the system.
     * 
     * @throws Exception
     */
    protected void delete() throws InterruptedException, ManagedLedgerException {
        close();

        synchronized (this) {
            try {
                for (LedgerStat ls : ledgers.values()) {
                    log.debug("[{}] Deleting ledger {}", name, ls);
                    bookKeeper.deleteLedger(ls.getLedgerId());
                }
            } catch (BKException e) {
                throw new ManagedLedgerException(e);
            }

            store.removeManagedLedger(name);
        }
    }

    protected synchronized long getNumberOfEntries(Position position) {
        long count = 0;
        // First count the number of unread entries in the ledger pointed by
        // position
        if (position.getLedgerId() >= 0)
            count += ledgers.get(position.getLedgerId()).getEntriesCount() - position.getEntryId();

        // Then, recur all the next ledgers and sum all the entries they contain
        for (LedgerStat ls : ledgers.tailMap(position.getLedgerId(), false).values()) {
            count += ls.getEntriesCount();
        }

        // Last add the entries in the current ledger
        if (!newLedgerIsBeingCreated)
            count += currentLedger.getLastAddConfirmed() + 1;

        return count;
    }

    /**
     * Skip a specified number of entries and return the resulting position.
     * 
     * @param startPosition
     *            the current position
     * @param entriesToSkip
     *            the numbers of entries to skip
     * @return the new position
     */
    protected synchronized Position skipEntries(Position startPosition, int entriesToSkip) {
        long ledgerId = startPosition.getLedgerId();
        entriesToSkip += startPosition.getEntryId();

        while (entriesToSkip > 0) {
            if (currentLedger != null && ledgerId == currentLedger.getId()) {
                checkArgument(entriesToSkip <= (currentLedger.getLastAddConfirmed() + 1));
                return new Position(ledgerId, entriesToSkip);
            } else {
                LedgerStat ledger = ledgers.get(ledgerId);
                if (ledger == null) {
                    checkArgument(!ledgers.isEmpty());
                    ledgerId = ledgers.ceilingKey(ledgerId);
                    continue;
                }

                if (entriesToSkip < ledger.getEntriesCount()) {
                    return new Position(ledgerId, entriesToSkip);
                } else {
                    // Move to next ledger
                    entriesToSkip -= ledger.getEntriesCount();
                    ledgerId = ledgers.ceilingKey(ledgerId + 1);
                }
            }
        }

        return new Position(ledgerId, 0);
    }

    /**
     * Validate whether a specified position is valid for the current managed
     * ledger.
     * 
     * @param position
     *            the position to validate
     * @return true if the position is valid, false otherwise
     */
    protected synchronized boolean isValidPosition(Position position) {
        if (currentLedger != null && position.getLedgerId() == currentLedger.getId()) {
            return position.getEntryId() <= currentLedger.getLastAddConfirmed();
        } else {
            // Look in the ledgers map
            LedgerStat ls = ledgers.get(position.getLedgerId());
            if (ls == null)
                return false;

            return position.getEntryId() < ls.getEntriesCount();
        }
    }

    private boolean currentLedgerIsFull() {
        return currentLedgerEntries >= config.getMaxEntriesPerLedger()
                || currentLedgerSize >= (config.getMaxSizePerLedgerMb() * MegaByte);
    }

    protected Executor getExecutor() {
        return executor;
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerImpl.class);

}
