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
package com.yahoo.messaging.bookkeeper.ledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.yahoo.messaging.bookkeeper.ledger.util.VarArgs.va;
import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.AddEntryCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.CloseCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenCursorCallback;
import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedger;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerConfig;
import com.yahoo.messaging.bookkeeper.ledger.Position;
import com.yahoo.messaging.bookkeeper.ledger.util.Pair;

public class ManagedLedgerImpl implements ManagedLedger {

    private final static long MegaByte = 1024 * 1024;

    private final BookKeeper bookKeeper;
    private final String name;

    private final ManagedLedgerConfig config;
    private final MetaStore store;

    private final Cache<Long, LedgerHandle> ledgerCache;
    private final TreeMap<Long, LedgerStat> ledgers = Maps.newTreeMap();

    private final ManagedCursorContainer cursors = new ManagedCursorContainer();

    private LedgerHandle lastLedger;

    private AtomicLong numberOfEntries = new AtomicLong(0);
    private AtomicLong totalSize = new AtomicLong(0);

    private final Executor executor;
    private final ManagedLedgerFactoryImpl factory;

    // //////////////////////////////////////////////////////////////////////

    public ManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
            ManagedLedgerConfig config, Executor executor, final String name) throws Exception {
        this.factory = factory;
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.store = store;
        this.name = name;
        this.executor = executor;
        this.lastLedger = null;

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

        log.info("Opening managed ledger {}", name);

        // Fetch the list of existing ledgers in the managed ledger
        for (LedgerStat ls : store.getLedgerIds(name)) {
            ledgers.put(ls.getLedgerId(), ls);
        }

        // Last ledger stat may be zeroed, we must update it
        if (ledgers.size() > 0) {
            long id = ledgers.lastKey();
            LedgerHandle handle = bookKeeper.openLedger(id, config.getDigestType(), config.getPassword());
            ledgers.put(id, new LedgerStat(id, handle.getLastAddConfirmed() + 1, handle.getLength()));
            handle.close();
        }

        log.debug("[{}] Contains: {}", name, ledgers);

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

    public String getName() {
        return name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#addEntry(byte[])
     */
    public synchronized void addEntry(byte[] data) throws Exception {
        log.debug("Adding entry");

        if (isLedgerFull(lastLedger)) {
            // The last ledger has reached the limit of entries/size, so we
            // force to close current ledger and start a new one
            lastLedger.close();
            log.info("[{}] Closing ledger {} for being full.", name, lastLedger.getId());

            // Update LedgerStat instance
            ledgers.put(lastLedger.getId(), new LedgerStat(lastLedger.getId(), lastLedger.getLastAddConfirmed() + 1,
                    lastLedger.getLength()));

            lastLedger = null;
        }

        if (lastLedger == null) {
            // We need to open a new ledger for writing
            lastLedger = bookKeeper.createLedger(config.getEnsembleSize(), config.getQuorumSize(),
                    config.getDigestType(), config.getPassword());
            ledgerCache.put(lastLedger.getId(), lastLedger);

            ledgers.put(lastLedger.getId(), new LedgerStat(lastLedger.getId(), 0, 0));
            store.updateLedgersIds(name, ledgers.values());
            log.debug("[{}] Created a new ledger: {}", name, lastLedger.getId());
        }

        lastLedger.addEntry(data);
        numberOfEntries.incrementAndGet();
        totalSize.addAndGet(data.length);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#asyncAddEntry(byte[],
     * com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.AddEntryCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncAddEntry(final byte[] data, final AddEntryCallback callback, final Object ctx) {
        // If we can append to the last ledger, we do it in the async mode, else
        // we fallback to the background thread async.
        synchronized (this) {
            if (lastLedger != null && !isLedgerFull(lastLedger)) {
                log.debug("Using ledger.asyncAddEntry()");
                lastLedger.asyncAddEntry(data, new AddCallback() {
                    public void addComplete(int rc, LedgerHandle lh, long entryId, Object mlCtx) {
                        log.debug("addComplete: rc={} entryId={}", rc, entryId);
                        BKException exception = null;
                        if (rc != BKException.Code.OK) {
                            exception = BKException.create(rc);
                        } else {
                            ManagedLedgerImpl ml = (ManagedLedgerImpl) mlCtx;
                            ml.numberOfEntries.incrementAndGet();
                            ml.totalSize.addAndGet(data.length);
                        }
                        callback.addComplete(exception, ctx);
                    }
                }, this);

                return;
            }
        }

        // If there is some more complicated things to do (opening/closing
        // ledgers, etc.. ), execute the addEntry() in a background thread.
        log.debug("Using sync api in a background thread");
        executor.execute(new Runnable() {
            public void run() {
                try {
                    addEntry(data);
                    callback.addComplete(null, ctx);
                } catch (Exception e) {
                    log.warn("Got exception when adding entry: {}", e);
                    callback.addComplete(e, ctx);
                }
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#openCursor(java.
     * lang.String)
     */
    @Override
    public synchronized ManagedCursor openCursor(String cursorName) throws Exception {
        ManagedCursor cursor = cursors.get(cursorName);

        if (cursor == null) {
            // Create a new one and persist it
            Position position;
            LedgerHandle ledger = lastLedger;

            if (ledger != null) {
                // Set the position past the end of the last ledger
                position = new Position(ledger.getId(), ledger.getLastAddConfirmed());
            } else {
                // Create an invalid position, this will be updated at the next
                // read
                position = new Position(-1, -1);
            }

            cursor = new ManagedCursorImpl(this, cursorName, position);
            cursors.add(cursor);
        }

        log.debug("[{}] Opened new cursor: {}", this.name, cursor);
        return cursor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#asyncOpenCursor(java
     * .lang.String,
     * com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenCursorCallback,
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
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#getNumberOfEntries()
     */
    @Override
    public long getNumberOfEntries() {
        return numberOfEntries.get();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#getTotalSize()
     */
    @Override
    public long getTotalSize() {
        return totalSize.get();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#close()
     */
    @Override
    public synchronized void close() throws Exception {
        for (LedgerHandle ledger : ledgerCache.asMap().values()) {
            log.debug("Closing ledger: {}", ledger.getId());
            ledger.close();
        }

        ledgerCache.invalidateAll();
        log.info("Invalidated {} ledgers in cache", ledgerCache.size());
        factory.close(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#asyncClose(com.yahoo
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

    // //////////////////////////////////////////////////////////////////////
    // Private helpers

    protected Pair<List<Entry>, Position> readEntries(Position position, int count) throws Exception {

        LedgerHandle ledger = null;
        LedgerHandle last = null;

        synchronized (this) {
            if (position.getLedgerId() == -1) {
                position = new Position(ledgers.firstKey(), 0);
            }

            last = lastLedger;
        }

        long id = position.getLedgerId();

        if (last != null && id == last.getId()) {
            // Current writing ledger is not in the cache (since we don't want
            // it to be automatically evicted), and we cannot use 2 different
            // ledger handles (read & write)for the same ledger.
            ledger = last;
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
            ledger = bookKeeper.openLedger(id, config.getDigestType(), config.getPassword());
        }

        // Perform the read
        long firstEntry = position.getEntryId();

        if (firstEntry > ledger.getLastAddConfirmed()) {
            log.debug("[{}] No more messages to read from ledger={} lastEntry={} readEntry={}",
                    va(name, ledger.getId(), ledger.getLastAddConfirmed(), firstEntry));
            return new Pair<List<Entry>, Position>(new ArrayList<Entry>(), position);
        }

        long lastEntry = min(firstEntry + count - 1, ledger.getLastAddConfirmed());

        log.debug("[{}] Reading entries from ledger {} - first={} last={}", va(name, id, firstEntry, lastEntry));

        Enumeration<LedgerEntry> entriesEnum = ledger.readEntries(firstEntry, lastEntry);
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

    protected boolean hasMoreEntries(Position position) {
        LedgerHandle last = null;
        synchronized (this) {
            last = lastLedger;
        }

        if (last != null) {
            if (position.getLedgerId() == last.getId()) {
                // If we are reading from the last ledger ensure, use the
                // LedgerHandle metadata
                return position.getEntryId() <= last.getLastAddConfirmed();
            } else if (last.getLastAddConfirmed() >= 0) {
                // We have entries in the last ledger and we are reading in an
                // older ledger
                return true;
            }
        }

        // At this point, lastLedger is either null or empty, we need to check
        // in the older ledgers for entries past the current position
        LedgerStat ls = ledgers.get(position.getLedgerId());
        if (ls == null) {
            // Position is still invalid
            return false;
        } else {
            checkArgument(position.getEntryId() < ls.getEntriesCount());

            // There are still entries to read in the current reading ledger
            return true;
        }
    }

    protected void updateCursor(ManagedCursorImpl cursor, Position newPosition) throws Exception {
        // First update the metadata store, so that if we don't succeed we have
        // not changed any other state
        store.updateConsumer(name, cursor.getName(), newPosition);
        cursor.setAcknowledgedPosition(newPosition);

        cursors.cursorUpdated(cursor);

        // Delete ledgers if needed
        long slowestReaderPosition = cursors.getSlowestReaderPosition().getLedgerId();
        while (!ledgers.isEmpty() && ledgers.firstKey() < slowestReaderPosition) {
            // Delete ledger from BookKeeper
            LedgerStat ledgerToDelete = ledgers.firstEntry().getValue();
            log.info("[{}] Removing ledger {}", name, ledgerToDelete.getLedgerId());
            bookKeeper.deleteLedger(ledgerToDelete.getLedgerId());

            // Update metadata
            store.updateLedgersIds(name, ledgers.values());

            synchronized (this) {
                ledgers.remove(ledgerToDelete.getLedgerId());
                numberOfEntries.addAndGet(-ledgerToDelete.getEntriesCount());
                totalSize.addAndGet(-ledgerToDelete.getSize());
            }
        }
    }

    /**
     * Delete this ManagedLedger completely from the system.
     * 
     * @throws Exception
     */
    void delete() throws Exception {
        close();

        synchronized (this) {
            for (LedgerStat ls : ledgers.values()) {
                log.debug("[{}] Deleting ledger {}", name, ls);
                bookKeeper.deleteLedger(ls.getLedgerId());
            }

            store.removeManagedLedger(name);
        }
    }

    private boolean isLedgerFull(LedgerHandle ledger) {
        return ledger != null && //
                (ledger.getLastAddConfirmed() >= config.getMaxEntriesPerLedger() - 1 //
                || ledger.getLength() >= (config.getMaxSizePerLedgerMb() * MegaByte));
    }

    Executor getExecutor() {
        return executor;
    }

    MetaStore getStore() {
        return store;
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerImpl.class);

}
