/**
 * 
 */
package com.yahoo.messaging.bookkeeper.ledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.yahoo.messaging.bookkeeper.ledger.util.VarArgs.va;
import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedger;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerConfig;
import com.yahoo.messaging.bookkeeper.ledger.Position;
import com.yahoo.messaging.bookkeeper.ledger.util.Pair;

/**
 * 
 */
public class ManagedLedgerImpl implements ManagedLedger {

    private final int ensembleSize;
    private final int quorumSize;
    private final BookKeeper bookKeeper;
    private final String name;
    private final DigestType digestType;
    private final byte[] passwd;

    private final MetaStore store;

    private final Cache<Long, LedgerHandle> ledgerCache;
    private final TreeSet<Long> ledgers = Sets.newTreeSet();

    private final Map<String, ManagedCursor> cursors = Maps.newHashMap();

    private LedgerHandle lastLedger;

    private long numberOfEntries;
    private long totalSize;

    // //////////////////////////////////////////////////////////////////////

    public ManagedLedgerImpl(BookKeeper bookKeeper, ZooKeeper zookeeper,
            ManagedLedgerConfig config, final String name) throws Exception {
        this.ensembleSize = config.getEnsembleSize();
        this.quorumSize = config.getQuorumSize();
        this.bookKeeper = bookKeeper;
        this.name = name;
        this.digestType = config.getDigestType();
        this.passwd = config.getPassword();

        RemovalListener<Long, LedgerHandle> removalListener = new RemovalListener<Long, LedgerHandle>() {
            public void onRemoval(RemovalNotification<Long, LedgerHandle> entry) {
                LedgerHandle ledger = entry.getValue();
                log.debug("[{}] Closing ledger: {} cause={}",
                        va(name, ledger.getId(), entry.getCause()));
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

        this.store = new MetaStoreImplZookeeper(zookeeper);

        log.info("Opening managed ledger {}", name);

        // Fetch the list of existing ledgers in the managed ledger
        ledgers.addAll(store.getLedgerIds(name));
        log.debug("[{}] Contains: {}", name, ledgers);

        // Save it back to ensure all nodes exist
        store.updateLedgersIds(name, ledgers);

        // Load existing cursors
        for (Pair<String, Position> pair : store.getConsumers(name)) {
            log.debug("[{}] Loading cursor {}", name, pair);
            cursors.put(pair.first, new ManagedCursorImpl(this, store, pair.first, pair.second));
        }

        // FIXME: Matteo - Store the sizes/entries count in metadata instead of
        // recalculating every time
        for (long id : ledgers) {
            LedgerHandle ledger = bookKeeper.openLedger(id, digestType, passwd);
            this.numberOfEntries += ledger.getLastAddConfirmed() + 1;
            this.totalSize += ledger.getLength();

            ledgerCache.put(id, ledger);
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
    public void addEntry(byte[] data) throws Exception {
        // XXX: Restricting to 50 entries per ledger
        if (lastLedger != null && lastLedger.getLastAddConfirmed() >= 49) {
            // Close current ledger and force to write into a new one
            lastLedger.close();
            lastLedger = null;
        }

        if (lastLedger == null) {
            // We need to open a new ledger for writing
            lastLedger = bookKeeper.createLedger(ensembleSize, quorumSize, digestType, passwd);
            ledgers.add(lastLedger.getId());
            store.updateLedgersIds(name, ledgers);
            log.debug("[{}] Created a new ledger: {}", name, lastLedger.getId());
        }

        lastLedger.addEntry(data);
        ++numberOfEntries;
        totalSize += data.length;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#openCursor(java.
     * lang.String)
     */
    @Override
    public ManagedCursor openCursor(String cursorName) throws Exception {
        ManagedCursor cursor = cursors.get(cursorName);

        if (cursor == null) {
            // Create a new one and persist it
            Position position;
            if (lastLedger != null) {
                // Set the position past the end of the last ledger
                position = new Position(lastLedger.getId(), lastLedger.getLastAddConfirmed() + 1);
            } else {
                // Create an invalid position, this will be updated at the next
                // read
                position = new Position(-1, -1);
            }

            cursor = new ManagedCursorImpl(this, store, cursorName, position);
        }

        log.debug("[{}] Opened new cursor: {}", this.name, cursor);
        return cursor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#getNumberOfEntries()
     */
    @Override
    public long getNumberOfEntries() {
        return numberOfEntries;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#getTotalSize()
     */
    @Override
    public long getTotalSize() {
        return totalSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.ManagedLedger#close()
     */
    @Override
    public void close() {
        ledgerCache.invalidateAll();
    }

    // //////////////////////////////////////////////////////////////////////

    // //////////////////////////////////////////////////////////////////////
    // Private helpers

    protected Pair<List<Entry>, Position> readEntries(Position position, int count)
            throws Exception {
        if (position.getLedgerId() == -1) {
            position = new Position(ledgers.first(), 0);
        }

        LedgerHandle ledger = null;
        long id = position.getLedgerId();
        if (lastLedger != null && id == lastLedger.getId()) {
            // Current writing ledger is not in the cache (since we don't want
            // it to be automatically evicted), and we cannot use 2 different
            // ledger handles (read & write)for the same ledger.
            ledger = lastLedger;
        } else {
            ledger = ledgerCache.getIfPresent(id);
        }

        if (ledger == null) {
            // Ledger is not already open, verify that id is valid and try to
            // open it
            checkArgument(ledgers.contains(id),
                    "[%s] Ledger id is not assigned to this managed ledger id=%s", name, id);

            // Open the ledger and cache the handle
            log.debug("[{}] Opening ledger {} for read", name, id);
            ledger = bookKeeper.openLedger(id, digestType, passwd);
        }

        // Perform the read
        long firstEntry = position.getEntryId();

        if (firstEntry > ledger.getLastAddConfirmed()) {
            log.debug("[{}] No more messages to read from ledger={} lastEntry={} readEntry={}",
                    va(name, ledger.getId(), ledger.getLastAddConfirmed(), firstEntry));
            return new Pair<List<Entry>, Position>(new ArrayList<Entry>(), position);
        }

        long lastEntry = min(firstEntry + count - 1, ledger.getLastAddConfirmed());

        log.debug("[{}] Reading entries from ledger {} - first={} last={}",
                va(name, id, firstEntry, lastEntry));

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
            Long nextLedgerId = ledgers.ceiling(id + 1);
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
        if (lastLedger == null)
            return false;

        return position.getLedgerId() < lastLedger.getId()
                || position.getEntryId() <= lastLedger.getLastAddConfirmed();
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerImpl.class);

}
