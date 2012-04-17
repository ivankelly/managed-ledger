package com.yahoo.messaging.bookkeeper.ledger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenLedgerCallback;
import com.yahoo.messaging.bookkeeper.ledger.impl.ManagedLedgerImpl;
import com.yahoo.messaging.bookkeeper.ledger.impl.MetaStore;
import com.yahoo.messaging.bookkeeper.ledger.impl.MetaStoreImplZookeeper;

public class ManagedLedgerFactory {

    private final MetaStore store;
    private final BookKeeper bookKeeper;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public ManagedLedgerFactory(ZooKeeper zooKeeper, BookKeeper bookKeeper) throws Exception {
        this.bookKeeper = bookKeeper;
        this.store = new MetaStoreImplZookeeper(zooKeeper);
    }

    public ManagedLedger open(String name) throws Exception {
        return open(name, new ManagedLedgerConfig());
    }

    public ManagedLedger open(String name, ManagedLedgerConfig config) throws Exception {
        return new ManagedLedgerImpl(bookKeeper, store, config, executor, name);
    }

    public Future<?> asyncOpen(final String name, final ManagedLedgerConfig config,
            final OpenLedgerCallback callback, final Object ctx) {
        return executor.submit(new Runnable() {
            public void run() {
                try {
                    ManagedLedger ledger = open(name, config);
                    callback.openLedgerComplete(null, ledger, ctx);
                } catch (Exception e) {
                    log.warn("Got exception when adding entry: {}", e);
                    callback.openLedgerComplete(e, null, ctx);
                }
            }
        });
    }

    public void delete(String name) throws Exception {

    }

    private static Logger log = LoggerFactory.getLogger(ManagedLedgerFactory.class);
}
