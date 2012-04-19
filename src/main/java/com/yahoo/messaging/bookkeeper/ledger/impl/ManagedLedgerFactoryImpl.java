package com.yahoo.messaging.bookkeeper.ledger.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.DeleteLedgerCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenLedgerCallback;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedger;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerConfig;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory;

public class ManagedLedgerFactoryImpl implements ManagedLedgerFactory {
    private final MetaStore store;
    private final BookKeeper bookKeeper;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public ManagedLedgerFactoryImpl(BookKeeper bookKeeper, ZooKeeper zooKeeper) throws Exception {
        this.bookKeeper = bookKeeper;
        this.store = new MetaStoreImplZookeeper(zooKeeper);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory#open(java.
     * lang.String)
     */
    @Override
    public ManagedLedger open(String name) throws Exception {
        return open(name, new ManagedLedgerConfig());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory#open(java.
     * lang.String, com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerConfig)
     */
    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config) throws Exception {
        return new ManagedLedgerImpl(bookKeeper, store, config, executor, name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory#asyncOpen(
     * java.lang.String,
     * com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenLedgerCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, new ManagedLedgerConfig(), callback, ctx);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory#asyncOpen(
     * java.lang.String,
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerConfig,
     * com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenLedgerCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncOpen(final String name, final ManagedLedgerConfig config,
            final OpenLedgerCallback callback, final Object ctx) {
        executor.submit(new Runnable() {
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory#delete(java
     * .lang.String)
     */
    @Override
    public void delete(String name) throws Exception {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory#asyncDelete
     * (java.lang.String,
     * com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.DeleteLedgerCallback
     * , java.lang.Object)
     */
    @Override
    public void asyncDelete(String name, DeleteLedgerCallback callback, Object ctx) {
        // TODO Auto-generated method stub

    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryImpl.class);
}
