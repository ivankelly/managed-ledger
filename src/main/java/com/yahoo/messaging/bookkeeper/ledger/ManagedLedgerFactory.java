package com.yahoo.messaging.bookkeeper.ledger;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.ZooKeeper;

import com.yahoo.messaging.bookkeeper.ledger.impl.ManagedLedgerImpl;
import com.yahoo.messaging.bookkeeper.ledger.impl.MetaStore;
import com.yahoo.messaging.bookkeeper.ledger.impl.MetaStoreImplZookeeper;

public class ManagedLedgerFactory {

    private final MetaStore store;
    private final BookKeeper bookKeeper;

    public ManagedLedgerFactory(ZooKeeper zooKeeper, BookKeeper bookKeeper) throws Exception {
        this.bookKeeper = bookKeeper;
        this.store = new MetaStoreImplZookeeper(zooKeeper);
    }

    public ManagedLedger open(String name) throws Exception {
        return open(name, new ManagedLedgerConfig());
    }

    public ManagedLedger open(String name, ManagedLedgerConfig config) throws Exception {
        return new ManagedLedgerImpl(bookKeeper, store, config, name);
    }

    public void delete(String name) throws Exception {

    }
}
