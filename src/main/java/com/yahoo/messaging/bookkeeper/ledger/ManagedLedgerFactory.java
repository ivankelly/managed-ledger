package com.yahoo.messaging.bookkeeper.ledger;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.ZooKeeper;

import com.yahoo.messaging.bookkeeper.ledger.impl.ManagedLedgerImpl;

public class ManagedLedgerFactory {

    private final ZooKeeper zooKeeper;
    private final BookKeeper bookKeeper;

    public ManagedLedgerFactory(ZooKeeper zooKeeper, BookKeeper bookKeeper) {
        this.zooKeeper = zooKeeper;
        this.bookKeeper = bookKeeper;
    }

    public ManagedLedger open(String name) throws Exception {
        return open(name, new ManagedLedgerConfig());
    }

    public ManagedLedger open(String name, ManagedLedgerConfig config) throws Exception {
        return new ManagedLedgerImpl(bookKeeper, zooKeeper, config, name);
    }

    public void delete(String name) throws Exception {

    }
}
