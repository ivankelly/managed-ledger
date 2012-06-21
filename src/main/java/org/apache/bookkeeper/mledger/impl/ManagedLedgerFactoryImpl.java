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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class ManagedLedgerFactoryImpl implements ManagedLedgerFactory {
    private final MetaStore store;
    private final BookKeeper bookKeeper;
    private final boolean isBookkeeperManaged;
    private final ZooKeeper zookeeper;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final ConcurrentMap<String, ManagedLedgerImpl> ledgers = Maps.newConcurrentMap();

    public ManagedLedgerFactoryImpl(String zookeeperQuorum) throws Exception {
        this(zookeeperQuorum, 32000);
    }

    public ManagedLedgerFactoryImpl(String zookeeperQuorum, int sessionTimeout) throws Exception {
        this.bookKeeper = new BookKeeper(zookeeperQuorum);
        this.isBookkeeperManaged = true;

        final CountDownLatch counter = new CountDownLatch(1);

        zookeeper = new ZooKeeper(zookeeperQuorum, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                    log.info("Connected to zookeeper");
                    counter.countDown();
                } else {
                    log.error("Error connecting to zookeeper {}", event);
                }
            }
        });

        counter.await();

        this.store = new MetaStoreImplZookeeper(zookeeper);
    }

    public ManagedLedgerFactoryImpl(BookKeeper bookKeeper, ZooKeeper zooKeeper) throws Exception {
        this.bookKeeper = bookKeeper;
        this.isBookkeeperManaged = false;
        this.zookeeper = null;
        this.store = new MetaStoreImplZookeeper(zooKeeper);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedgerFactory#open(java.
     * lang.String)
     */
    @Override
    public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException {
        return open(name, new ManagedLedgerConfig());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedgerFactory#open(java.
     * lang.String, org.apache.bookkeeper.mledger.ManagedLedgerConfig)
     */
    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config) throws InterruptedException,
            ManagedLedgerException {
        ManagedLedgerImpl ledger = ledgers.get(name);
        if (ledger != null) {
            log.info("Reusing opened ManagedLedger: {}", name);
            return ledger;
        } else {
            ledger = new ManagedLedgerImpl(this, bookKeeper, store, config, executor, name);
            ManagedLedgerImpl oldValue = ledgers.putIfAbsent(name, ledger);
            if (oldValue != null) {
                // There has been a concurrent open(), reuse the other instance
                return oldValue;
            } else {
                // Initialize the new ManagedLedger instance
                try {
                    ledger.initialize();
                } catch (ManagedLedgerException e) {
                    // If initialize fails we need to remove the
                    // half-initialized managed ledger from the cache
                    ledgers.remove(name);
                    throw e;
                } catch (InterruptedException e) {
                    ledgers.remove(name);
                    throw e;
                }

                return ledger;
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedgerFactory#asyncOpen(
     * java.lang.String,
     * org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, new ManagedLedgerConfig(), callback, ctx);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedgerFactory#asyncOpen(
     * java.lang.String, org.apache.bookkeeper.mledger.ManagedLedgerConfig,
     * org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncOpen(final String name, final ManagedLedgerConfig config, final OpenLedgerCallback callback,
            final Object ctx) {
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
     * @see org.apache.bookkeeper.mledger.ManagedLedgerFactory#delete(java
     * .lang.String)
     */
    @Override
    public void delete(String name) throws InterruptedException, ManagedLedgerException {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) open(name);
        ledgers.remove(ledger.getName());
        ledger.delete();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.ManagedLedgerFactory#asyncDelete
     * (java.lang.String,
     * org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback ,
     * java.lang.Object)
     */
    @Override
    public void asyncDelete(final String ledger, final DeleteLedgerCallback callback, final Object ctx) {
        executor.submit(new Runnable() {
            public void run() {
                try {
                    delete(ledger);
                    callback.deleteLedgerComplete(null, ctx);
                } catch (Exception e) {
                    log.warn("Got exception when deleting MangedLedger: {}", e);
                    callback.deleteLedgerComplete(e, ctx);
                }
            }
        });
    }

    protected void close(ManagedLedger ledger) {
        // Remove the ledger from the internal factory cache
        ledgers.remove(ledger.getName());
    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
        executor.shutdown();

        for (ManagedLedger ledger : ledgers.values()) {
            ledger.close();
        }

        if (zookeeper != null) {
            zookeeper.close();
        }

        if (isBookkeeperManaged) {
            try {
                bookKeeper.close();
            } catch (BKException e) {
                throw new ManagedLedgerException(e);
            }
        }

    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryImpl.class);
}
