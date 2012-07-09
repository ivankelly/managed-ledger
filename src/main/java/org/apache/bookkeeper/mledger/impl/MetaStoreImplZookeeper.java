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

import static org.apache.bookkeeper.mledger.util.VarArgs.va;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.mledger.ManagedLedgerException.BadVersionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class MetaStoreImplZookeeper implements MetaStore {

    private static final Charset Encoding = Charsets.UTF_8;
    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private static final String prefixName = "/managed-ledgers";
    private static final String prefix = prefixName + "/";

    private final ZooKeeper zk;

    private static class ZKVersion implements Version {
        int version;

        ZKVersion(int version) {
            this.version = version;
        }
    }

    public MetaStoreImplZookeeper(ZooKeeper zk) throws Exception {
        this.zk = zk;

        if (zk.exists(prefixName, false) == null) {
            zk.create(prefixName, new byte[0], Acl, CreateMode.PERSISTENT);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.impl.MetaStore#getLedgerIds(java
     * .lang.String)
     */
    @Override
    public void getLedgerIds(final String ledgerName, final MetaStoreCallback<List<LedgerStat>> callback) {
        // Try to get the content or create an empty node
        zk.getData(prefix + ledgerName, false, new DataCallback() {
            public void processResult(int rc, String path, Object ctx, final byte[] readData, Stat stat) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    List<LedgerStat> ids = Lists.newArrayList();
                    String content = new String(readData, Encoding);

                    for (String ledgerData : content.split(" ")) {
                        ids.add(LedgerStat.parseData(ledgerData));
                    }
                    callback.operationComplete(ids, new ZKVersion(stat.getVersion()));
                } else if (rc == KeeperException.Code.NONODE.intValue()) {
                    log.info("Creating '{}'", prefix + ledgerName);

                    StringCallback createcb = new StringCallback() {
                            public void processResult(int rc, String path, Object ctx, String name) {
                                if (rc == KeeperException.Code.OK.intValue()) {
                                    List<LedgerStat> ids = Lists.newArrayList();
                                    callback.operationComplete(ids, new ZKVersion(0));
                                } else {
                                    callback.operationFailed(new MetaStoreException(KeeperException.create(rc)));
                                }
                            }
                        };
                    zk.create(prefix + ledgerName, new byte[0], Acl, CreateMode.PERSISTENT, createcb, null);
                } else {
                    callback.operationFailed(new MetaStoreException(KeeperException.create(rc)));
                }
            }
        }, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.impl.MetaStore#updateLedgersIds
     * (java.lang.String, java.lang.Iterable)
     */
    @Override
    public Version updateLedgersIds(String ledgerName, Iterable<LedgerStat> ledgerIds, Version version)
            throws MetaStoreException {
        final CountDownLatch counter = new CountDownLatch(1);

        class Result {
            MetaStoreException status;
            Version version;
        }
        final Result result = new Result();

        asyncUpdateLedgerIds(ledgerName, ledgerIds, version, new MetaStoreCallback<Void>() {
            public void operationComplete(Void v, Version version) {
                result.version = version;
                counter.countDown();
            }
            public void operationFailed(MetaStoreException e) {
                result.status = e;
                counter.countDown();
            }
        });

        try {
            counter.await();
        } catch (InterruptedException e) {
            throw new MetaStoreException(e);
        }

        if (result.status != null) {
            throw result.status;
        }

        return result.version;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.bookkeeper.mledger.impl.MetaStore#asyncUpdateLedgerIds(java
     * .lang.String, java.lang.Iterable,
     * org.apache.bookkeeper.mledger.impl.MetaStore.UpdateLedgersIdsCallback,
     * java.lang.Object)
     */
    @Override
    public void asyncUpdateLedgerIds(String ledgerName, Iterable<LedgerStat> ledgerIds, Version version,
                                     final MetaStoreCallback<Void> callback) {
        StringBuilder sb = new StringBuilder();
        for (LedgerStat item : ledgerIds)
            sb.append(item).append(' ');

        ZKVersion zkVersion = (ZKVersion) version;
        log.debug("Updating {} version={} with content={}", va(prefix + ledgerName, zkVersion.version, sb));

        zk.setData(prefix + ledgerName, sb.toString().getBytes(Encoding), zkVersion.version, new StatCallback() {
            public void processResult(int rc, String path, Object zkCtx, Stat stat) {
                log.debug("UpdateLedgersIdsCallback.processResult rc={}", rc);
                MetaStoreException status = null;
                if (rc == KeeperException.Code.BADVERSION.intValue()) {
                    // Content has been modified on ZK since our last read
                    status = new BadVersionException(KeeperException.create(KeeperException.Code.get(rc)));
                    callback.operationFailed(status);
                } else if (rc != KeeperException.Code.OK.intValue()) {
                    status = new MetaStoreException(KeeperException.create(KeeperException.Code.get(rc)));
                    callback.operationFailed(status);
                } else {
                    callback.operationComplete(null, new ZKVersion(stat.getVersion()));
                }
            }
        }, null);

        log.debug("asyncUpdateLedgerIds done");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.impl.MetaStore#getConsumers(java
     * .lang.String)
     */
    @Override
    public void getConsumers(final String ledgerName, final MetaStoreCallback<List<Pair<String, Position>>> callback) {
        final List<Pair<String, Position>> consumers = Lists.newArrayList();
        final AtomicInteger childCount = new AtomicInteger(0);

        final DataCallback datacb = new DataCallback() {
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (rc != KeeperException.Code.OK.intValue()) {
                        childCount.set(-1);
                        callback.operationFailed(new MetaStoreException(KeeperException.create(rc)));
                        return;
                    }
                    ZKVersion version = (ZKVersion)ctx;
                    String content = new String(data, Encoding);
                    String parts[] = path.split("/");
                    String name = parts[parts.length-1];

                    log.debug("[{}] Processing consumer '{}' pos={}", va(ledgerName, name, content));
                    synchronized(consumers) {
                        consumers.add(Pair.create(name, new Position(content)));
                    }
                    if (childCount.decrementAndGet() == 0) {
                        log.debug("Consumer list: {}", consumers);
                        callback.operationComplete(consumers, version);
                    }
                }
            };
        zk.getChildren(prefix + ledgerName, false,
                new Children2Callback() {
                    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            callback.operationFailed(new MetaStoreException(KeeperException.create(rc)));
                            return;
                        }

                        ZKVersion version = new ZKVersion(stat.getVersion());
                        if (children.size() == 0) {
                            callback.operationComplete(consumers, version);
                        }
                        childCount.set(children.size());
                        for (String name : children) {
                            zk.getData(prefix + ledgerName + "/" + name,
                                       false, datacb, version);
                        }
                    }
                }, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.impl.MetaStore#updateConsumer(
     * java.lang.String, java.lang.String,
     * org.apache.bookkeeper.mledger.Position)
     */
    @Override
    public void updateConsumer(String ledgerName, String consumerName, Position position) throws MetaStoreException {
        log.trace("[{}] Updating position consumer={} new_position={}", va(ledgerName, consumerName, position));

        try {
            try {
                zk.setData(prefix + ledgerName + "/" + consumerName, position.toString().getBytes(Encoding), -1);
            } catch (NoNodeException e) {
                zk.create(prefix + ledgerName + "/" + consumerName, position.toString().getBytes(Encoding), Acl,
                        CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new MetaStoreException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.impl.MetaStore#removeConsumer(java
     * .lang.String, java.lang.String)
     */
    @Override
    public void removeConsumer(String ledgerName, String consumerName) throws MetaStoreException {
        log.info("[{}] Remove consumer={}", ledgerName, consumerName);
        try {
            zk.delete(prefix + ledgerName + "/" + consumerName, -1);
        } catch (Exception e) {
            throw new MetaStoreException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.impl.MetaStore#removeManagedLedger
     * (java.lang.String)
     */
    @Override
    public void removeManagedLedger(String ledgerName) throws MetaStoreException {
        try {
            // First remove all the consumers
            for (String consumer : zk.getChildren(prefix + ledgerName, false)) {
                removeConsumer(ledgerName, consumer);
            }

            log.info("[{}] Remove ManagedLedger", ledgerName);
            zk.delete(prefix + ledgerName, -1);
        } catch (Exception e) {
            throw new MetaStoreException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MetaStoreImplZookeeper.class);
}
