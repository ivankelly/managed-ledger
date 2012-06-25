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

import org.apache.bookkeeper.mledger.ManagedLedgerException.BadVersionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
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
    public Pair<Version, List<LedgerStat>> getLedgerIds(final String ledgerName) throws MetaStoreException {
        final CountDownLatch counter = new CountDownLatch(1);

        class Result {
            Exception status;
            int version;
            byte[] data;
        }
        final Result result = new Result();

        // Try to get the content or create an empty node
        zk.getData(prefix + ledgerName, false, new DataCallback() {
            public void processResult(int rc, String path, Object ctx, final byte[] readData, Stat stat) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    result.version = stat.getVersion();
                    result.data = readData;
                } else if (rc == KeeperException.Code.NONODE.intValue()) {
                    log.info("Creating '{}'", prefix + ledgerName);

                    try {
                        zk.create(prefix + ledgerName, new byte[0], Acl, CreateMode.PERSISTENT);
                        result.data = new byte[0];
                        result.version = 0;
                    } catch (Exception ce) {
                        result.status = ce;
                    }
                } else {
                    result.status = KeeperException.create(KeeperException.Code.get(rc));
                }

                counter.countDown();
            }
        }, null);

        try {
            counter.await();
        } catch (InterruptedException e) {
            throw new MetaStoreException(e);
        }

        if (result.status != null) {
            throw new MetaStoreException(result.status);
        }

        List<LedgerStat> ids = Lists.newArrayList();

        if (result.data.length == 0)
            return new Pair<Version, List<LedgerStat>>(new ZKVersion(result.version), ids);

        String content = new String(result.data, Encoding);

        for (String ledgerData : content.split(" ")) {
            ids.add(LedgerStat.parseData(ledgerData));
        }

        return new Pair<Version, List<LedgerStat>>(new ZKVersion(result.version), ids);
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

        asyncUpdateLedgerIds(ledgerName, ledgerIds, version, new UpdateLedgersIdsCallback() {
            public void updateLedgersIdsComplete(MetaStoreException status, Version version) {
                result.status = status;
                result.version = version;
                counter.countDown();
            }
        }, null);

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
            final UpdateLedgersIdsCallback callback, final Object ctx) {
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
                    callback.updateLedgersIdsComplete(status, null);
                } else if (rc != KeeperException.Code.OK.intValue()) {
                    status = new MetaStoreException(KeeperException.create(KeeperException.Code.get(rc)));
                    callback.updateLedgersIdsComplete(status, null);
                } else {
                    callback.updateLedgersIdsComplete(null, new ZKVersion(stat.getVersion()));
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
    public List<Pair<String, Position>> getConsumers(String ledgerName) throws MetaStoreException {
        List<Pair<String, Position>> consumers = Lists.newArrayList();

        try {
            for (String name : zk.getChildren(prefix + ledgerName, false)) {
                byte[] data = zk.getData(prefix + ledgerName + "/" + name, false, null);
                String content = new String(data, Encoding);
                log.debug("[{}] Processing consumer '{}' pos={}", va(ledgerName, name, content));
                consumers.add(Pair.create(name, new Position(content)));
            }
        } catch (Exception e) {
            throw new MetaStoreException(e);
        }

        log.debug("Consumer list: {}", consumers);
        return consumers;
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
