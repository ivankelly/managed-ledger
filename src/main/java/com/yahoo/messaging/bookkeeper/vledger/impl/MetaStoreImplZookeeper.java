package com.yahoo.messaging.bookkeeper.vledger.impl;

import static com.yahoo.messaging.bookkeeper.vledger.util.VarArgs.va;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.yahoo.messaging.bookkeeper.vledger.Position;
import com.yahoo.messaging.bookkeeper.vledger.util.Pair;

public class MetaStoreImplZookeeper implements MetaStore {

    private static final Charset Encoding = Charsets.UTF_8;
    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private static final String prefixName = "/managed-ledgers";
    private static final String prefix = prefixName + "/";

    private final ZooKeeper zk;

    public MetaStoreImplZookeeper(ZooKeeper zk) throws Exception {
        this.zk = zk;

        if (zk.exists(prefixName, false) == null) {
            zk.create(prefixName, new byte[0], Acl, CreateMode.PERSISTENT);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.vledger.impl.MetaStore#getLedgerIds(java
     * .lang.String)
     */
    @Override
    public List<Long> getLedgerIds(String ledgerName) throws Exception {
        byte[] data;
        try {
            data = zk.getData(prefix + ledgerName, false, null);
        } catch (NoNodeException e) {
            return Lists.newArrayList();
        }

        String content = new String(data, Encoding);
        List<Long> ids = Lists.newArrayList();
        for (String id : content.split(" "))
            ids.add(Long.parseLong(id));

        return ids;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.vledger.impl.MetaStore#updateLedgersIds
     * (java.lang.String, java.lang.Iterable)
     */
    @Override
    public void updateLedgersIds(String ledgerName, Iterable<Long> ledgerIds) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (Long id : ledgerIds)
            sb.append(id).append(' ');

        try {
            zk.setData(prefix + ledgerName, sb.toString().getBytes(Encoding), -1);
        } catch (NoNodeException e) {
            log.info("Creating '{}' Content='{}'", prefix + ledgerName,
                    Arrays.toString(sb.toString().getBytes(Encoding)));
            zk.create(prefix + ledgerName, sb.toString().getBytes(Encoding), Acl,
                    CreateMode.PERSISTENT);

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.vledger.impl.MetaStore#getConsumers(java
     * .lang.String)
     */
    @Override
    public List<Pair<String, Position>> getConsumers(String ledgerName) throws Exception {
        List<Pair<String, Position>> consumers = Lists.newArrayList();

        for (String name : zk.getChildren(prefix + ledgerName, false)) {
            byte[] data = zk.getData(prefix + ledgerName + "/" + name, false, null);
            String content = new String(data, Encoding);
            log.debug("[{}] Processing consumer '{}' pos={}", va(ledgerName, name, content));
            consumers.add(Pair.create(name, new Position(content)));
        }

        return consumers;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.yahoo.messaging.bookkeeper.vledger.impl.MetaStore#updateConsumer(
     * java.lang.String, java.lang.String,
     * com.yahoo.messaging.bookkeeper.vledger.Position)
     */
    @Override
    public void updateConsumer(String ledgerName, String consumerName, Position position)
            throws Exception {
        log.trace("[{}] Updating position consumer={} new_position={}",
                va(ledgerName, consumerName, position));

        try {
            zk.setData(prefix + ledgerName + "/" + consumerName,
                    position.toString().getBytes(Encoding), -1);
        } catch (NoNodeException e) {
            zk.create(prefix + ledgerName + "/" + consumerName,
                    position.toString().getBytes(Encoding), Acl, CreateMode.PERSISTENT);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MetaStoreImplZookeeper.class);
}
