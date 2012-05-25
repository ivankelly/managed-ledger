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
package org.apache.bookkeeper.test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperUtil {
    static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);

    // ZooKeeper related variables
    protected static Integer ZooKeeperDefaultPort = 2185;
    private final InetSocketAddress zkaddr;

    protected ZooKeeperServer zks;
    protected ZooKeeper zkc; // zookeeper client
    protected NIOServerCnxnFactory serverFactory;
    protected File ZkTmpDir;
    private final String connectString;

    public ZooKeeperUtil() {
        zkaddr = new InetSocketAddress(ZooKeeperDefaultPort);
        connectString = "127.0.0.1:" + ZooKeeperDefaultPort;
    }

    public ZooKeeper getZooKeeperClient() {
        return zkc;
    }

    public String getZooKeeperConnectString() {
        return connectString;
    }

    public void startServer() throws Exception {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.debug("Running ZK server");
        // ServerStats.registerAsConcrete();
        ClientBase.setupTestEnv();
        ZkTmpDir = File.createTempFile("zookeeper", "test");
        ZkTmpDir.delete();
        ZkTmpDir.mkdir();

        zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);
        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(zkaddr, 100);
        serverFactory.startup(zks);

        boolean b = ClientBase.waitForServerUp(getZooKeeperConnectString(), ClientBase.CONNECTION_TIMEOUT);
        LOG.debug("Server up: " + b);

        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        final CountDownLatch latch = new CountDownLatch(1);
        zkc = new ZooKeeper(getZooKeeperConnectString(), 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // handle session disconnects and expires
                if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                    latch.countDown();
                }
            }
        });
        if (!latch.await(10000, TimeUnit.MILLISECONDS)) {
            zkc.close();
            fail("Could not connect to zookeeper server");
        }

        // initialize the zk client with values
        zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void killServer() throws Exception {
        if (zkc != null) {
            zkc.close();
        }

        // shutdown ZK server
        if (serverFactory != null) {
            serverFactory.shutdown();
            assertTrue(ClientBase.waitForServerDown(getZooKeeperConnectString(), ClientBase.CONNECTION_TIMEOUT),
                    "waiting for server down");
        }
        // ServerStats.unregister();
        FileUtils.deleteDirectory(ZkTmpDir);
    }
}