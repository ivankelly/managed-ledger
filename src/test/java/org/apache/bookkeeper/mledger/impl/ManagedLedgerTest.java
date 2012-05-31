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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

public class ManagedLedgerTest extends BookKeeperClusterTestCase {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTest.class);

    private static final Charset Encoding = Charsets.UTF_8;

    @Test
    public void managedLedgerApi() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor cursor = ledger.openCursor("c1");

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        // Reads all the entries in batches of 20
        while (cursor.hasMoreEntries()) {

            List<Entry> entries = cursor.readEntries(20);
            log.debug("Read {} entries", entries.size());

            for (Entry entry : entries) {
                log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            }

            // Acknowledge only on last entry
            Entry lastEntry = entries.get(entries.size() - 1);
            cursor.markDelete(lastEntry.getPosition());

            log.info("-----------------------");
        }

        log.info("Finished reading entries");

        ledger.close();
    }

    @Test
    public void simple() throws Exception {
        String zookeeperQuorum = bkc.getConf().getZkServers();
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(zookeeperQuorum);

        ManagedLedger ledger = factory.open("my_test_ledger");

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        ManagedCursor cursor = ledger.openCursor("c1");

        assertEquals(cursor.hasMoreEntries(), false);
        assertEquals(cursor.readEntries(100), new ArrayList<Entry>());

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);

        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);

        ledger.close();
    }

    @Test
    public void closeAndReopen() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger
        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger");

        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);

        ledger.close();
    }

    @Test
    public void acknowledge1() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);

        List<Entry> entries = cursor.readEntries(2);
        assertEquals(entries.size(), 2);
        assertEquals(cursor.hasMoreEntries(), false);

        cursor.markDelete(entries.get(0).getPosition());

        ledger.close();

        // / Reopen the same managed-ledger

        ledger = factory.open("my_test_ledger");
        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        assertEquals(cursor.hasMoreEntries(), true);

        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);

        ledger.close();
    }

    @Test
    public void asyncAPI() throws Throwable {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        final CyclicBarrier barrier = new CyclicBarrier(2);

        factory.asyncOpen("my_test_ledger", new ManagedLedgerConfig(), new OpenLedgerCallback() {
            public void openLedgerComplete(Throwable status, ManagedLedger ledger, Object ctx) {
                assertNull(status);

                ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
                    public void openCursorComplete(Throwable status, ManagedCursor cursor, Object ctx) {
                        assertNull(status);
                        ManagedLedger ledger = (ManagedLedger) ctx;

                        ledger.asyncAddEntry("test".getBytes(Encoding), new AddEntryCallback() {
                            public void addComplete(Throwable status, Position position, Object ctx) {
                                assertNull(status);

                                @SuppressWarnings("unchecked")
                                Pair<ManagedLedger, ManagedCursor> pair = (Pair<ManagedLedger, ManagedCursor>) ctx;
                                ManagedLedger ledger = pair.first;
                                ManagedCursor cursor = pair.second;

                                assertEquals(ledger.getNumberOfEntries(), 1);
                                assertEquals(ledger.getTotalSize(), "test".getBytes(Encoding).length);

                                cursor.asyncReadEntries(2, new ReadEntriesCallback() {
                                    public void readEntriesComplete(Throwable status, List<Entry> entries, Object ctx) {
                                        assertNull(status);
                                        ManagedCursor cursor = (ManagedCursor) ctx;

                                        assertEquals(entries.size(), 1);
                                        Entry entry = entries.get(0);
                                        assertEquals(new String(entry.getData(), Encoding), "test");

                                        cursor.asyncMarkDelete(entry.getPosition(), new MarkDeleteCallback() {
                                            public void markDeleteComplete(Throwable status, Object ctx) {
                                                assertNull(status);
                                                ManagedCursor cursor = (ManagedCursor) ctx;

                                                assertEquals(cursor.hasMoreEntries(), false);

                                                try {
                                                    barrier.await();
                                                } catch (Exception e) {
                                                    log.error("Error waiting for barrier");
                                                }
                                            }
                                        }, cursor);
                                    }
                                }, cursor);
                            }
                        }, new Pair<ManagedLedger, ManagedCursor>(ledger, cursor));
                    }
                }, ledger);
            }
        }, null);

        barrier.await(5, TimeUnit.SECONDS);

        log.info("Test completed");
    }

    @Test
    public void spanningMultipleLedgers() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        for (int i = 0; i < 11; i++)
            ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 10);
        assertEquals(cursor.hasMoreEntries(), true);

        Position first = entries.get(0).getPosition();

        // Read again, from next ledger id
        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        assertEquals(cursor.hasMoreEntries(), false);

        Position last = entries.get(0).getPosition();

        log.info("First={} Last={}", first, last);
        assertTrue(first.getLedgerId() < last.getLedgerId());
        assertEquals(first.getEntryId(), 0);
        assertEquals(last.getEntryId(), 0);
        ledger.close();
    }

    @Test(timeOut = 3000)
    public void spanningMultipleLedgersWithSize() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1000000);
        config.setMaxSizePerLedgerMb(1);
        config.setEnsembleSize(1);
        config.setQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        byte[] content = new byte[1023 * 1024];

        for (int i = 0; i < 3; i++)
            ledger.addEntry(content);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 2);
        assertEquals(cursor.hasMoreEntries(), true);

        Position first = entries.get(0).getPosition();

        // Read again, from next ledger id
        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        assertEquals(cursor.hasMoreEntries(), false);

        Position last = entries.get(0).getPosition();

        log.info("First={} Last={}", first, last);
        assertTrue(first.getLedgerId() < last.getLedgerId());
        assertEquals(first.getEntryId(), 0);
        assertEquals(last.getEntryId(), 0);
        ledger.close();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidReadEntriesArg1() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());
        cursor.readEntries(-1);

        fail("Should have thrown an exception in the above line");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidReadEntriesArg2() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());
        cursor.readEntries(0);

        fail("Should have thrown an exception in the above line");
    }

    @Test
    public void deleteAndReopen() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Delete and reopen
        factory.delete("my_test_ledger");
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 0);
        ledger.close();
    }

    @Test
    public void deleteAndReopenWithCursors() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Delete and reopen
        factory.delete("my_test_ledger");
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 0);
        ManagedCursor cursor = ledger.openCursor("test-cursor");
        assertEquals(cursor.hasMoreEntries(), false);
        ledger.close();
    }

    @Test(timeOut = 3000)
    public void asyncDeleteWithError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        stopBKCluster();
        stopZKCluster();

        // Delete and reopen
        factory.asyncDelete("my_test_ledger", new DeleteLedgerCallback() {

            public void deleteLedgerComplete(Throwable status, Object ctx) {
                assertNull(ctx);
                assertNotNull(status);

                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("Received exception ", e);
                }
            }
        }, null);

        barrier.await();
    }

    @Test(timeOut = 3000)
    public void asyncAddEntryWithoutError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CyclicBarrier barrier = new CyclicBarrier(2);

        ledger.asyncAddEntry("dummy-entry-1".getBytes(Encoding), new AddEntryCallback() {
            public void addComplete(Throwable status, Position position, Object ctx) {
                assertNull(ctx);
                assertNull(status);

                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("Received exception ", e);
                }
            }
        }, null);

        barrier.await();
    }

    @Test(timeOut = 3000)
    public void doubleAsyncAddEntryWithoutError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CountDownLatch done = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final String content = "dummy-entry-" + i;
            ledger.asyncAddEntry(content.getBytes(Encoding), new AddEntryCallback() {
                public void addComplete(Throwable status, Position position, Object ctx) {
                    assertNotNull(ctx);
                    assertNull(status);

                    log.info("Successfully added {}", content);
                    done.countDown();
                }
            }, this);
        }

        done.await();
        assertEquals(ledger.getNumberOfEntries(), 10);
    }

    @Test(timeOut = 3000)
    public void asyncAddEntryWithError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CyclicBarrier barrier = new CyclicBarrier(2);
        stopBKCluster();
        stopZKCluster();

        ledger.asyncAddEntry("dummy-entry-1".getBytes(Encoding), new AddEntryCallback() {
            public void addComplete(Throwable status, Position position, Object ctx) {
                assertNull(ctx);
                assertNull(position);
                assertNotNull(status);

                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("Received exception ", e);
                }
            }
        }, null);

        barrier.await();
    }

    @Test(timeOut = 3000)
    public void asyncCloseWithoutError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CyclicBarrier barrier = new CyclicBarrier(2);

        ledger.asyncClose(new CloseCallback() {
            public void closeComplete(Throwable status, Object ctx) {
                assertNull(ctx);
                assertNull(status);

                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("Received exception ", e);
                }
            }
        }, null);

        barrier.await();
    }

    @Test(timeOut = 3000)
    public void asyncCloseWithError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CyclicBarrier barrier = new CyclicBarrier(2);

        stopBKCluster();
        stopZKCluster();

        ledger.asyncClose(new CloseCallback() {
            public void closeComplete(Throwable status, Object ctx) {
                assertNull(ctx);
                assertNotNull(status);

                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("Received exception ", e);
                }
            }
        }, null);

        barrier.await();
    }

    @Test(timeOut = 3000)
    public void asyncOpenCursorWithoutError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");

        final CyclicBarrier barrier = new CyclicBarrier(2);

        ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
            public void openCursorComplete(Throwable status, ManagedCursor cursor, Object ctx) {
                assertNull(ctx);
                assertNull(status);
                assertNotNull(cursor);

                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("Received exception ", e);
                }
            }
        }, null);

        barrier.await();
    }

    @Test(timeOut = 3000)
    public void asyncOpenCursorWithError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedger ledger = factory.open("my_test_ledger");

        final CyclicBarrier barrier = new CyclicBarrier(2);

        stopBKCluster();
        stopZKCluster();

        ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
            public void openCursorComplete(Throwable status, ManagedCursor cursor, Object ctx) {
                assertNull(ctx);
                assertNotNull(status);
                assertNull(cursor);

                try {
                    barrier.await();
                } catch (Exception e) {
                    fail("Received exception ", e);
                }
            }
        }, null);

        barrier.await();
    }

    @Test
    public void readFromOlderLedger() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
    }

    @Test
    public void readFromOlderLedgers() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
        cursor.readEntries(1);
        assertEquals(cursor.hasMoreEntries(), true);
        cursor.readEntries(1);
        assertEquals(cursor.hasMoreEntries(), true);
        cursor.readEntries(1);
        assertEquals(cursor.hasMoreEntries(), false);
    }

    @Test
    public void triggerLedgerDeletion() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(ledger.getNumberOfEntries(), 3);

        assertEquals(cursor.hasMoreEntries(), true);
        entries = cursor.readEntries(1);
        assertEquals(cursor.hasMoreEntries(), true);

        cursor.markDelete(entries.get(0).getPosition());
        // A ledger must have been deleted at this point
        assertEquals(ledger.getNumberOfEntries(), 2);

        cursor.readEntries(1);
        assertEquals(cursor.hasMoreEntries(), false);
    }

    @Test
    public void testEmptyManagedLedgerContent() throws Exception {
        ZooKeeper zk = bkc.getZkHandle();
        zk.create("/managed-ledger", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/managed-ledger/my_test_ledger", " ".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
    }

    @Test
    public void testProducerAndNoConsumer() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);

        ledger.addEntry("entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Since there are no consumers, older ledger will be deleted
        // immediately
        ledger.addEntry("entry-2".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);

        ledger.addEntry("entry-3".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
    }

    @Test
    public void testAsyncAddEntryAndSyncClose() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 0);

        final CountDownLatch counter = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.asyncAddEntry(content.getBytes(Encoding), new AddEntryCallback() {
                public void addComplete(Throwable status, Position position, Object ctx) {
                    counter.countDown();
                }
            }, null);
        }

        counter.await();

        assertEquals(ledger.getNumberOfEntries(), 100);
    }

    @Test
    public void moveCursorToNextLedger() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);

        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 2);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 0);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 1);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 1);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 0);
    }
}
