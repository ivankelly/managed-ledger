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
package com.yahoo.messaging.bookkeeper.ledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.AddEntryCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.MarkDeleteCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenCursorCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.OpenLedgerCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.ReadEntriesCallback;
import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedger;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerConfig;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory;
import com.yahoo.messaging.bookkeeper.ledger.Position;
import com.yahoo.messaging.bookkeeper.ledger.util.Pair;

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
                log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(
                        entry.getData()));
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
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

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
                    public void openCursorComplete(Throwable status, ManagedCursor cursor,
                            Object ctx) {
                        assertNull(status);
                        ManagedLedger ledger = (ManagedLedger) ctx;

                        ledger.asyncAddEntry("test".getBytes(Encoding), new AddEntryCallback() {
                            public void addComplete(Throwable status, Object ctx) {
                                assertNull(status);

                                @SuppressWarnings("unchecked")
                                Pair<ManagedLedger, ManagedCursor> pair = (Pair<ManagedLedger, ManagedCursor>) ctx;
                                ManagedLedger ledger = pair.first;
                                ManagedCursor cursor = pair.second;

                                assertEquals(ledger.getNumberOfEntries(), 1);
                                assertEquals(ledger.getTotalSize(),
                                        "test".getBytes(Encoding).length);

                                cursor.asyncReadEntries(2, new ReadEntriesCallback() {
                                    public void readEntriesComplete(Throwable status,
                                            List<Entry> entries, Object ctx) {
                                        assertNull(status);
                                        ManagedCursor cursor = (ManagedCursor) ctx;

                                        assertEquals(entries.size(), 1);
                                        Entry entry = entries.get(0);
                                        assertEquals(new String(entry.getData(), Encoding), "test");

                                        cursor.asyncMarkDelete(entry.getPosition(),
                                                new MarkDeleteCallback() {
                                                    public void markDeleteComplete(
                                                            Throwable status, Object ctx) {
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

}
