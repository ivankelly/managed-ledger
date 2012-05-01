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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.MarkDeleteCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.ReadEntriesCallback;
import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedger;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory;

public class ManagedCursorTest extends BookKeeperClusterTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    @Test(timeOut = 3000)
    void asyncReadWithoutErrors() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CyclicBarrier barrier = new CyclicBarrier(2);

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(Throwable status, List<Entry> entries, Object ctx) {
                assertNull(ctx);
                assertNull(status);
                assertEquals(entries.size(), 1);

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
    void asyncReadWithErrors() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CyclicBarrier barrier = new CyclicBarrier(2);

        stopBKCluster();

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(Throwable status, List<Entry> entries, Object ctx) {
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
    void asyncMarkDeleteWithErrors() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        List<Entry> entries = cursor.readEntries(100);

        stopZKCluster();

        final CyclicBarrier barrier = new CyclicBarrier(2);

        assertEquals(entries.size(), 1);

        cursor.asyncMarkDelete(entries.get(0).getPosition(), new MarkDeleteCallback() {
            public void markDeleteComplete(Throwable status, Object ctx) {
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

        log.info("Cursor state: {}", cursor);
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorTest.class);
}
