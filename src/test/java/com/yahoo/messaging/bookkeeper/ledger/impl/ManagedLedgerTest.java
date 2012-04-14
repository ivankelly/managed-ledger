package com.yahoo.messaging.bookkeeper.ledger.impl;

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedger;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory;

public class ManagedLedgerTest extends BookKeeperClusterTestCase {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTest.class);

    private static final Charset Encoding = Charsets.UTF_8;

    @Test
    public void managedLedgerApi() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactory(bkc.getZkHandle(), bkc);

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
            cursor.markDelete(lastEntry);

            log.info("-----------------------");
        }

        log.info("Finished reading entries");

        ledger.close();
    }

    @Test
    public void simple() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactory(bkc.getZkHandle(), bkc);

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
        ManagedLedgerFactory factory = new ManagedLedgerFactory(bkc.getZkHandle(), bkc);

        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger

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
        ManagedLedgerFactory factory = new ManagedLedgerFactory(bkc.getZkHandle(), bkc);

        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertEquals(cursor.hasMoreEntries(), true);

        List<Entry> entries = cursor.readEntries(2);
        assertEquals(entries.size(), 2);
        assertEquals(cursor.hasMoreEntries(), false);

        cursor.markDelete(entries.get(0));

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
}
