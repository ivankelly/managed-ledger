package com.yahoo.messaging.bookkeeper.ledger;

import java.util.List;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerTest extends BookKeeperClusterTestCase {

    private static Logger log = LoggerFactory.getLogger(ManagedLedgerTest.class);

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

}
