package com.yahoo.messaging.bookkeeper.ledger;

import java.util.List;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedger;
import com.yahoo.messaging.bookkeeper.ledger.ManagedLedgerFactory;
import com.yahoo.messaging.bookkeeper.ledger.Position;

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

            List<LedgerEntry> entries = cursor.readEntries(20);
            log.debug("Read {} entries", entries.size());

            for (LedgerEntry entry : entries) {
                log.info("Read entry. Position={} Content='{}'", new Position(entry), new String(
                        entry.getEntry()));
            }

            // Acknowledge only on last entry
            LedgerEntry lastEntry = entries.get(entries.size() - 1);
            cursor.markDelete(lastEntry);

            log.info("-----------------------");
        }

        log.info("Finished reading entries");

        ledger.close();
    }

}
