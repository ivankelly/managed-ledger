package com.yahoo.messaging.bookkeeper.vledger;

import java.util.List;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualLedgerTest extends BookKeeperClusterTestCase {

    private static Logger log = LoggerFactory.getLogger(VirtualLedgerTest.class);

    public VirtualLedgerTest() {
        // Create a 3 bookies cluster
        super(1);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.test.BookKeeperClusterTestCase#tearDown()
     */
    @Override
    public void tearDown() throws Exception {
        log.info("Shutting down bookkeeper cluster");
        super.tearDown();
    }

    @Test
    public void testVirtualLedgerApi() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactory(bkc.getZkHandle(), bkc);

        ManagedLedgerConfig config = new ManagedLedgerConfig().setEnsembleSize(1).setQuorumSize(1);       
        ManagedLedger ledger = factory.open("my_test_ledger3", config);

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
            cursor.acknowledge(lastEntry);

            log.info("-----------------------");
        }

        log.info("Finished reading entries");

        ledger.close();
    }

}
