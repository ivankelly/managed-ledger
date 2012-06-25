package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

public class ManagedLedgerSingleBookieTest extends BookKeeperClusterTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    public ManagedLedgerSingleBookieTest() {
        // Just one bookie
        super(1);
    }

    @Test(timeOut = 3000)
    public void simple() throws Exception {
        String zookeeperQuorum = bkc.getConf().getZkServers();
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(zookeeperQuorum);

        ManagedLedgerConfig config = new ManagedLedgerConfig().setEnsembleSize(1).setQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

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
}
