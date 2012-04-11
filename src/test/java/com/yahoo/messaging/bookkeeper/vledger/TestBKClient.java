package com.yahoo.messaging.bookkeeper.vledger;

import java.io.IOException;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;

public class TestBKClient {

    private static final String SECRET = "secret";

    /**
     * @param args
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {

        ClientConfiguration config = new ClientConfiguration();
        config.setZkServers("localhost:2181");
        BookKeeper bookKeeper = new BookKeeper(config);
        LedgerHandle ledger = bookKeeper.createLedger(DigestType.MAC, SECRET.getBytes());
        long ledgerId = ledger.getId();
        System.out.println("Writing to ledger: " + ledgerId);

        for (int i = 0; i < 10; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes("UTF-8"));
        }

        ledger.close();

        ledger = bookKeeper.openLedger(ledgerId, DigestType.MAC, SECRET.getBytes());

        Enumeration<LedgerEntry> entries = ledger.readEntries(0, 9);
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String content = new String(entry.getEntry(), "UTF-8");
            System.out.println("Entry " + entry.getEntryId() + " lenght=" + entry.getLength()
                    + " content='" + content + "'");
        }

        ledger.close();
        bookKeeper.close();
    }

}
