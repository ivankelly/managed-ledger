package com.yahoo.messaging.bookkeeper.ledger;

import static com.yahoo.messaging.bookkeeper.ledger.util.VarArgs.va;

import java.nio.charset.Charset;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class SimpleBookKeeperTest extends BookKeeperClusterTestCase {

    private static final String SECRET = "secret";
    private static final Charset Encoding = Charsets.UTF_8;

    @Test
    public void simpleTest() throws Exception {

        LedgerHandle ledger = bkc.createLedger(DigestType.MAC, SECRET.getBytes());
        long ledgerId = ledger.getId();
        log.info("Writing to ledger: {}", ledgerId);

        for (int i = 0; i < 10; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes(Encoding));
        }

        ledger.close();

        ledger = bkc.openLedger(ledgerId, DigestType.MAC, SECRET.getBytes());

        Enumeration<LedgerEntry> entries = ledger.readEntries(0, 9);
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String content = new String(entry.getEntry(), Encoding);
            log.info("Entry {}  lenght={} content='{}'",
                    va(entry.getEntryId(), entry.getLength(), content));
        }

        ledger.close();
    }

    private static Logger log = LoggerFactory.getLogger(SimpleBookKeeperTest.class);

}
