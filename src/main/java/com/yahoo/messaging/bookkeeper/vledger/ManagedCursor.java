package com.yahoo.messaging.bookkeeper.vledger;

import java.util.List;

import org.apache.bookkeeper.client.LedgerEntry;

public interface ManagedCursor {

    public List<LedgerEntry> readEntries(int numberOfEntriesToRead) throws Exception;

    public boolean hasMoreEntries();

    public void acknowledge(LedgerEntry entry) throws Exception;

}
