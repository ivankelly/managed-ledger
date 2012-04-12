/**
 * 
 */
package com.yahoo.messaging.bookkeeper.ledger.impl;

import org.apache.bookkeeper.client.LedgerEntry;

import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.Position;

/**
 * 
 */
class EntryImpl implements Entry {

    private final LedgerEntry ledgerEntry;

    EntryImpl(LedgerEntry ledgerEntry) {
        this.ledgerEntry = ledgerEntry;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.Entry#getData()
     */
    @Override
    public byte[] getData() {
        return ledgerEntry.getEntry();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.messaging.bookkeeper.ledger.Entry#getPosition()
     */
    @Override
    public Position getPosition() {
        return new Position(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId());
    }

}
