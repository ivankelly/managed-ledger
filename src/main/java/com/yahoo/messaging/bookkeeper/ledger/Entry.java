package com.yahoo.messaging.bookkeeper.ledger;

/**
 * An Entry represent a ledger entry data and its associated position.
 */
public interface Entry {

    /**
     * @return the data
     */
    byte[] getData();

    /**
     * @return the position at which the entry was stored
     */
    Position getPosition();
}
