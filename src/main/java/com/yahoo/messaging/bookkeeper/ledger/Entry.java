package com.yahoo.messaging.bookkeeper.ledger;

public interface Entry {
    byte[] getData();

    Position getPosition();
}
