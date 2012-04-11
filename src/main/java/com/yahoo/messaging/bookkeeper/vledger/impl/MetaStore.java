package com.yahoo.messaging.bookkeeper.vledger.impl;

import java.util.List;

import com.yahoo.messaging.bookkeeper.vledger.Position;
import com.yahoo.messaging.bookkeeper.vledger.util.Pair;

public interface MetaStore {
    List<Long> getLedgerIds(String ledgerName) throws Exception;

    void updateLedgersIds(String ledgerName, Iterable<Long> ledgerIds) throws Exception;

    List<Pair<String, Position>> getConsumers(String ledgerName) throws Exception;

    void updateConsumer(String ledgerName, String consumerName, Position position) throws Exception;
}
