package com.yahoo.messaging.bookkeeper.ledger.impl;

import java.util.List;

import com.yahoo.messaging.bookkeeper.ledger.Position;
import com.yahoo.messaging.bookkeeper.ledger.util.Pair;

public interface MetaStore {
    List<LedgerStat> getLedgerIds(String ledgerName) throws Exception;

    void updateLedgersIds(String ledgerName, Iterable<LedgerStat> ledgerIds) throws Exception;

    List<Pair<String, Position>> getConsumers(String ledgerName) throws Exception;

    void updateConsumer(String ledgerName, String consumerName, Position position) throws Exception;
}
