package com.yahoo.messaging.bookkeeper.ledger.impl;

import java.util.List;

import com.yahoo.messaging.bookkeeper.ledger.Position;
import com.yahoo.messaging.bookkeeper.ledger.util.Pair;

/**
 * Interface that describes the operations that the ManagedLedger need to do on
 * the metadata store.
 */
public interface MetaStore {

    /**
     * Get the list of ledgers used by the ManagedLedger
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @return a list of LedgerStats
     * @throws Exception
     */
    List<LedgerStat> getLedgerIds(String ledgerName) throws Exception;

    /**
     * Update the list of LedgerStats associated with a ManagedLedger
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param ledgerIds
     *            a sequence of LedgerStats
     * @throws Exception
     */
    void updateLedgersIds(String ledgerName, Iterable<LedgerStat> ledgerIds) throws Exception;

    /**
     * Get the list of consumer registered on a ManagedLedger.
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @return a list of Pair<ConsumerId,Position> for the consumers
     * @throws Exception
     */
    List<Pair<String, Position>> getConsumers(String ledgerName) throws Exception;

    /**
     * Update the persisted position of a consumer
     * 
     * @param ledgerName
     * the name of the ManagedLedger
     * @param consumerName
     * @param position
     * @throws Exception
     */
    void updateConsumer(String ledgerName, String consumerName, Position position) throws Exception;
}
