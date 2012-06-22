/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import java.util.List;

import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.util.Pair;

/**
 * Interface that describes the operations that the ManagedLedger need to do on
 * the metadata store.
 */
public interface MetaStore {

    public static interface Version {
    }

    public static interface UpdateLedgersIdsCallback {
        void updateLedgersIdsComplete(MetaStoreException status, Version version);
    }

    /**
     * Get the list of ledgers used by the ManagedLedger
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @return a version object and a list of LedgerStats
     * @throws MetaStoreException
     */
    Pair<Version, List<LedgerStat>> getLedgerIds(String ledgerName) throws MetaStoreException;

    /**
     * Update the list of LedgerStats associated with a ManagedLedger
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param ledgerIds
     *            a sequence of LedgerStats
     * @param version
     *            version object associated with current state
     * @throws MetaStoreException
     */
    Version updateLedgersIds(String ledgerName, Iterable<LedgerStat> ledgerIds, Version version)
            throws MetaStoreException;

    /**
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * 
     * @param ledgerIds
     *            a sequence of LedgerStats
     * @param version
     *            version object associated with current state
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context object
     */
    void asyncUpdateLedgerIds(String ledgerName, Iterable<LedgerStat> ledgerIds, Version version,
            UpdateLedgersIdsCallback callback, Object ctx);

    /**
     * Get the list of consumer registered on a ManagedLedger.
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @return a list of Pair<ConsumerId,Position> for the consumers
     * @throws MetaStoreException
     */
    List<Pair<String, Position>> getConsumers(String ledgerName) throws MetaStoreException;

    /**
     * Update the persisted position of a consumer
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param consumerName
     * @param position
     * @throws MetaStoreException
     */
    void updateConsumer(String ledgerName, String consumerName, Position position) throws MetaStoreException;

    /**
     * Drop the persistent state of a consumer from the metadata store
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param consumerName
     *            the consumer name
     * @throws MetaStoreException
     */
    void removeConsumer(String ledgerName, String consumerName) throws MetaStoreException;

    /**
     * Drop the persistent state for the ManagedLedger and all its associated
     * consumers.
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @throws MetaStoreException
     */
    void removeManagedLedger(String ledgerName) throws MetaStoreException;
}
