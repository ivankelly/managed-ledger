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
package org.apache.bookkeeper.mledger;

import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;

/**
 * A factory to open/create managed ledgers and delete them.
 * 
 */
public interface ManagedLedgerFactory {

    /**
     * Open a managed ledger. If the managed ledger does not exist, a new one
     * will be automatically created. Uses the default configuration parameters.
     * 
     * @param name
     *            the unique name that identifies the managed ledger
     * @return the managed ledger
     * @throws Exception
     */
    public ManagedLedger open(String name) throws Exception;

    /**
     * Open a managed ledger. If the managed ledger does not exist, a new one
     * will be automatically created.
     * 
     * @param name
     *            the unique name that identifies the managed ledger
     * @param config
     *            managed ledger configuration
     * @return the managed ledger
     * @throws Exception
     */
    public ManagedLedger open(String name, ManagedLedgerConfig config) throws Exception;

    /**
     * Asynchronous open method.
     * 
     * @see #open(String)
     * @param name
     *            the unique name that identifies the managed ledger
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx);

    /**
     * Asynchronous open method.
     * 
     * @see #open(String, ManagedLedgerConfig)
     * @param name
     *            the unique name that identifies the managed ledger
     * @param config
     *            managed ledger configuration
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncOpen(String name, ManagedLedgerConfig config, OpenLedgerCallback callback, Object ctx);

    /**
     * Delete a managed ledger completely from the system. Frees and remove all
     * the associated persisted resources (Ledgers, Cursors).
     * 
     * @param name
     *            the unique name that identifies the managed ledger
     * @throws Exception
     */
    public void delete(String name) throws Exception;

    /**
     * Delete a managed ledger asynchronously.
     * 
     * @see #asyncDelete(String, DeleteLedgerCallback, Object)
     * @param name
     *            name the unique name that identifies the managed ledger
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncDelete(String name, DeleteLedgerCallback callback, Object ctx);
}
