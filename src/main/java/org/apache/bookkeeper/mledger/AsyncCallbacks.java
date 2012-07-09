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

import java.util.List;

/**
 * Definition of all the callbacks used for the ManagedLedger asynchronous API.
 * 
 */
public interface AsyncCallbacks {
    public interface ManagedLedgerCallback<T> {
        public void operationComplete(T result);
        public void operationFailed(ManagedLedgerException e);
    }

    public interface OpenLedgerCallback {
        public void openLedgerComplete(ManagedLedgerException status, ManagedLedger ledger, Object ctx);
    }

    public interface DeleteLedgerCallback {
        public void deleteLedgerComplete(ManagedLedgerException status, Object ctx);
    }

    public interface OpenCursorCallback {
        public void openCursorComplete(ManagedLedgerException status, ManagedCursor cursor, Object ctx);
    }

    public interface AddEntryCallback {
        public void addComplete(ManagedLedgerException status, Position position, Object ctx);
    }

    public interface CloseCallback {
        public void closeComplete(ManagedLedgerException status, Object ctx);
    }

    public interface ReadEntriesCallback {
        public void readEntriesComplete(ManagedLedgerException status, List<Entry> entries, Object ctx);
    }

    public interface MarkDeleteCallback {
        public void markDeleteComplete(ManagedLedgerException status, Object ctx);
    }

}
