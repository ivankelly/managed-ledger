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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.util.VarArgs.va;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the life-cycle of an addEntry() operation
 * 
 */
class OpAddEntry implements AddCallback, CloseCallback {
    private final ManagedLedgerImpl ml;
    private LedgerHandle ledger;
    private long entryId;
    protected final byte[] data;
    private final AddEntryCallback callback;
    private final Object ctx;
    private boolean closeWhenDone;

    OpAddEntry(ManagedLedgerImpl ml, byte[] data, AddEntryCallback callback, Object ctx) {
        this.ml = ml;
        this.ledger = null;
        this.data = data;
        this.callback = callback;
        this.ctx = ctx;
        this.closeWhenDone = false;
        this.entryId = -1;
    }

    public void setLedger(LedgerHandle ledger) {
        this.ledger = ledger;
    }

    public void setCloseWhenDone(boolean closeWhenDone) {
        this.closeWhenDone = closeWhenDone;
    }

    public void initiate() {
        ledger.asyncAddEntry(data, this, ctx);
    }

    public void failed(Exception e) {
        callback.addComplete(e, null, ctx);
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        checkArgument(ledger.getId() == lh.getId());
        checkArgument(this.ctx == ctx);

        this.entryId = entryId;

        log.debug("[{}] write-complete: ledger-id={} entry-id={} rc={}", va(ml.getName(), lh.getId(), entryId, rc));
        if (rc == BKException.Code.OK) {
            ml.numberOfEntries.incrementAndGet();
            ml.totalSize.addAndGet(data.length);

            if (closeWhenDone) {
                log.info("[{}] Closing ledger {} for being full", ml.getName(), lh.getId());
                ledger.asyncClose(this, null);
            } else {
                callback.addComplete(null, new Position(lh.getId(), entryId), ctx);
            }
        } else if (rc == BKException.Code.LedgerFencedException) {
            ManagedLedgerException status = new ManagedLedgerFencedException(BKException.create(rc));
            ml.setFenced();
            callback.addComplete(status, null, ctx);
        } else {
            ManagedLedgerException status = new ManagedLedgerException(BKException.create(rc));
            callback.addComplete(status, null, ctx);
        }
    }

    @Override
    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
        checkArgument(ledger.getId() == lh.getId());

        if (rc == BKException.Code.OK) {
            log.debug("Successfuly closed ledger {}", lh.getId());
        } else {
            log.warn("Error when closing ledger {}. Status={}", lh.getId(), BKException.getMessage(rc));
        }

        ml.ledgerClosed(lh);
        callback.addComplete(null, new Position(lh.getId(), entryId), ctx);
    }

    private static final Logger log = LoggerFactory.getLogger(OpAddEntry.class);
}
