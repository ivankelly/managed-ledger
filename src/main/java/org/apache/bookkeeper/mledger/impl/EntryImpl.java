/**
 * Copyright (C) 2012 Yahoo! Inc. All rights reserved.
 *
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
/**
 * 
 */
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;


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
     * @see org.apache.bookkeeper.mledger.Entry#getData()
     */
    @Override
    public byte[] getData() {
        return ledgerEntry.getEntry();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.bookkeeper.mledger.Entry#getPosition()
     */
    @Override
    public Position getPosition() {
        return new Position(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId());
    }

}
