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
package com.yahoo.messaging.bookkeeper.ledger;

import org.apache.bookkeeper.client.BookKeeper.DigestType;

import com.google.common.base.Charsets;

/**
 * Configuration class for a ManagedLedger
 */
public class ManagedLedgerConfig {

    private int ensembleSize = 3;
    private int quorumSize = 2;
    private DigestType digestType = DigestType.MAC;
    private byte[] password = "".getBytes(Charsets.UTF_8);

    /**
     * @return the ensembleSize
     */
    public int getEnsembleSize() {
        return ensembleSize;
    }

    /**
     * @param ensembleSize
     *            the ensembleSize to set
     */
    public ManagedLedgerConfig setEnsembleSize(int ensembleSize) {
        this.ensembleSize = ensembleSize;
        return this;
    }

    /**
     * @return the quorumSize
     */
    public int getQuorumSize() {
        return quorumSize;
    }

    /**
     * @param quorumSize
     *            the quorumSize to set
     */
    public ManagedLedgerConfig setQuorumSize(int quorumSize) {
        this.quorumSize = quorumSize;
        return this;
    }

    /**
     * @return the digestType
     */
    public DigestType getDigestType() {
        return digestType;
    }

    /**
     * @param digestType
     *            the digestType to set
     */
    public ManagedLedgerConfig setDigestType(DigestType digestType) {
        this.digestType = digestType;
        return this;
    }

    /**
     * @return the password
     */
    public byte[] getPassword() {
        return password;
    }

    /**
     * @param password
     *            the password to set
     */
    public ManagedLedgerConfig setPassword(String password) {
        this.password = password.getBytes(Charsets.UTF_8);
        return this;
    }

}
