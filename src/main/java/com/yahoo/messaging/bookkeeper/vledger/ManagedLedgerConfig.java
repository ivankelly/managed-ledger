/**
 * 
 */
package com.yahoo.messaging.bookkeeper.vledger;

import org.apache.bookkeeper.client.BookKeeper.DigestType;

import com.google.common.base.Charsets;

/**
 *
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
