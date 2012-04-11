package com.yahoo.messaging.bookkeeper.vledger;


public interface ManagedLedger {

    public String getName();
    
    /**
     * 
     * Add a new entry to the end of a managed ledger.
     * 
     * @param data
     *            to be added to the managed ledger
     */
    public void addEntry(byte[] data) throws Exception;
    
    public ManagedCursor openCursor( String name ) throws Exception;

    /**
     * Get the total number of entries for this managed ledger.
     * 
     * @return
     */
    public long getNumberOfEntries() throws Exception;

    /**
     * Get the total sizes in bytes of the managed ledger, without accounting
     * for replicas.
     * 
     * @return total size in bytes
     */
    public long getTotalSize() throws Exception;

    /**
     * close the current virtual ledger.
     */
    public void close() throws Exception;
}
