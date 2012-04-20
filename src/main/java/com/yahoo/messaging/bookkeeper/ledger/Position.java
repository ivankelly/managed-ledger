package com.yahoo.messaging.bookkeeper.ledger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * A Position is a pointer to a specific entry into the managed ledger.
 * <p>
 * Specifically a Position is composed of a (LedgerId,EntryId) pair.
 */
public class Position implements Comparable<Position> {

    private final long ledgerId;
    private final long entryId;

    /**
     * 
     * @param text
     *            string serialized position
     * 
     * @throws IllegalArgumentException
     *             if text is not a valid serialized position
     */
    public Position(String text) {
        checkNotNull(text);

        String[] ids = text.split(":");
        checkArgument(ids.length == 2, "Invalid Position text format");

        try {
            ledgerId = Long.parseLong(ids[0]);
            entryId = Long.parseLong(ids[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid position format '" + text + "'", e);
        }
    }

    public Position(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    /**
     * String representation of virtual cursor - LedgerId:EntryId
     */
    @Override
    public String toString() {
        return String.format("%d:%d", ledgerId, entryId);
    }

    @Override
    public int compareTo(Position other) {
        checkNotNull(other);

        return ComparisonChain.start().compare(this.ledgerId, other.ledgerId)
                .compare(this.entryId, other.entryId).result();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(ledgerId, entryId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Position) {
            Position other = (Position) obj;
            return ledgerId == other.ledgerId && entryId == other.entryId;
        }

        return false;
    }

}
