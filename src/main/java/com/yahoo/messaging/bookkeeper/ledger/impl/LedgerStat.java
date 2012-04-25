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
package com.yahoo.messaging.bookkeeper.ledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.primitives.Longs;

/**
 * LedgerStat holds a tuple of (LedgerId, EntriesCount, Size)
 */
public class LedgerStat implements Comparable<LedgerStat> {
    private final long ledgerId;
    private final long entriesCount;
    private final long size;

    private static final Pattern pattern = Pattern.compile("\\((\\d+)\\:(\\d+)\\:(\\d+)\\)");

    LedgerStat(long ledgerId, long entriesCount, long size) {
        this.ledgerId = ledgerId;
        this.entriesCount = entriesCount;
        this.size = size;
    }

    public static LedgerStat parseData(String data) {
        checkNotNull(data);

        Matcher m = pattern.matcher(data);
        checkArgument(m.matches(), "LedgerStat format is incorrect");

        return new LedgerStat(Long.parseLong(m.group(1)), Long.parseLong(m.group(2)),
                Long.parseLong(m.group(3)));
    }

    /**
     * @return the ledgerId
     */
    public long getLedgerId() {
        return ledgerId;
    }

    /**
     * @return the entriesCount
     */
    public long getEntriesCount() {
        return entriesCount;
    }

    /**
     * @return the size
     */
    public long getSize() {
        return size;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("(%d:%d:%d)", ledgerId, entriesCount, size);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(LedgerStat other) {
        return Longs.compare(this.ledgerId, other.ledgerId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Longs.hashCode(ledgerId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LedgerStat) {
            LedgerStat other = (LedgerStat) obj;
            return this.ledgerId == other.ledgerId;
        }

        return false;
    }

}
