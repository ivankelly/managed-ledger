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
package org.apache.bookkeeper.mledger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.apache.bookkeeper.mledger.Position;
import org.testng.annotations.Test;

public class PositionTest {
    @Test(expectedExceptions = NullPointerException.class)
    public void nullParam() {
        new Position(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidArg1() {
        new Position("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidArg2() {
        new Position("xxx");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidArg3() {
        new Position("1:");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidArg4() {
        new Position("1:x");
    }

    @Test
    public void simpleTest() {
        Position pos = new Position("1:2");
        assertEquals(pos.getLedgerId(), 1);
        assertEquals(pos.getEntryId(), 2);
        assertEquals(pos, new Position(1, 2));

        assertFalse(pos.equals(new Position(1, 3)));
        assertFalse(pos.equals(new Position(3, 2)));
        assertFalse(pos.equals("1:2"));
    }

    @Test
    public void comparisons() {
        Position pos1_1 = new Position(1, 1);
        Position pos2_5 = new Position(2, 5);
        Position pos10_0 = new Position(10, 0);
        Position pos10_1 = new Position(10, 1);

        assertEquals(0, pos1_1.compareTo(pos1_1));
        assertEquals(-1, pos1_1.compareTo(pos2_5));
        assertEquals(-1, pos1_1.compareTo(pos10_0));
        assertEquals(-1, pos1_1.compareTo(pos10_1));

        assertEquals(+1, pos2_5.compareTo(pos1_1));
        assertEquals(0, pos2_5.compareTo(pos2_5));
        assertEquals(-1, pos2_5.compareTo(pos10_0));
        assertEquals(-1, pos2_5.compareTo(pos10_1));

        assertEquals(+1, pos10_0.compareTo(pos1_1));
        assertEquals(+1, pos10_0.compareTo(pos2_5));
        assertEquals(0, pos10_0.compareTo(pos10_0));
        assertEquals(-1, pos10_0.compareTo(pos10_1));

        assertEquals(+1, pos10_1.compareTo(pos1_1));
        assertEquals(+1, pos10_1.compareTo(pos2_5));
        assertEquals(+1, pos10_1.compareTo(pos10_0));
        assertEquals(0, pos10_1.compareTo(pos10_1));
    }

    @Test
    public void hashes() {
        assertEquals(new Position(5, 5).hashCode(), new Position("5:5").hashCode());
    }
}
