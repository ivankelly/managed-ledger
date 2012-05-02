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
package com.yahoo.messaging.bookkeeper.ledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.List;

import org.testng.annotations.Test;

import com.google.inject.internal.Lists;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.MarkDeleteCallback;
import com.yahoo.messaging.bookkeeper.ledger.AsyncCallbacks.ReadEntriesCallback;
import com.yahoo.messaging.bookkeeper.ledger.Entry;
import com.yahoo.messaging.bookkeeper.ledger.ManagedCursor;
import com.yahoo.messaging.bookkeeper.ledger.Position;

@Test
public class ManagedCursorContainerTest {

    private static class MockManagedCursor implements ManagedCursor {

        ManagedCursorContainer container;
        Position position;
        String name;

        public MockManagedCursor(ManagedCursorContainer container, String name, Position position) {
            this.container = container;
            this.name = name;
            this.position = position;
        }

        @Override
        public List<Entry> readEntries(int numberOfEntriesToRead) throws Exception {
            return Lists.newArrayList();
        }

        @Override
        public void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback,
                Object ctx) {
            callback.readEntriesComplete(null, null, ctx);
        }

        @Override
        public boolean hasMoreEntries() {
            return true;
        }

        @Override
        public void markDelete(Position position) throws Exception {
            this.position = position;
            container.cursorUpdated(this);
        }

        @Override
        public void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx) {
            fail();
        }

        @Override
        public Position getMarkDeletedPosition() {
            return position;
        }

        @Override
        public String getName() {
            return name;
        }

        public String toString() {
            return String.format("%s=%s", name, position);
        }
    }

    @Test
    void simple() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();
        assertEquals(container.getSlowestReaderPosition(), null);

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", new Position(5, 5));
        container.add(cursor1);
        assertEquals(container.getSlowestReaderPosition(), new Position(5, 5));

        ManagedCursor cursor2 = new MockManagedCursor(container, "test2", new Position(2, 2));
        container.add(cursor2);
        assertEquals(container.getSlowestReaderPosition(), new Position(2, 2));

        ManagedCursor cursor3 = new MockManagedCursor(container, "test3", new Position(2, 0));
        container.add(cursor3);
        assertEquals(container.getSlowestReaderPosition(), new Position(2, 2));

        ManagedCursor cursor4 = new MockManagedCursor(container, "test4", new Position(4, 0));
        container.add(cursor4);
        assertEquals(container.getSlowestReaderPosition(), new Position(2, 2));

        ManagedCursor cursor5 = new MockManagedCursor(container, "test5", new Position(3, 5));
        container.add(cursor5);
        assertEquals(container.getSlowestReaderPosition(), new Position(2, 2));

        cursor3.markDelete(new Position(3, 0));
        assertEquals(container.getSlowestReaderPosition(), new Position(2, 2));

        cursor2.markDelete(new Position(10, 5));
        assertEquals(container.getSlowestReaderPosition(), new Position(3, 0));

        container.removeCursor(cursor3);
        assertEquals(container.getSlowestReaderPosition(), new Position(3, 5));

        container.removeCursor(cursor2);
        container.removeCursor(cursor5);
        container.removeCursor(cursor1);
        assertEquals(container.getSlowestReaderPosition(), new Position(4, 0));
        container.removeCursor(cursor4);
        assertEquals(container.getSlowestReaderPosition(), null);

        ManagedCursor cursor6 = new MockManagedCursor(container, "test6", new Position(6, 5));
        container.add(cursor6);
        assertEquals(container.getSlowestReaderPosition(), new Position(6, 5));
    }

}
