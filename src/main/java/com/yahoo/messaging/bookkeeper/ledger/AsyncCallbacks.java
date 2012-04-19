package com.yahoo.messaging.bookkeeper.ledger;

import java.util.List;

public interface AsyncCallbacks {
    public interface OpenLedgerCallback {
        public void openLedgerComplete(Throwable status, ManagedLedger ledger, Object ctx);
    }

    public interface DeleteLedgerCallback {
        public void deleteLedgerComplete(Throwable status, Object ctx);
    }

    public interface OpenCursorCallback {
        public void openCursorComplete(Throwable status, ManagedCursor cursor, Object ctx);
    }

    public interface AddEntryCallback {
        public void addComplete(Throwable status, Object ctx);
    }

    public interface CloseCallback {
        public void closeComplete(Throwable status, Object ctx);
    }

    public interface ReadEntriesCallback {
        public void readEntriesComplete(Throwable status, List<Entry> entries, Object ctx);
    }

    public interface MarkDeleteCallback {
        public void markDeleteComplete(Throwable status, Object ctx);
    }

}
