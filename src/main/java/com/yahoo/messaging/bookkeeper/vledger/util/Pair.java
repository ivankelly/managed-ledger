package com.yahoo.messaging.bookkeeper.vledger.util;

import com.google.common.base.Objects;

public class Pair<A, B> {
    public final A first;
    public final B second;

    public static <X, Y> Pair<X, Y> create(X x, Y y) {
        return new Pair<X, Y>(x, y);
    }

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first, second);
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", first, second);
    }

}