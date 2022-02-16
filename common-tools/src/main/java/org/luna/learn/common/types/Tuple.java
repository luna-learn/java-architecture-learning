package org.luna.learn.common.types;

import java.io.Serializable;

public class Tuple<T1, T2> implements Serializable {
    public final T1 _1;
    public final T2 _2;

    Tuple(T1 _1, T2 _2) {
        this._1 = _1;
        this._2 = _2;
    }

    public static <T1, T2> Tuple<T1, T2> of(T1 _1, T2 _2) {
        return new Tuple<>(_1, _2);
    }
}