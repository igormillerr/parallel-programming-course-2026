package com.miller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class DefaultMetricsCollector implements MetricsCollector {

    private final List<ThreadState> allStates = new ArrayList<>();

    private final Object listLock = new Object();

    @Override
    public void record(long value) {
        ThreadState state = myState.get();
        int bucket = (int) Math.min(value / 4, 255);

        state.buckets.setRelease(bucket, state.buckets.getPlain(bucket) + 1);
        state.count.setRelease(state.count.getPlain() + 1);
        state.sum.setRelease(state.sum.getPlain() + value);

        if (value < state.min.getPlain()) state.min.setRelease(value);
        if (value > state.max.getPlain()) state.max.setRelease(value);
    }

    @Override
    public Snapshot snapshot() {
        long[] out = new long[256];
        long count = 0, sum = 0, min = Long.MAX_VALUE, max = 0;
        List<ThreadState> copyOfStates;
        synchronized (listLock) {
            copyOfStates = new ArrayList<>(allStates);
        }

        for (ThreadState s : copyOfStates) {
            for (int i = 0; i < 256; i++) {
                out[i] += s.buckets.get(i);
            }
            count += s.count.get();
            sum += s.sum.get();
            min = Math.min(min, s.min.get());
            max = Math.max(max, s.max.get());
        }

        long p50 = computePercentile(out, count, 0.50);
        long p99 = computePercentile(out, count, 0.99);
        return new Snapshot(out, count, sum, min, max, p50, p99);
    }

    private long computePercentile(long[] buckets, long count, double p) {
        double limit = count * p;
        long accumulated = 0;
        for (int i = 0; i < 256; i++) {
            accumulated += buckets[i];
            if (accumulated >= limit) {
                return i * 4;
            }
        }
        return 0;
    }

    private final ThreadLocal<ThreadState> myState = ThreadLocal.withInitial(() -> {
        ThreadState s = new ThreadState();
        synchronized (listLock) {
            allStates.add(s);
        }
        return s;
    });

    static final class ThreadState {
        final AtomicLongArray buckets = new AtomicLongArray(256);
        final AtomicLong count = new AtomicLong();
        final AtomicLong sum = new AtomicLong();
        final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong max = new AtomicLong(0);
    }

}
