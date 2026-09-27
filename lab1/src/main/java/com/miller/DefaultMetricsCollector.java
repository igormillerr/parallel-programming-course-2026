package com.miller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMetricsCollector implements MetricsCollector {

    private final List<ThreadBuffers> allStates = new ArrayList<>();

    private final Object listLock = new Object();

    private volatile int active = 0;

    private long[] globalBuckets = new long[256];

    private long globalCount = 0;

    private long globalSum = 0;

    private long globalMin = Long.MAX_VALUE;

    private long globalMax = 0;

    @Override
    public void record(long value) {
        ThreadBuffers buffer = myState.get();
        int b = active;
        buffer.inside.set(b);
        while (true) {
            b = active;
            buffer.inside.set(b);
            if (active == b) {
                break;
            }
            buffer.inside.setRelease(-1);
        }

        int bucket = (int) Math.min(value / 4, 255);
        buffer.buckets[b][bucket]++;
        buffer.count[b]++;
        buffer.sum[b] += value;
        buffer.min[b] = Math.min(buffer.min[b], value);
        buffer.max[b] = Math.max(buffer.max[b], value);

        buffer.inside.setRelease(-1);
    }

    @Override
    public Snapshot snapshot() {
        List<ThreadBuffers> copyOfStates;
        synchronized (listLock) {
            copyOfStates = new ArrayList<>(allStates);
            int old = active;
            active = 1 - old;

            for (ThreadBuffers s : copyOfStates) {
                while (s.inside.get() == old) {
                    Thread.onSpinWait();
                }

                for (int i = 0; i < 256; i++) {
                    globalBuckets[i] += s.buckets[old][i];
                }
                globalCount += s.count[old];
                globalSum += s.sum[old];
                globalMin = Math.min(globalMin, s.min[old]);
                globalMax = Math.max(globalMax, s.max[old]);

                s.count[old] = 0;
                s.sum[old] = 0;
                Arrays.fill(s.buckets[old], 0);
                s.min[old] = Long.MAX_VALUE;
                s.max[old] = 0;
            }

            long p50 = computePercentile(globalBuckets, globalCount, 0.50);
            long p99 = computePercentile(globalBuckets, globalCount, 0.99);
            return new Snapshot(globalBuckets.clone(), globalCount, globalSum, globalMin, globalMax, p50, p99);
        }
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

    private final ThreadLocal<ThreadBuffers> myState = ThreadLocal.withInitial(() -> {
        ThreadBuffers s = new ThreadBuffers();
        synchronized (listLock) {
            allStates.add(s);
        }
        return s;
    });

    static final class ThreadBuffers {
        final long[][] buckets = new long[2][256];
        final long[] count = new long[2];
        final long[] sum = new long[2];
        final long[] min = {Long.MAX_VALUE, Long.MAX_VALUE};
        final long[] max = {0, 0};

        final AtomicInteger inside = new AtomicInteger(-1);
    }

}
