package com.miller;

import java.util.concurrent.atomic.AtomicLong;

public class DefaultMetricsCollector implements MetricsCollector {

    private final long[] buckets = new long[256];

    private final Object[] locks = createLocks();

    private final AtomicLong count = new AtomicLong(0);

    private final AtomicLong sum = new AtomicLong(0);

    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);

    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

    @Override
    public void record(long value) {
        int bucket = (int) Math.min(value / 4, 255);
        int stripe = bucket % 16;
        synchronized (locks[stripe]) {
            buckets[bucket]++;
        }

        count.getAndIncrement();
        sum.getAndAdd(value);
        long current;
        do {
            current = min.get();
            if (value >= current) {
                break;
            }
        } while (!min.compareAndSet(current, value));

        do {
            current = max.get();
            if (value <= current) {
                break;
            }
        } while (!max.compareAndSet(current, value));
    }

    @Override
    public Snapshot snapshot() {
        long[] bucketsReplica = new long[256];
        for (int i = 0; i < 16; i++) {
            synchronized (locks[i]) {
                for (int j = i; j < 256; j += 16) {
                    bucketsReplica[j] = buckets[j];
                }
            }
        }

        long countReplica = count.get();
        long sumReplica = sum.get();
        long minReplica = min.get();
        long maxReplica = max.get();
        long p50 = cumulative(bucketsReplica, countReplica, 0.5);
        long p99 = cumulative(bucketsReplica, countReplica, 0.99);

        return new Snapshot(
                bucketsReplica,
                countReplica,
                sumReplica,
                minReplica,
                maxReplica,
                p50,
                p99
        );
    }

    private long cumulative(long[] buckets, long count, double p) {
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

    private static Object[] createLocks() {
        Object[] locks = new Object[16];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
        return locks;
    }

}
