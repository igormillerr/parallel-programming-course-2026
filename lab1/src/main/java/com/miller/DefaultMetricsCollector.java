package com.miller;

public class DefaultMetricsCollector implements MetricsCollector {

    private final long[] buckets = new long[256];

    private long count = 0;

    private long sum = 0;

    private long min = Long.MAX_VALUE;

    private long max = Long.MIN_VALUE;

    @Override
    public synchronized void record(long value) {
        int bucket = (int) Math.min(value / 4, 255);
        buckets[bucket]++;

        count++;
        sum += value;
        min = Long.min(min, value);
        max = Long.max(max, value);
    }

    @Override
    public synchronized Snapshot snapshot() {
        long[] bucketsReplica = buckets.clone();
        long countReplica = count;
        long sumReplica = sum;
        long minReplica = min;
        long maxReplica = max;
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

}
