package com.miller;

public record Snapshot(
        long[] buckets,
        long count,
        long sum,
        long min,
        long max,
        long p50,
        long p99
) {
}
