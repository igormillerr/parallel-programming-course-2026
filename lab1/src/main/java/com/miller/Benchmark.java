package com.miller;

public interface Benchmark {

    double measurePoint(MetricsCollector collector, long[] values, int threadsCount) throws InterruptedException;

}
