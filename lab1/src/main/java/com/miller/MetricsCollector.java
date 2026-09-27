package com.miller;

public interface MetricsCollector {

    void record(long value);

    Snapshot snapshot();

}
