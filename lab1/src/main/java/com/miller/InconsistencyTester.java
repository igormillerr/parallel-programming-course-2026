package com.miller;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class InconsistencyTester {

    private static final int WRITERS_COUNT = 4;

    public void run(MetricsCollector collector) throws InterruptedException {
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] threads = new Thread[WRITERS_COUNT];
        long[] writersOps = new long[WRITERS_COUNT];

        for (int i = 0; i < WRITERS_COUNT; i++) {
            int k = i;
            threads[i] = new Thread(() -> {
                long counter = 0;
                while (!stop.get()) {
                    collector.record(1);
                    counter++;
                }
                writersOps[k] = counter;
            });
            threads[i].start();
        }

        long sumLess = 0;
        long sumEqual = 0;
        long sumGreater = 0;

        for (int i = 0; i < 10_000; i++) {
            Snapshot snapshot = collector.snapshot();
            long count = snapshot.count();
            long sum = Arrays.stream(snapshot.buckets()).sum();

            if (count == sum) {
                sumEqual++;
            } else if (count < sum) {
                sumGreater++;
            } else {
                sumLess++;
            }
        }

        stop.set(true);
        for (Thread thread : threads) {
            thread.join();
        }

        long totalOps = 0;
        for (long op : writersOps) {
            totalOps += op;
        }
        Snapshot snapshot = collector.snapshot();

        System.out.println("sumLess: " + sumLess);
        System.out.println("sumGreater: " + sumGreater);
        System.out.println("p: " + ((sumLess + sumGreater) / 10_000.0 * 100));
        System.out.println("totalOps: " + totalOps);
        System.out.println("snapshotCount: " + snapshot.count());
    }

}
