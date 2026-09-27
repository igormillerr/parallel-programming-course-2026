package com.miller;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public class DefaultBenchmark implements Benchmark {

    @Override
    public double measurePoint(MetricsCollector collector, long[] values, int threadsCount) throws InterruptedException {
        run(collector, values,threadsCount, 5);

        double[] results = new double[5];
        for (int i = 0; i < 5; i++) {
            results[i] = run(collector, values, threadsCount, 5);
        }
        System.out.println(collector.snapshot().count());
        Arrays.sort(results);
        return results[2];
    }

    private double run(MetricsCollector collector, long[] values, int threadsCount, int second) throws InterruptedException {

        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stop = new AtomicBoolean(false);
        long[] ops = new long[threadsCount];

        Thread[] workers = new Thread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            int k = i;
            workers[i] = new Thread(() -> {
                try {
                    long localCount = 0;
                    int j = k * 1000;
                    start.await();

                    while (!stop.get()) {
                        collector.record(values[j]);
                        localCount++;
                        j++;

                        if (j == values.length) {
                            j = 0;
                        }
                    }
                    ops[k] = localCount;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            workers[i].start();
        }

        long t0 = System.nanoTime();
        start.countDown();
        sleep(second * 1000L);
        stop.set(true);
        long t1 = System.nanoTime();
        double elapsedSeconds = (t1 - t0) / 1_000_000_000.0;

        for (Thread worker : workers) {
            worker.join();
        }

        long totalOps = 0;
        for (long op : ops) {
            totalOps += op;
        }

        return totalOps / elapsedSeconds;
    }

}
