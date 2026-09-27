package com.miller;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        LoadGenerator generator = new LoadGenerator();
        long[] values = generator.generate();

        MetricsCollector collector = new DefaultMetricsCollector();
        Benchmark benchmark = new DefaultBenchmark();

        double result = benchmark.measurePoint(collector, values, 1);
//        for (int i = 0; i < 5; i++) {
//            System.out.println("Launch " + i + " result: " + result[i]);
//        }
//        System.out.println(collector.snapshot());
        System.out.printf("Baseline: %.2f M ops/sec%n", result / 1_000_000.0);
    }
}