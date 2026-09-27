package com.miller;

import java.util.Arrays;
import java.util.Random;

public class LoadGenerator {

    private static final int SEED = 18;

    private static final int MAX_VALUE = 1023;

    public long[] generate() {
        double[] weights = new double[MAX_VALUE];
        for (int i = 0; i < MAX_VALUE; i++) {
            weights[i] = 1.0 / Math.pow(i + 1, 1.15);
        }
        double totalWeight = Arrays.stream(weights).sum();

        double cumulative = 0.0;
        double[] cdf = new double[MAX_VALUE];
        for (int i = 0; i < MAX_VALUE; i++) {
            double probability = weights[i] / totalWeight;
            cumulative += probability;
            cdf[i] = cumulative;
        }

        int size = 1 << 20;
        long[] values = new long[size];
        Random random = new Random(SEED);
        for (int i = 0; i < size; i++) {
            double r = random.nextDouble();
            for (int j = 0; j < MAX_VALUE; j++) {
                if (cdf[j] >= r) {
                    values[i] = j + 1;
                    break;
                }
            }
        }

        return values;
    }

}
