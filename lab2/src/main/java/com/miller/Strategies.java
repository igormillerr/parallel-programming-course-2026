package com.miller;

import java.util.List;

/* Стратегии выбора следующего потока.
 * FirstAlive — этап 1, Replay — этап 2, RandomPick — этап 3, Dfs — этап 4 (ограничение bound — этап 5).
 * enabled — номера потоков, которые могут сделать шаг, по возрастанию; last — прошлый выбранный (-1 в начале). */
public final class Strategies {
    private Strategies() { }

    /** Всегда первый поток из enabled. */
    public static final class FirstAlive implements Strategy {
        public int pick(List<Integer> enabled, int last) {
            return enabled.getFirst();
        }
    }

    /** Проигрывает трассу: на i-м решении выбирает поток trace[i] ('A' -> 0, 'B' -> 1). */
    public static final class Replay implements Strategy {
        private final String trace;
        private int position;

        public Replay(String trace) {
            this.trace = java.util.Objects.requireNonNull(trace);
        }

        /** Сбросить позицию в трассе перед прогоном. */
        @Override
        public void beginRun() {
            position = 0;
        }

        @Override
        public int pick(List<Integer> enabled, int last) {
            if (position >= trace.length()) {
                throw new IllegalStateException("Replay trace ended at position " + position);
            }

            char letter = trace.charAt(position++);
            int workerId = letter - 'A';
            if (!enabled.contains(workerId)) {
                throw new IllegalStateException("Replay worker " + letter + " is not enabled. Enabled: " + enabled);
            }

            return workerId;
        }
    }

    /** Случайный поток: enabled.get(rnd.nextInt(enabled.size())), rnd = new java.util.Random(seed). */
    public static final class RandomPick implements Strategy {
        private final java.util.Random rnd;

        public RandomPick(long seed) {
            this.rnd = new java.util.Random(seed);
        }

        @Override
        public int pick(List<Integer> enabled, int last) {
            return enabled.get(rnd.nextInt(enabled.size()));
        }
    }

    /** Stateless DFS по дереву выборов: каждый прогон с нуля, повторяет префикс прошлого и берёт следующий вариант.
     *  bound — максимум вытеснений (этап 5); для dfs runner передаёт Integer.MAX_VALUE. */
    public static final class Dfs implements Strategy {
        private final List<List<Integer>> alternatives = new java.util.ArrayList<>();
        private final List<Integer> selectedIndices = new java.util.ArrayList<>();
        private int depth;

        private final int bound;
        private int preemptions;

        public Dfs(int bound) {
            if (bound < 0) {
                throw new IllegalArgumentException("bound must be nonnegative");
            }

            this.bound = bound;
        }

        /** Подготовиться к новому прогону (позиция в пути, счётчик вытеснений). */
        @Override
        public void beginRun() {
            depth = 0;
            preemptions = 0;
        }

        @Override
        public int pick(List<Integer> enabled, int last) {
            boolean canPreempt = last >= 0 && enabled.contains(last);
            List<Integer> options;

            if (canPreempt && preemptions >= bound) {
                options = List.of(last);
            } else {
                options = List.copyOf(enabled);
            }

            if (depth == alternatives.size()) {
                alternatives.add(options);
                selectedIndices.add(0);

            } else if (!alternatives.get(depth).equals(options)) {
                throw new IllegalStateException("Enabled workers differ at decision " + depth);
            }

            int worker = alternatives.get(depth).get(selectedIndices.get(depth));

            if (canPreempt && worker != last) {
                preemptions++;
            }
            depth++;
            return worker;
        }

        /** Выбрать следующий путь; false — дерево обойдено. */
        @Override
        public boolean endRun() {
            while (alternatives.size() > depth) {
                alternatives.removeLast();
                selectedIndices.removeLast();
            }

            for (int i = depth - 1; i >= 0; i--) {
                int next = selectedIndices.get(i) + 1;

                if (next < alternatives.get(i).size()) {
                    selectedIndices.set(i, next);

                    while (alternatives.size() > i + 1) {
                        alternatives.removeLast();
                        selectedIndices.removeLast();
                    }

                    return true;
                }
            }

            return false;
        }
    }
}
