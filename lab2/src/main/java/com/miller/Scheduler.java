package com.miller;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;

/* Этап 1: планировщик, который ведёт настоящие потоки строго по одному (передача хода через семафоры).
 * Этап 2: обнаружение deadlock в главном цикле и корректное завершение застрявших потоков.
 * Внутреннее устройство (свои классы, семафоры, вспомогательные методы для Shared и SchedLock) придумываете сами. */
public final class Scheduler {
    /** Если true, каждая операция с общими данными добавляет строку в log. Выставляет runner. */
    boolean logging;
    /** Лог прогона: строки "<поток>  <операция>", например "A  lock mu", "A  balance -> 100". */
    final List<String> log = new ArrayList<>();
    /** null, если deadlock не было; иначе "held={a:A, b:B}" (этап 2). */
    String deadlock;

    private final List<Worker> workers = new ArrayList<>();
    private final Semaphore schedulerSignal = new Semaphore(0);
    private final StringBuilder trace = new StringBuilder();

    private static final ThreadLocal<Worker> currentWorker =
            new ThreadLocal<>();

    private enum State {
        NEW, RUNNING, PAUSED, WAITING, DONE
    }

    private final class Worker {
        final int id;
        final Runnable body;
        final Semaphore permit = new Semaphore(0);

        Thread thread;
        volatile State state = State.NEW;
        BooleanSupplier condition;
        boolean touchedSincePause;
        Throwable failure;

        Worker(int id, Runnable body) {
            this.id = id;
            this.body = body;
        }

        Scheduler getScheduler() {
            return Scheduler.this;
        }
    }

    public Scheduler() {
    }

    /** Зарегистрировать поток. Потоки получают буквы A, B, C… в порядке вызова spawn. */
    public void spawn(Runnable body) {
        Objects.requireNonNull(body, "body");

        Worker worker = new Worker(workers.size(), body);

        worker.thread = new Thread(() -> {
            currentWorker.set(worker);

            try {
                worker.state = State.PAUSED;
                schedulerSignal.release();

                worker.permit.acquire();

                worker.state = State.RUNNING;
                worker.body.run();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                worker.failure = e;

            } catch (Throwable e) {
                worker.failure = e;

            } finally {
                worker.state = State.DONE;
                schedulerSignal.release();
                currentWorker.remove();
            }
        }, "worker-" + (char) ('A' + worker.id));

        workers.add(worker);
    }

    /** Запустить потоки и вести их по одному: на каждой паузе спросить strategy.pick(enabled, last). */
    public void run(Strategy strategy) {
        Objects.requireNonNull(strategy, "strategy");

        boolean completed = false;

        try {
            for (Worker worker : workers) {
                worker.thread.start();
            }

            for (int i = 0; i < workers.size(); i++) {
                waitForSignal();
            }

            int last = -1;

            while (true) {
                List<Integer> enabled = new ArrayList<>();
                boolean allDone = true;

                for (Worker worker : workers) {
                    if (worker.state != State.DONE) {
                        allDone = false;
                    }

                    if (worker.state == State.WAITING && worker.condition.getAsBoolean()) {
                        worker.state = State.PAUSED;
                    }

                    if (worker.state == State.PAUSED) {
                        enabled.add(worker.id);
                    }
                }

                if (allDone) {
                    break;
                }

                if (enabled.isEmpty()) {
                    throw new IllegalStateException("No enabled workers, but some workers are not DONE");
                }

                int next = strategy.pick(enabled, last);

                if (!enabled.contains(next)) {
                    throw new IllegalStateException("Strategy selected a worker that is not enabled: " + next);
                }

                Worker selected = workers.get(next);
                trace.append((char) ('A' + next));
                last = next;

                selected.state = State.RUNNING;
                selected.permit.release();
                waitForSignal();

                if (selected.failure != null) {
                    throw new IllegalStateException("Worker " + (char) ('A' + next) + " failed", selected.failure);
                }
            }

            completed = true;

        } finally {
            if (!completed) {
                for (Worker worker : workers) {
                    if (worker.thread.isAlive()) {
                        worker.thread.interrupt();
                    }
                }
            }

            for (Worker worker : workers) {
                if (worker.thread.isAlive()) {
                    try {
                        worker.thread.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while joining workers", e);
                    }
                }
            }
        }
    }

    private void waitForSignal() {
        try {
            schedulerSignal.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Scheduler interrupted", e);
        }
    }

    /** Трасса прогона: буквы потоков, выбранных во всех решениях подряд, например "AAAABB". */
    public String traceString() {
        return trace.toString();
    }

    /** window() из тестовых программ: пауза перед следующей операцией с общими данными обязательна. */
    public static void point() {
        Worker worker = requireCurrentWorker();
        worker.touchedSincePause = true;
        Scheduler scheduler = worker.getScheduler();

        if (scheduler.logging) {
            scheduler.log.add((char) ('A' + worker.id) + "  window");
        }
    }

    /** Ждать, пока cond не станет true. До этого поток не попадает в enabled. */
    public static void await(BooleanSupplier cond) {
        Objects.requireNonNull(cond, "cond");
        Worker worker = requireCurrentWorker();
        boolean ready = cond.getAsBoolean();

        if (worker.touchedSincePause || !ready) {
            worker.condition = cond;

            yieldToScheduler(worker, ready ? State.PAUSED : State.WAITING);
            worker.touchedSincePause = false;
        }

        while (!cond.getAsBoolean()) {
            worker.condition = cond;
            yieldToScheduler(worker, State.WAITING);
        }

        worker.condition = null;
        worker.touchedSincePause = true;
        Scheduler scheduler = worker.getScheduler();

        if (scheduler.logging) {
            scheduler.log.add((char) ('A' + worker.id) + "  await done");
        }
    }

    public static void beforeSharedOperation() {
        Worker worker = requireCurrentWorker();

        if (worker.touchedSincePause) {
            yieldToScheduler(worker);
        }
        worker.touchedSincePause = false;
    }

    public static <T> void afterSharedRead(String name, T value) {
        afterSharedOperation(name, value, "->");
    }

    public static <T> void afterSharedWrite(String name, T value) {
        afterSharedOperation(name, value, "=");
    }

    private static <T> void afterSharedOperation(String name, T value, String operation) {
        Worker worker = requireCurrentWorker();

        worker.touchedSincePause = true;
        Scheduler scheduler = worker.getScheduler();

        if (scheduler.logging) {
            scheduler.log.add((char) ('A' + worker.id) + "  " + name + " " + operation + " " + value);
        }
    }

    private static Worker requireCurrentWorker() {
        Worker worker = currentWorker.get();

        if (worker == null) {
            throw new IllegalStateException("No worker bound to current thread");
        }
        return worker;
    }

    private static void yieldToScheduler(Worker worker) {
        yieldToScheduler(worker, State.PAUSED);
    }

    private static void yieldToScheduler(Worker worker, State waitingState) {
        worker.state = waitingState;

        worker.getScheduler().schedulerSignal.release();

        try {
            worker.permit.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Worker interrupted while waiting for Scheduler", e);
        }

        worker.state = State.RUNNING;
    }
}
