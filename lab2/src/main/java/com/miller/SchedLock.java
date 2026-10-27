package com.miller;

/* Этап 2: лок, о котором знает планировщик. Поток, ждущий занятый лок, не попадает в enabled;
 * обычный synchronized / ReentrantLock здесь нельзя — поток уснёт с ходом в руках и всё зависнет.
 * Лог: "lock <имя>", "unlock <имя>". Планировщик должен знать все локи, чтобы печатать held={a:A, b:B}. */
public final class SchedLock {
    final String name;
    final Scheduler scheduler;

    int ownerId = -1;

    /** Создаётся в setup() тестовой программы; зарегистрируйте лок в планировщике s. */
    public SchedLock(Scheduler s, String name) {
        this.scheduler = s;
        this.name = name;

        scheduler.registerLock(this);
    }

    /** Пауза; если лок занят — ждать, не участвуя в выборе; затем стать владельцем. */
    public void lock() {
        scheduler.beforeLockOperation(this);
        int workerId = scheduler.currentWorkerId();

        if (ownerId == workerId) {
            throw new IllegalStateException("Worker " + (char) ('A' + workerId) + " already owns lock " + name);
        }

        while (!isFree()) {
            scheduler.waitForLock(this);
        }

        ownerId = workerId;
        scheduler.clearWaitingLock();
        scheduler.afterLockOperation();

        if (scheduler.logging) {
            scheduler.log.add((char) ('A' + workerId) + "  lock " + name);
        }
    }

    /** Пауза; освободить лок, ждавшие потоки снова могут быть выбраны. */
    public void unlock() {
        scheduler.beforeLockOperation();

        int workerId = scheduler.currentWorkerId();

        if (ownerId != workerId) {
            throw new IllegalMonitorStateException("Worker " + (char) ('A' + workerId) + " does not own lock " + name);
        }

        ownerId = -1;
        scheduler.afterLockOperation();

        if (scheduler.logging) {
            scheduler.log.add((char) ('A' + workerId) + "  unlock " + name);
        }
    }

    boolean isFree() {
        return ownerId == -1;
    }
}
