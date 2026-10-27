package com.miller;

/* Этап 1: общая переменная. Перед каждым get/set — точка переключения (пауза планировщика),
 * после — строка в лог: чтение "<имя> -> <значение>", запись "<имя> = <значение>".
 * getAcquire/setRelease для планировщика ведут себя так же, как get/set. */
public final class Shared<T> {
    private final String name;
    private T value;

    public Shared(String name, T value) { this.name = name; this.value = value; }

    /** Чтение с паузой перед ним. */
    public T get() {
        Scheduler.beforeSharedOperation();
        T result = value;
        Scheduler.afterSharedRead(name, result);

        return result;
    }

    /** Запись с паузой перед ней. */
    public void set(T newValue) {
        Scheduler.beforeSharedOperation();
        value = newValue;
        Scheduler.afterSharedWrite(name, newValue);
    }

    /** То же, что get(): планировщик не моделирует порядки памяти. */
    public T getAcquire() {
        return get();
    }

    /** То же, что set(v). */
    public void setRelease(T newValue) {
        set(newValue);
    }

    /** Чтение без паузы и без лога: для проверок после прогона и условий await. */
    public T peek() {
        return value;
    }
}
