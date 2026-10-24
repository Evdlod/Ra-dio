package com.example.directtest.fast;

import com.example.directtest.P2pConfig;

/**
 * Окна синхронизированного поиска (A3).
 *
 * <p>Идея, перенесённая с Apple AWDL (без NAN и Bluetooth): если обе стороны
 * договорились искать в одни и те же моменты времени, «встреча» перестаёт
 * быть случайной. Обычный цикл «каждые 4–8 секунд» даёт в среднем половину
 * периода ожидания; выровненные окна дают верхнюю границу, равную периоду.
 *
 * <p>Границы окон считаются от эпохи (UTC), поэтому оба телефона получают
 * одинаковые окна без какой-либо договорённости. Дополнительно можно
 * сдвинуть границу на известное расхождение часов
 * ({@link PeerClock#offset(String)}), если на одном из телефонов время убежало.
 *
 * <p>Сейчас используется как приоритет: внутри окна поиск запускается сразу
 * (без общего троттлинга), вне окна — только если есть работа (ожидание ACK
 * или свежие сообщения).
 */
public final class SyncWindow {

    private SyncWindow() {
    }

    /** Позиция внутри периода (мс). */
    public static long phase(long utcMs) {
        long period = Math.max(1, P2pConfig.SYNC_WINDOW_PERIOD_MS);
        long p = utcMs % period;
        return p < 0 ? p + period : p;
    }

    /** Открыто ли окно в момент {@code utcMs}. */
    public static boolean isOpen(long utcMs) {
        return phase(utcMs) < P2pConfig.SYNC_WINDOW_LENGTH_MS;
    }

    /** Сколько мс осталось до конца текущего окна (0, если окно закрыто). */
    public static long msUntilClose(long utcMs) {
        if (!isOpen(utcMs)) return 0;
        return P2pConfig.SYNC_WINDOW_LENGTH_MS - phase(utcMs);
    }

    /** Сколько мс до открытия следующего окна (0, если окно уже открыто). */
    public static long msUntilOpen(long utcMs) {
        if (isOpen(utcMs)) return 0;
        return P2pConfig.SYNC_WINDOW_PERIOD_MS - phase(utcMs);
    }

    /** Ближайшая граница начала окна (UTC, мс). */
    public static long nextBoundary(long utcMs) {
        return utcMs + msUntilOpen(utcMs);
    }

    /** Текстовое представление для лога: {@code open|closed@phase}. */
    public static String describe(long utcMs) {
        long p = phase(utcMs);
        return (isOpen(utcMs) ? "open" : "closed") + "@" + p;
    }
}
