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
 * сдвигать границу на оценку расхождения часов, полученную из TXT-heartbeat,
 * если на одном из телефонов время убежало.
 *
 * <p>Сейчас используется как приоритет: внутри окна поиск запускается сразу
 * (без общего троттлинга), вне окна — только если есть работа (ожидание ACK
 * или свежие сообщения).
 */
public final class SyncWindow {

    private SyncWindow() {
    }

    /** Позиция внутри периода (мс) при периоде по умолчанию. */
    public static long phase(long utcMs) {
        return phase(utcMs, P2pConfig.SYNC_WINDOW_PERIOD_MS);
    }

    /** Позиция внутри произвольного периода (мс). */
    public static long phase(long utcMs, long periodMs) {
        long period = Math.max(1, periodMs);
        long p = utcMs % period;
        return p < 0 ? p + period : p;
    }

    /** Открыто ли окно в момент {@code utcMs} при периоде по умолчанию. */
    public static boolean isOpen(long utcMs) {
        return isOpen(utcMs, P2pConfig.SYNC_WINDOW_PERIOD_MS);
    }

    /**
     * Открыто ли окно при произвольном периоде.
     *
     * Период выбирает вызывающая сторона: частый — когда есть что доставлять,
     * редкий ({@link P2pConfig#SYNC_WINDOW_IDLE_PERIOD_MS}) — в покое, чтобы не
     * дёргать supplicant без нужды.
     */
    public static boolean isOpen(long utcMs, long periodMs) {
        return phase(utcMs, periodMs) < P2pConfig.SYNC_WINDOW_LENGTH_MS;
    }

    /** Сколько мс осталось до конца текущего окна (0, если окно закрыто). */
    public static long msUntilClose(long utcMs, long periodMs) {
        if (!isOpen(utcMs, periodMs)) return 0;
        return P2pConfig.SYNC_WINDOW_LENGTH_MS - phase(utcMs, periodMs);
    }

    /** Сколько мс до открытия следующего окна (0, если окно уже открыто). */
    public static long msUntilOpen(long utcMs, long periodMs) {
        if (isOpen(utcMs, periodMs)) return 0;
        return Math.max(1, periodMs) - phase(utcMs, periodMs);
    }

    /** Ближайшая граница начала окна (UTC, мс). */
    public static long nextBoundary(long utcMs) {
        return utcMs + msUntilOpen(utcMs, P2pConfig.SYNC_WINDOW_PERIOD_MS);
    }

    /** Текстовое представление для лога: {@code open|closed@phase}. */
    public static String describe(long utcMs) {
        return describe(utcMs, P2pConfig.SYNC_WINDOW_PERIOD_MS);
    }

    /** Текстовое представление для произвольного периода. */
    public static String describe(long utcMs, long periodMs) {
        long p = phase(utcMs, periodMs);
        return (isOpen(utcMs, periodMs) ? "open" : "closed") + "@" + p;
    }
}
