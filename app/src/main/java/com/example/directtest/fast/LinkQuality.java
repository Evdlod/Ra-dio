package com.example.directtest.fast;

import com.example.directtest.P2pConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Качество канала до каждого собеседника: сглаженный RTT, разброс, потери и
 * производные таймауты (B8).
 *
 * <p>Зачем: константный таймаут 8 с на подключение и 60 с на чтение одинаково
 * плох для всех условий. Через прямой Wi-Fi Direct RTT — единицы
 * миллисекунд, и ждать 8 с, когда собеседник отвалился, бессмысленно: за это
 * время пользователь считает, что «связи нет». И наоборот, при плохом канале
 * слишком короткий таймаут вызывает лишние переподключения.
 *
 * <p>Формула как в TCP: {@code RTO = 4 × SRTT} с ограничениями
 * {@link P2pConfig#ADAPTIVE_RTO_MIN_MS}…{@link P2pConfig#ADAPTIVE_RTO_MAX_MS}.
 */
public final class LinkQuality {

    private static final double ALPHA = 0.125;   // для SRTT
    private static final double BETA = 0.25;     // для вариации

    private static final class Stats {
        double srttMs = -1;
        double rttVarMs = 0;
        long samples;
        long lost;
        long sent;
    }

    private final Map<String, Stats> stats = new ConcurrentHashMap<>();

    private Stats stats(String peer) {
        return stats.computeIfAbsent(peer != null ? peer : "-", k -> new Stats());
    }

    /** Учесть измеренный RTT по каналу. */
    public void onRttSample(String peer, long rttMs) {
        if (peer == null || rttMs < 0) return;
        Stats s = stats(peer);
        synchronized (s) {
            if (s.srttMs < 0) {
                s.srttMs = rttMs;
                s.rttVarMs = rttMs / 2.0;
            } else {
                double diff = Math.abs(s.srttMs - rttMs);
                s.srttMs = (1 - ALPHA) * s.srttMs + ALPHA * rttMs;
                s.rttVarMs = (1 - BETA) * s.rttVarMs + BETA * diff;
            }
            s.samples++;
        }
    }

    public void onSent(String peer) {
        Stats s = stats(peer);
        synchronized (s) {
            s.sent++;
        }
    }

    public void onLost(String peer) {
        Stats s = stats(peer);
        synchronized (s) {
            s.lost++;
        }
    }

    /** Сглаженный RTT (мс) или -1, если замеров не было. */
    public long srtt(String peer) {
        Stats s = stats.get(peer);
        if (s == null) return -1;
        synchronized (s) {
            return s.srttMs < 0 ? -1 : Math.round(s.srttMs);
        }
    }

    public long rttVar(String peer) {
        Stats s = stats.get(peer);
        if (s == null) return -1;
        synchronized (s) {
            return Math.round(s.rttVarMs);
        }
    }

    /** Адаптивный таймаут ожидания подтверждения для собеседника. */
    public long rto(String peer) {
        long srtt = srtt(peer);
        if (srtt < 0) return P2pConfig.ADAPTIVE_RTO_MIN_MS * 2L;

        long rto = Math.max(srtt * (long) P2pConfig.ADAPTIVE_RTO_FACTOR, srtt + 4 * Math.max(1, rttVar(peer)));
        if (rto < P2pConfig.ADAPTIVE_RTO_MIN_MS) rto = P2pConfig.ADAPTIVE_RTO_MIN_MS;
        if (rto > P2pConfig.ADAPTIVE_RTO_MAX_MS) rto = P2pConfig.ADAPTIVE_RTO_MAX_MS;
        return rto;
    }

    /** Задержка перед повтором попытки № {@code attempt} (0 — первая). */
    public static long retryDelay(int attempt) {
        long[] ladder = P2pConfig.RETRY_LADDER_MS;
        if (ladder.length == 0) return 1_000;
        int idx = Math.max(0, Math.min(attempt, ladder.length - 1));
        return ladder[idx];
    }

    /** Доля потерь (0..1) по данным сокетного канала. */
    public double lossRate(String peer) {
        Stats s = stats.get(peer);
        if (s == null) return 0;
        synchronized (s) {
            if (s.sent == 0) return 0;
            return (double) s.lost / (double) s.sent;
        }
    }

    public void forget(String peer) {
        if (peer != null) stats.remove(peer);
    }

    /** Краткая строка для диагностики: {@code srtt/rto/loss}. */
    public String describe(String peer) {
        long srtt = srtt(peer);
        if (srtt < 0) return "rtt=?";
        return "rtt=" + srtt + "ms/rto=" + rto(peer) + "ms/loss="
                + String.format(java.util.Locale.US, "%.2f", lossRate(peer));
    }
}
