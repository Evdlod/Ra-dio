package com.example.directtest.fast;

import com.example.directtest.P2pConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Оценка расхождения часов с каждым собеседником.
 *
 * <p>Нужна для двух вещей:
 * <ul>
 *   <li>окна синхронизированного поиска (A3) — обе стороны должны открывать
 *       поиск в одно и то же физическое время, а не «по своим часам»;</li>
 *   <li>корректное сопоставление логов двух телефонов: в журнале рядом с
 *       событием пишется {@code skew_ms}, и по нему можно пересчитать время
 *       собеседника в своё.</li>
 * </ul>
 *
 * <p>Смещение = {@code peerUtc - localUtc}. Оно сглаживается EMA, потому что
 * значение из одного пакета содержит сетевую задержку и «дрожит» на десятки
 * миллисекунд. Слишком большие значения (явно битые часы) не принимаются.
 */
public final class PeerClock {

    private static final double ALPHA = 0.25;

    private static final class Sample {
        double offsetMs;
        long updatedAtUtc;
        long samples;
    }

    private final Map<String, Sample> samples = new ConcurrentHashMap<>();

    /**
     * Учесть метку времени собеседника.
     *
     * @param peer        короткий ID собеседника
     * @param peerUtcMs   время, которое собеседник вписал в запись
     * @param localRecvMs локальное время приёма
     * @param rttMs       измеренная задержка до собеседника (компенсация
     *                    «полпути»); 0, если неизвестна
     */
    public void observe(String peer, long peerUtcMs, long localRecvMs, long rttMs) {
        if (peer == null || peerUtcMs <= 0 || localRecvMs <= 0) return;

        long halfRtt = Math.max(0, rttMs / 2);
        double offset = (peerUtcMs + halfRtt) - localRecvMs;

        if (Math.abs(offset) > P2pConfig.CLOCK_SKEW_LIMIT_MS) {
            // Часы собеседника ушли слишком далеко — не портим фильтр возраста.
            return;
        }

        Sample s = samples.get(peer);
        if (s == null) {
            s = new Sample();
            s.offsetMs = offset;
            s.updatedAtUtc = localRecvMs;
            s.samples = 1;
            samples.put(peer, s);
            return;
        }

        synchronized (s) {
            if (s.samples == 0) {
                s.offsetMs = offset;
            } else {
                s.offsetMs = s.offsetMs * (1 - ALPHA) + offset * ALPHA;
            }
            s.samples++;
            s.updatedAtUtc = localRecvMs;
        }
    }

    /** Смещение часов собеседника относительно нас (мс). 0, если неизвестно. */
    public long offset(String peer) {
        Sample s = peer != null ? samples.get(peer) : null;
        if (s == null) return 0;
        synchronized (s) {
            return Math.round(s.offsetMs);
        }
    }

    public boolean isKnown(String peer) {
        return peer != null && samples.containsKey(peer);
    }

    /** Пересчитать время собеседника в локальное. */
    public long toLocal(String peer, long peerUtcMs) {
        return peerUtcMs - offset(peer);
    }

    /** Пересчитать локальное время во время собеседника. */
    public long toPeer(String peer, long localUtcMs) {
        return localUtcMs + offset(peer);
    }

    /**
     * Локальное время, соответствующее границе окна в шкале собеседника.
     * Нужно, чтобы обе стороны открывали окно одновременно, даже если на
     * одном из телефонов часы убежали на секунду.
     */
    public long localBoundaryFor(String peer, long peerBoundaryUtc) {
        return toLocal(peer, peerBoundaryUtc);
    }

    public void forget(String peer) {
        if (peer != null) samples.remove(peer);
    }

    public void clear() {
        samples.clear();
    }
}
