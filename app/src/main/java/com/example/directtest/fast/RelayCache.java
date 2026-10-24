package com.example.directtest.fast;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Защита от лавины повторов при ретрансляции (C6).
 *
 * <p>Идея: дальность без Bluetooth можно расширить, если узел в центре
 * пересылает сообщение дальше — «один хоп». Но без фильтра пересланный кадр
 * вернётся по кругу и размножится. Здесь запоминаются уже обработанные
 * {@code msgId} с временем жизни: кадр обрабатывается один раз, повторно —
 * только если он реально новый.
 */
public final class RelayCache {

    private static final long TTL_MS = 120_000;
    private static final int MAX_ENTRIES = 512;

    private final Map<String, Long> seen = new ConcurrentHashMap<>();

    /** Отметить кадр как увиденный. */
    public void markSeen(String key) {
        if (key == null || key.isEmpty()) return;
        pruneIfNeeded();
        seen.put(key, System.currentTimeMillis());
    }

    /** Был ли кадр уже обработан. */
    public boolean isDuplicate(String key) {
        if (key == null || key.isEmpty()) return false;
        Long at = seen.get(key);
        if (at == null) return false;
        if (System.currentTimeMillis() - at > TTL_MS) {
            seen.remove(key);
            return false;
        }
        return true;
    }

    /**
     * Атомарно: «если новый — запомнить и сказать true».
     * Именно этот вызов должен стоять перед обработкой ретранслированного кадра.
     */
    public boolean markIfNew(String key) {
        if (key == null || key.isEmpty()) return false;
        long now = System.currentTimeMillis();
        pruneIfNeeded();
        Long previous = seen.put(key, now);
        return previous == null || now - previous > TTL_MS;
    }

    public int size() {
        return seen.size();
    }

    public void clear() {
        seen.clear();
    }

    private void pruneIfNeeded() {
        if (seen.size() < MAX_ENTRIES) return;
        long now = System.currentTimeMillis();
        seen.entrySet().removeIf(e -> now - e.getValue() > TTL_MS);
        if (seen.size() >= MAX_ENTRIES) {
            seen.clear();   // крайний случай: лучше потерять фильтр, чем расти
        }
    }
}
