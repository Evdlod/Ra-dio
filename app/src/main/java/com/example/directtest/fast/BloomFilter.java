package com.example.directtest.fast;

import com.example.directtest.P2pConfig;

import java.util.Locale;

/**
 * Компактный Bloom-фильтр для сверки историй сообщений (C2).
 *
 * <p>Проблема: чтобы понять, «все ли сообщения дошли», сейчас в SYNC
 * перечисляются все ID ({@code mid1,mid2,...}), и запись упирается в лимит
 * TXT (240 байт). С ростом истории сверка либо не влезает, либо начинает
 * врать.
 *
 * <p>Bloom-фильтр на 128 бит укладывается в 32 hex-символа и позволяет
 * собеседнику одной проверкой понять, есть ли у него конкретный {@code msgId}:
 * <ul>
 *   <li>«нет» — точный ответ, ID точно отсутствует → переотправляем;</li>
 *   <li>«да» — с малой вероятностью ложное срабатывание (при 128 битах и
 *       десятках сообщений — проценты).</li>
 * </ul>
 * Ложное «да» означает лишь «сверка не запустилась», а не потерю сообщения:
 * настоящая доставка подтверждается ACK.
 */
public final class BloomFilter {

    /** Два хеша — компромисс между точностью и стоимостью. */
    private static final int HASHES = 2;

    private final long[] bits;

    public BloomFilter(int bitCount) {
        int words = Math.max(1, (bitCount + 63) / 64);
        this.bits = new long[words];
    }

    public static BloomFilter forMessageIds() {
        return new BloomFilter(P2pConfig.BLOOM_BITS);
    }

    public void add(String value) {
        if (value == null || value.isEmpty()) return;
        int size = bits.length * 64;
        int h1 = hash1(value) & 0x7FFFFFFF;
        int h2 = hash2(value) & 0x7FFFFFFF;
        for (int i = 0; i < HASHES; i++) {
            int idx = (int) (((long) h1 + (long) i * h2) % size);
            bits[idx >>> 6] |= (1L << (idx & 63));
        }
    }

    public boolean mightContain(String value) {
        if (value == null || value.isEmpty()) return false;
        int size = bits.length * 64;
        int h1 = hash1(value) & 0x7FFFFFFF;
        int h2 = hash2(value) & 0x7FFFFFFF;
        for (int i = 0; i < HASHES; i++) {
            int idx = (int) (((long) h1 + (long) i * h2) % size);
            if ((bits[idx >>> 6] & (1L << (idx & 63))) == 0) {
                return false;
            }
        }
        return true;
    }

    /** Hex-представление (32 символа для 128 бит) — влезает в TXT-запись. */
    public String toHex() {
        StringBuilder sb = new StringBuilder(bits.length * 16);
        for (long word : bits) {
            sb.append(String.format(Locale.US, "%016x", word));
        }
        return sb.toString();
    }

    public static BloomFilter fromHex(String hex, int bitCount) {
        BloomFilter filter = new BloomFilter(bitCount);
        if (hex == null) return filter;
        String clean = hex.trim();
        for (int i = 0; i < filter.bits.length && (i + 1) * 16 <= clean.length(); i++) {
            try {
                filter.bits[i] = Long.parseUnsignedLong(clean.substring(i * 16, (i + 1) * 16), 16);
            } catch (NumberFormatException e) {
                return filter;
            }
        }
        return filter;
    }

    /** Сколько битов установлено — грубая оценка заполненности (для диагностики). */
    public int setBitCount() {
        int count = 0;
        for (long word : bits) {
            count += Long.bitCount(word);
        }
        return count;
    }

    private static int hash1(String value) {
        int h = 0x811C9DC5;                       // FNV-1a, 32 бита
        for (int i = 0; i < value.length(); i++) {
            h ^= value.charAt(i);
            h *= 0x01000193;
        }
        return h;
    }

    private static int hash2(String value) {
        int h = 0x7FFFFFFF;
        for (int i = 0; i < value.length(); i++) {
            h = 31 * h + value.charAt(i);
        }
        // Испортим младшие биты, чтобы индексы не совпадали с hash1
        return (h ^ (h >>> 15)) * 0x2545F491;
    }
}
