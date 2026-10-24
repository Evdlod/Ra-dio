package com.example.directtest.fast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Бинарный кадр быстрого канала (B4).
 *
 * <p>Раньше по сокету шли текстовые строки {@code WFD1|MSG|id|msgId|base64}:
 * каждый байт полезной нагрузки раздувался base64 на 33 %, а разбор
 * сплитом по {@code '|'} ломался от любого неожиданного символа. Кадр решает
 * обе проблемы:
 *
 * <pre>
 *  0  1  2      3      4      5  6        7..n        n+1..n+4
 * +--+--+--+------+------+------+--------+----------+---------+
 * |'R'|'D'|ver| type |flags | len(2, big-endian) | payload | crc32 |
 * +--+--+--+------+------+------+--------+----------+---------+
 * </pre>
 *
 * <ul>
 *   <li>длина — в байтах, а не в символах: кириллица больше не «съедает»
 *       лимит и не обрезается посередине символа;</li>
 *   <li>CRC32 по всему кадру: битое соединение или обрезанный пакет на
 *       уровне TCP-стека вендорской прошивки отсекается сразу, а не превращается
 *       в «сообщение с мусором»;</li>
 *   <li>флаги — место для ACK-пиггибэка ({@link #FLAG_ACK_INLINE}), ответа на
 *       probe и ретрансляции (C6).</li>
 * </ul>
 */
public final class BinaryFrame {

    public static final byte MAGIC_0 = 'R';
    public static final byte MAGIC_1 = 'D';
    public static final byte VERSION = 2;

    public static final int HEADER_LEN = 7;
    public static final int FOOTER_LEN = 4;
    public static final int MAX_PAYLOAD = 60_000;

    // ==================== ТИПЫ КАДРОВ ====================
    public static final byte T_HELLO = 1;
    public static final byte T_MSG = 2;
    public static final byte T_ACK = 3;
    public static final byte T_HB = 4;
    public static final byte T_PING = 5;
    public static final byte T_PONG = 6;
    public static final byte T_PRESENCE = 7;
    public static final byte T_SYNC = 8;

    // ==================== ФЛАГИ ====================
    /** В кадре есть поле ack (msgId, подтверждаемый этим же кадром). */
    public static final byte FLAG_ACK_INLINE = 0x01;
    /** Кадр можно ретранслировать (C6). */
    public static final byte FLAG_RELAYABLE = 0x02;
    /** Ответ на probe — не требует подтверждения. */
    public static final byte FLAG_NO_ACK = 0x04;

    public final byte type;
    public final byte flags;
    public final byte[] payload;

    public BinaryFrame(byte type, byte flags, byte[] payload) {
        this.type = type;
        this.flags = flags;
        this.payload = payload != null ? payload : new byte[0];
    }

    public boolean hasFlag(byte flag) {
        return (flags & flag) != 0;
    }

    /** Собрать кадр в массив байт (заголовок + payload + CRC32). */
    public byte[] encode() {
        int len = payload.length;
        byte[] out = new byte[HEADER_LEN + len + FOOTER_LEN];

        out[0] = MAGIC_0;
        out[1] = MAGIC_1;
        out[2] = VERSION;
        out[3] = type;
        out[4] = flags;
        out[5] = (byte) ((len >> 8) & 0xFF);
        out[6] = (byte) (len & 0xFF);
        System.arraycopy(payload, 0, out, HEADER_LEN, len);

        CRC32 crc = new CRC32();
        crc.update(out, 0, HEADER_LEN + len);
        long value = crc.getValue();
        int pos = HEADER_LEN + len;
        out[pos] = (byte) ((value >> 24) & 0xFF);
        out[pos + 1] = (byte) ((value >> 16) & 0xFF);
        out[pos + 2] = (byte) ((value >> 8) & 0xFF);
        out[pos + 3] = (byte) (value & 0xFF);

        return out;
    }

    /**
     * Разобрать кадр из буфера.
     *
     * @return кадр или {@code null}, если данных мало или кадр повреждён
     */
    public static BinaryFrame decode(byte[] buf, int off, int avail) {
        if (buf == null || avail < HEADER_LEN + FOOTER_LEN) return null;
        if (buf[off] != MAGIC_0 || buf[off + 1] != MAGIC_1) return null;
        if (buf[off + 2] != VERSION) return null;

        int len = ((buf[off + 5] & 0xFF) << 8) | (buf[off + 6] & 0xFF);
        if (len < 0 || len > MAX_PAYLOAD) return null;
        if (avail < HEADER_LEN + len + FOOTER_LEN) return null;

        CRC32 crc = new CRC32();
        crc.update(buf, off, HEADER_LEN + len);
        long expected = ((long) (buf[off + HEADER_LEN + len] & 0xFF) << 24)
                | ((long) (buf[off + HEADER_LEN + len + 1] & 0xFF) << 16)
                | ((long) (buf[off + HEADER_LEN + len + 2] & 0xFF) << 8)
                | (buf[off + HEADER_LEN + len + 3] & 0xFF);
        if (crc.getValue() != expected) return null;

        byte[] payload = new byte[len];
        System.arraycopy(buf, off + HEADER_LEN, payload, 0, len);
        return new BinaryFrame(buf[off + 3], buf[off + 4], payload);
    }

    /** Ожидаемая полная длина кадра по заголовку (или -1, если заголовок битый). */
    public static int frameLength(byte[] header, int off) {
        if (header == null || off + HEADER_LEN > header.length) return -1;
        if (header[off] != MAGIC_0 || header[off + 1] != MAGIC_1) return -1;
        int len = ((header[off + 5] & 0xFF) << 8) | (header[off + 6] & 0xFF);
        if (len > MAX_PAYLOAD) return -1;
        return HEADER_LEN + len + FOOTER_LEN;
    }

    /** Записать кадр в поток (TCP). */
    public static void write(OutputStream out, BinaryFrame frame) throws IOException {
        out.write(frame.encode());
        out.flush();
    }

    /** Прочитать кадр из потока (TCP). Возвращает null при чистом EOF. */
    public static BinaryFrame read(InputStream in) throws IOException {
        byte[] header = new byte[HEADER_LEN];
        int read = readFully(in, header, 0, HEADER_LEN);
        if (read == 0) return null;               // штатное закрытие соединения
        if (read < HEADER_LEN) throw new EOFException("short header: " + read);

        int total = frameLength(header, 0);
        if (total < 0) {
            throw new IOException("bad frame header");
        }

        int rest = total - HEADER_LEN;
        byte[] body = new byte[rest];
        int got = readFully(in, body, 0, rest);
        if (got < rest) throw new EOFException("short body: " + got + "/" + rest);

        byte[] full = new byte[total];
        System.arraycopy(header, 0, full, 0, HEADER_LEN);
        System.arraycopy(body, 0, full, HEADER_LEN, rest);

        BinaryFrame frame = decode(full, 0, total);
        if (frame == null) throw new IOException("corrupted frame (crc)");
        return frame;
    }

    private static int readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int n = in.read(buf, off + total, len - total);
            if (n < 0) break;
            total += n;
        }
        return total;
    }

    // ==================== ПОЛЕЗНАЯ НАГРУЗКА ====================

    /**
     * Построитель payload: удобно писать строки с длиной и целые числа,
     * не изобретая разделители.
     */
    public static final class Writer {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(128);
        private final DataOutputStream out = new DataOutputStream(buffer);

        public Writer putString(String value) {
            try {
                byte[] bytes = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
                out.writeShort(bytes.length);
                out.write(bytes);
            } catch (IOException ignored) {
                // ByteArrayOutputStream не бросает
            }
            return this;
        }

        public Writer putLong(long value) {
            try {
                out.writeLong(value);
            } catch (IOException ignored) {
            }
            return this;
        }

        public Writer putInt(int value) {
            try {
                out.writeInt(value);
            } catch (IOException ignored) {
            }
            return this;
        }

        public Writer putByte(byte value) {
            try {
                out.writeByte(value);
            } catch (IOException ignored) {
            }
            return this;
        }

        public Writer putRaw(byte[] value) {
            try {
                out.writeShort(value == null ? 0 : value.length);
                if (value != null) out.write(value);
            } catch (IOException ignored) {
            }
            return this;
        }

        public byte[] toByteArray() {
            try {
                out.flush();
            } catch (IOException ignored) {
            }
            return buffer.toByteArray();
        }

        public BinaryFrame frame(byte type, byte flags) {
            return new BinaryFrame(type, flags, toByteArray());
        }
    }

    /** Читатель payload, симметричный {@link Writer}. */
    public static final class Reader {
        private final DataInputStream in;

        public Reader(byte[] payload) {
            this.in = new DataInputStream(
                    new ByteArrayInputStream(payload != null ? payload : new byte[0]));
        }

        public String getString() {
            try {
                int len = in.readUnsignedShort();
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            } catch (IOException e) {
                return "";
            }
        }

        public long getLong() {
            try {
                return in.readLong();
            } catch (IOException e) {
                return 0;
            }
        }

        public int getInt() {
            try {
                return in.readInt();
            } catch (IOException e) {
                return 0;
            }
        }

        public byte getByte() {
            try {
                return in.readByte();
            } catch (IOException e) {
                return 0;
            }
        }

        public byte[] getRaw() {
            try {
                int len = in.readUnsignedShort();
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                return bytes;
            } catch (IOException e) {
                return new byte[0];
            }
        }
    }
}
