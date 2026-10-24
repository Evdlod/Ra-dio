package com.example.directtest.fast;

/**
 * Кодирование/разбор полезной нагрузки кадров быстрого канала.
 *
 * <p>Один и тот же формат используется двумя транспортами:
 * <ul>
 *   <li>TCP поверх P2P-группы ({@code P2pSocketTransport}) — надёжный,
 *       с подтверждениями и ретрансмиссией;</li>
 *   <li>UDP внутри подсети группы ({@link GroupLinkChannel}) — самый быстрый,
 *       доставка «в один пакет», работает и без установления TCP-сессии.</li>
 * </ul>
 *
 * <p>Первое поле каждого кадра — идентификатор отправителя: по нему приёмник
 * узнаёт, кто говорит, не дожидаясь рукопожатия. Второе (там, где нужно) —
 * {@code msgId}: он же {@code trace} в журнале корреляции.
 */
public final class Protocol {

    private Protocol() {
    }

    // ==================== СТРУКТУРЫ ====================

    public static final class Hello {
        public String deviceId;
        public String sessionId;
        public String ack;          // подтверждение «в довесок» (B5)
        public long utcMs;
    }

    public static final class Msg {
        public String deviceId;
        public String sessionId;
        public String msgId;
        public String text;
        public String ack;          // подтверждение «в довесок» (B5)
        public long utcMs;
        public int ttl;             // 0 — доставлять только напрямую (C6)
    }

    public static final class Ack {
        public String deviceId;
        public String msgId;
        public long utcMs;
    }

    public static final class Ping {
        public String deviceId;
        public String probeId;
        public long utcMs;
        public long echoUtcMs;      // заполняется только в pong
    }

    public static final class Presence {
        public String deviceId;
        public String sessionId;
        public long utcMs;
    }

    // ==================== СБОРКА ====================

    public static BinaryFrame hello(String deviceId, String sessionId, long utcMs, String ack) {
        byte flags = ack != null && !ack.isEmpty() ? BinaryFrame.FLAG_ACK_INLINE : 0;
        return new BinaryFrame.Writer()
                .putString(deviceId)
                .putString(sessionId)
                .putLong(utcMs)
                .putString(ack)
                .frame(BinaryFrame.T_HELLO, flags);
    }

    public static BinaryFrame heartbeat(String deviceId, long utcMs, String ack) {
        byte flags = ack != null && !ack.isEmpty() ? BinaryFrame.FLAG_ACK_INLINE : 0;
        return new BinaryFrame.Writer()
                .putString(deviceId)
                .putLong(utcMs)
                .putString(ack)
                .frame(BinaryFrame.T_HB, flags);
    }

    public static BinaryFrame message(String deviceId, String sessionId, String msgId,
                                      String text, String ack, long utcMs, int ttl) {
        byte flags = BinaryFrame.FLAG_RELAYABLE;
        if (ack != null && !ack.isEmpty()) flags |= BinaryFrame.FLAG_ACK_INLINE;
        return new BinaryFrame.Writer()
                .putString(deviceId)
                .putString(sessionId)
                .putString(msgId)
                .putString(text)
                .putString(ack)
                .putLong(utcMs)
                .putByte((byte) ttl)
                .frame(BinaryFrame.T_MSG, flags);
    }

    public static BinaryFrame ack(String deviceId, String msgId, long utcMs) {
        return new BinaryFrame.Writer()
                .putString(deviceId)
                .putString(msgId)
                .putLong(utcMs)
                .frame(BinaryFrame.T_ACK, (byte) 0);
    }

    public static BinaryFrame ping(String deviceId, String probeId, long utcMs, boolean pong, long echoUtcMs) {
        return new BinaryFrame.Writer()
                .putString(deviceId)
                .putString(probeId)
                .putLong(utcMs)
                .putLong(echoUtcMs)
                .frame(pong ? BinaryFrame.T_PONG : BinaryFrame.T_PING, BinaryFrame.FLAG_NO_ACK);
    }

    public static BinaryFrame presence(String deviceId, String sessionId, long utcMs) {
        return new BinaryFrame.Writer()
                .putString(deviceId)
                .putString(sessionId)
                .putLong(utcMs)
                .frame(BinaryFrame.T_PRESENCE, BinaryFrame.FLAG_NO_ACK);
    }

    // ==================== РАЗБОР ====================

    public static Hello parseHello(BinaryFrame frame) {
        BinaryFrame.Reader r = new BinaryFrame.Reader(frame.payload);
        Hello hello = new Hello();
        hello.deviceId = r.getString();
        hello.sessionId = r.getString();
        hello.utcMs = r.getLong();
        hello.ack = r.getString();
        return hello;
    }

    public static String parseHeartbeatDeviceId(BinaryFrame frame) {
        return new BinaryFrame.Reader(frame.payload).getString();
    }

    public static Msg parseMessage(BinaryFrame frame) {
        BinaryFrame.Reader r = new BinaryFrame.Reader(frame.payload);
        Msg msg = new Msg();
        msg.deviceId = r.getString();
        msg.sessionId = r.getString();
        msg.msgId = r.getString();
        msg.text = r.getString();
        msg.ack = r.getString();
        msg.utcMs = r.getLong();
        msg.ttl = r.getByte();
        return msg;
    }

    public static Ack parseAck(BinaryFrame frame) {
        BinaryFrame.Reader r = new BinaryFrame.Reader(frame.payload);
        Ack ack = new Ack();
        ack.deviceId = r.getString();
        ack.msgId = r.getString();
        ack.utcMs = r.getLong();
        return ack;
    }

    public static Ping parsePing(BinaryFrame frame) {
        BinaryFrame.Reader r = new BinaryFrame.Reader(frame.payload);
        Ping ping = new Ping();
        ping.deviceId = r.getString();
        ping.probeId = r.getString();
        ping.utcMs = r.getLong();
        ping.echoUtcMs = r.getLong();
        return ping;
    }

    public static Presence parsePresence(BinaryFrame frame) {
        BinaryFrame.Reader r = new BinaryFrame.Reader(frame.payload);
        Presence presence = new Presence();
        presence.deviceId = r.getString();
        presence.sessionId = r.getString();
        presence.utcMs = r.getLong();
        return presence;
    }

    /** Разбор поля ack: список ID через запятую → без пустых элементов. */
    public static java.util.List<String> splitAcks(String ack) {
        java.util.List<String> result = new java.util.ArrayList<>();
        if (ack == null || ack.isEmpty()) return result;
        for (String part : ack.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) result.add(trimmed);
        }
        return result;
    }
}
