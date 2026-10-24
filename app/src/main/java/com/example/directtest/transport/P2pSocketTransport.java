package com.example.directtest.transport;

import com.example.directtest.DiagnosticLogger;
import com.example.directtest.P2pConfig;
import com.example.directtest.fast.BinaryFrame;
import com.example.directtest.fast.CorrelationLog;
import com.example.directtest.fast.LinkQuality;
import com.example.directtest.fast.Protocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Надёжный быстрый транспорт поверх P2P-группы Wi-Fi Direct.
 *
 * <h3>Что изменилось по сравнению с версией 4</h3>
 * <ul>
 *   <li><b>Бинарные кадры (B4)</b> вместо строк {@code WFD1|MSG|...|base64}:
 *       без 33 %-ного раздувания, с CRC32 и длиной в байтах — кириллица
 *       больше не упирается в лимиты, а битый поток не превращается в мусор;</li>
 *   <li><b>Немедленный и «попутный» ACK (B5)</b>: подтверждение уходит сразу
 *       отдельным кадром, а если в этот момент есть что отправить — ещё и
 *       приклеивается к следующему кадру (поле {@code ack});</li>
 *   <li><b>Лестница повторов 1→2→4→8 с (B6)</b> вместо ожидания 15 секунд:
 *       потерянный пакет восстанавливается за секунду;</li>
 *   <li><b>Keep-alive 5 с (B7)</b>: power-save вендорских прошивок рвёт
 *       «молчащее» TCP-соединение; частый heartbeat держит его живым;</li>
 *   <li><b>Адаптивные таймауты (B8)</b>: RTO считается от измеренного RTT
 *       ({@link LinkQuality}), а не берётся из константы 60 с.</li>
 * </ul>
 *
 * <p>Транспорт остаётся <i>дополнительным</i>: обнаружение и резервная
 * доставка работают через DNS-SD, дубликаты отсекаются по {@code msgId}.
 */
public class P2pSocketTransport {

    private static final String TAG = "[Sock]";

    /** Колбэки в сторону {@code FastDiscoveryManager}. */
    public interface Callback {
        void onPeerConnected(String deviceId);

        void onPeerDisconnected(String deviceId);

        void onMessageReceived(String senderDeviceId, String msgId, String text);

        void onAckReceived(String senderDeviceId, String msgId);

        /** Кадр доставлен и подтверждён (для расчёта сквозной задержки). */
        default void onDelivered(String peerDeviceId, String msgId, long rttMs) {
        }

        /** Все попытки исчерпаны — нужен резервный путь (TXT/SYNC). */
        default void onDeliveryFailed(String peerDeviceId, String msgId) {
        }

        /** Измеренный RTT до собеседника. */
        default void onRttSample(String peerDeviceId, long rttMs) {
        }

        /** На связи появился/пропал пир, известный только по быстрому каналу. */
        default void onPeerSeen(String deviceId, String address) {
        }
    }

    // ==================== СОСТОЯНИЕ ====================

    private final String deviceId;
    private final String sessionId;
    private final DiagnosticLogger log = DiagnosticLogger.getInstance();
    private final CorrelationLog corr = CorrelationLog.get();
    private final LinkQuality quality = new LinkQuality();

    private final Map<String, PeerConnection> peers = new ConcurrentHashMap<>();
    private final Map<String, PendingDelivery> awaitingAck = new ConcurrentHashMap<>();
    private final Set<String> piggybackAcks = new LinkedHashSet<>();

    private final AtomicLong pingSeq = new AtomicLong();
    private final Map<String, Long> pendingPings = new ConcurrentHashMap<>();

    private volatile boolean running = false;
    private volatile ServerSocket serverSocket;
    private volatile ScheduledExecutorService scheduler;
    private volatile Callback callback;

    private Thread acceptThread;

    public P2pSocketTransport(String deviceId, String sessionId) {
        this.deviceId = CorrelationLog.shortId(deviceId);
        this.sessionId = sessionId != null ? sessionId : "-";
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    public LinkQuality getLinkQuality() {
        return quality;
    }

    // ==================== LIFECYCLE ====================

    public synchronized void start() {
        if (running) return;
        running = true;

        scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "wfd-sock-sched");
            t.setDaemon(true);
            return t;
        });

        acceptThread = new Thread(this::acceptLoop, "wfd-sock-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        scheduler.scheduleWithFixedDelay(this::keepAliveTick,
                P2pConfig.SOCKET_KEEPALIVE_MS, P2pConfig.SOCKET_KEEPALIVE_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::probeTick,
                P2pConfig.RTT_PROBE_INTERVAL_MS, P2pConfig.RTT_PROBE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        log.i(TAG + " transport started, port=" + P2pConfig.SOCKET_PORT
                + ", protocol=" + P2pConfig.PROTOCOL_VERSION);
        corr.event(CorrelationLog.EV_START, null, null,
                "component", "socket-transport", "port", String.valueOf(P2pConfig.SOCKET_PORT));
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;

        ScheduledExecutorService s = scheduler;
        scheduler = null;
        if (s != null) s.shutdownNow();

        closeQuietly(serverSocket);
        serverSocket = null;

        for (PeerConnection pc : peers.values()) {
            pc.close();
        }
        peers.clear();
        awaitingAck.clear();
        piggybackAcks.clear();
        pendingPings.clear();

        log.i(TAG + " transport stopped");
        corr.event(CorrelationLog.EV_STOP, null, null, "component", "socket-transport");
    }

    public boolean isRunning() {
        return running;
    }

    public int getConnectedCount() {
        return peers.size();
    }

    public boolean isConnected(String targetDeviceId) {
        return targetDeviceId != null
                && peers.containsKey(CorrelationLog.shortId(targetDeviceId));
    }

    // ==================== ГРУППА ====================

    public void onGroupInfo(boolean formed, boolean groupOwner, InetAddress ownerAddress) {
        if (!running) return;

        if (!formed) {
            log.d(TAG + " group not formed, dropping peer sockets");
            for (PeerConnection pc : peers.values()) {
                pc.close();
            }
            peers.clear();
            awaitingAck.clear();
            return;
        }

        if (groupOwner) {
            log.success(TAG + " we are group owner, listening on " + P2pConfig.SOCKET_PORT);
            return;
        }

        if (ownerAddress != null) {
            connectToOwner(ownerAddress);
        } else {
            log.w(TAG + " group formed but owner address is unknown");
        }
    }

    public void connectToOwner(InetAddress ownerAddress) {
        ScheduledExecutorService s = scheduler;
        if (!running || ownerAddress == null || s == null) return;

        if (!peers.isEmpty()) {
            log.d(TAG + " already connected to " + peers.keySet());
            return;
        }

        s.execute(() -> {
            for (int attempt = 0; attempt < P2pConfig.SOCKET_CONNECT_ATTEMPTS && running; attempt++) {
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(ownerAddress, P2pConfig.SOCKET_PORT),
                            P2pConfig.SOCKET_CONNECT_TIMEOUT_MS);
                    socket.setTcpNoDelay(true);

                    log.success(TAG + " connected to owner " + ownerAddress.getHostAddress());
                    corr.event(CorrelationLog.EV_LINK_UP, null, null,
                            "channel", "tcp", "addr", ownerAddress.getHostAddress(),
                            "attempt", String.valueOf(attempt));
                    setupPeer(socket);
                    return;
                } catch (IOException e) {
                    log.w(TAG + " connect attempt " + attempt + " failed: " + e.getMessage());
                    sleepQuietly(LinkQuality.retryDelay(attempt));
                }
            }
            log.error(TAG + " could not connect to owner " + ownerAddress.getHostAddress()
                    + " — remaining on DNS-SD transport");
            corr.event(CorrelationLog.EV_LINK_DOWN, null, null,
                    "channel", "tcp", "reason", "connect-failed");
        });
    }

    // ==================== ОТПРАВКА ====================

    /**
     * Отправить сообщение. Метод возвращает true, если кадр поставлен в
     * очередь хотя бы одного получателя: подтверждение и повторы берёт на
     * себя транспорт.
     */
    public boolean sendMessage(String targetDeviceId, String msgId, String text) {
        return sendMessage(targetDeviceId, msgId, text, P2pConfig.PRIORITY_NORMAL);
    }

    public boolean sendMessage(String targetDeviceId, String msgId, String text, int priority) {
        if (!running || msgId == null) return false;

        List<String> targets = new ArrayList<>();
        if (targetDeviceId == null || targetDeviceId.isEmpty()) {
            targets.addAll(peers.keySet());
        } else {
            String key = CorrelationLog.shortId(targetDeviceId);
            if (peers.containsKey(key)) targets.add(key);
        }
        if (targets.isEmpty()) return false;

        long now = System.currentTimeMillis();
        boolean sentAny = false;

        for (String peerId : targets) {
            BinaryFrame frame = Protocol.message(deviceId, sessionId, msgId, text,
                    takePiggybackAcks(), now, P2pConfig.RELAY_TTL);

            PeerConnection pc = peers.get(peerId);
            if (pc == null) continue;

            quality.onSent(peerId);
            pc.send(frame);

            PendingDelivery delivery = new PendingDelivery(peerId, frame, msgId, now, priority);
            awaitingAck.put(msgId + "@" + peerId, delivery);
            scheduleRetry(delivery, 0);

            corr.event(CorrelationLog.EV_MSG_TX, msgId, peerId,
                    "channel", "tcp", "attempt", "0", "bytes", String.valueOf(frame.payload.length));
            sentAny = true;
        }
        return sentAny;
    }

    /** Отправить подтверждение (немедленно, без ожидания). */
    public boolean sendAck(String targetDeviceId, String msgId) {
        if (msgId == null) return false;
        return sendFrame(targetDeviceId, Protocol.ack(deviceId, msgId, System.currentTimeMillis()));
    }

    /** Отправить произвольный кадр конкретному пиру или всем. */
    public boolean sendFrame(String targetDeviceId, BinaryFrame frame) {
        if (!running || frame == null) return false;

        if (targetDeviceId == null || targetDeviceId.isEmpty()) {
            boolean sent = false;
            for (PeerConnection pc : peers.values()) {
                pc.send(frame);
                sent = true;
            }
            return sent;
        }

        PeerConnection pc = peers.get(CorrelationLog.shortId(targetDeviceId));
        if (pc == null) return false;
        pc.send(frame);
        return true;
    }

    // ==================== ПРИЁМ ====================

    private void acceptLoop() {
        while (running) {
            ServerSocket server = null;
            try {
                server = new ServerSocket();
                server.setReuseAddress(true);
                server.bind(new InetSocketAddress(P2pConfig.SOCKET_PORT));
                serverSocket = server;
                log.success(TAG + " listening on port " + P2pConfig.SOCKET_PORT);

                while (running) {
                    Socket socket = server.accept();
                    log.i(TAG + " incoming connection from " + socket.getInetAddress());
                    setupPeer(socket);
                }
            } catch (IOException e) {
                if (running) {
                    log.w(TAG + " accept loop error: " + e.getMessage() + " (retry in 2s)");
                    sleepQuietly(2000);
                }
            } finally {
                closeQuietly(server);
                if (serverSocket == server) serverSocket = null;
            }
        }
    }

    private void setupPeer(Socket socket) {
        ScheduledExecutorService s = scheduler;
        if (!running || s == null) {
            closeQuietly(socket);
            return;
        }

        try {
            PeerConnection pc = new PeerConnection(socket);
            s.execute(() -> readLoop(pc));
            // Рукопожатие: сразу сообщаем, кто мы, и «попутно» отдаём ACK
            pc.send(Protocol.hello(deviceId, sessionId, System.currentTimeMillis(),
                    takePiggybackAcks()));
        } catch (IOException e) {
            log.w(TAG + " failed to set up peer: " + e.getMessage());
            closeQuietly(socket);
        }
    }

    private void readLoop(PeerConnection pc) {
        try {
            BinaryFrame frame;
            while (running && (frame = pc.read()) != null) {
                handleFrame(pc, frame);
            }
        } catch (IOException e) {
            if (running) log.d(TAG + " read loop finished: " + e.getMessage());
        } finally {
            pc.close();
            String peerId = pc.peerId;
            if (peerId != null) {
                peers.remove(peerId, pc);
                log.i(TAG + " peer disconnected: " + peerId);
                corr.event(CorrelationLog.EV_LINK_DOWN, null, peerId, "channel", "tcp");
                Callback cb = callback;
                if (cb != null) cb.onPeerDisconnected(peerId);
            }
        }
    }

    private void handleFrame(PeerConnection pc, BinaryFrame frame) {
        switch (frame.type) {
            case BinaryFrame.T_HELLO: {
                Protocol.Hello hello = Protocol.parseHello(frame);
                if (hello.deviceId == null || hello.deviceId.equals(deviceId)) return;

                pc.peerId = CorrelationLog.shortId(hello.deviceId);
                peers.put(pc.peerId, pc);

                if (hello.utcMs > 0) {
                    // Оценка расхождения часов: полпути неизвестно, берём 0,
                    // уточняется после ping/pong.
                    // (PeerClock обновляет менеджер — он владеет общей картиной.)
                }

                log.success(TAG + " peer connected: " + pc.peerId);
                corr.event(CorrelationLog.EV_LINK_UP, null, pc.peerId, "channel", "tcp");
                Callback cb = callback;
                if (cb != null) {
                    cb.onPeerConnected(pc.peerId);
                    cb.onPeerSeen(pc.peerId, pc.address());
                }
                processPiggybackAcks(hello.ack, pc.peerId, cb);
                break;
            }

            case BinaryFrame.T_MSG: {
                Protocol.Msg msg = Protocol.parseMessage(frame);
                if (msg.deviceId == null || msg.deviceId.equals(deviceId)) return;

                Callback cb = callback;
                String sender = CorrelationLog.shortId(msg.deviceId);
                pc.peerId = sender;
                peers.put(sender, pc);

                // B5: подтверждаем немедленно, отдельным кадром
                pc.send(Protocol.ack(deviceId, msg.msgId, System.currentTimeMillis()));
                corr.event(CorrelationLog.EV_MSG_RX, msg.msgId, sender,
                        "channel", "tcp",
                        "delay_ms", String.valueOf(System.currentTimeMillis() - msg.utcMs),
                        "bytes", String.valueOf(msg.text != null ? msg.text.length() : 0));

                if (cb != null) cb.onMessageReceived(sender, msg.msgId, msg.text);
                processPiggybackAcks(msg.ack, sender, cb);
                break;
            }

            case BinaryFrame.T_ACK: {
                Protocol.Ack ack = Protocol.parseAck(frame);
                if (ack.deviceId == null || ack.deviceId.equals(deviceId)) return;

                String sender = CorrelationLog.shortId(ack.deviceId);
                PendingDelivery delivery = awaitingAck.remove(ack.msgId + "@" + sender);
                if (delivery != null) {
                    delivery.cancel();
                    long rtt = System.currentTimeMillis() - delivery.sentAt;
                    quality.onRttSample(sender, rtt);
                    corr.event(CorrelationLog.EV_ACK_RX, ack.msgId, sender,
                            "channel", "tcp", "rtt_ms", String.valueOf(rtt),
                            "attempt", String.valueOf(delivery.attempt));
                    Callback cb = callback;
                    if (cb != null) cb.onDelivered(sender, ack.msgId, rtt);
                } else {
                    corr.event(CorrelationLog.EV_ACK_RX, ack.msgId, sender,
                            "channel", "tcp", "state", "late-or-unknown");
                }

                Callback cb = callback;
                if (cb != null) cb.onAckReceived(sender, ack.msgId);
                break;
            }

            case BinaryFrame.T_HB: {
                String sender = Protocol.parseHeartbeatDeviceId(frame);
                if (sender == null || sender.equals(deviceId)) return;
                String key = CorrelationLog.shortId(sender);
                if (pc.peerId == null) {
                    pc.peerId = key;
                    peers.put(key, pc);
                    Callback cb = callback;
                    if (cb != null) cb.onPeerConnected(key);
                }
                Callback cb = callback;
                processPiggybackAcks(parseHeartbeatAck(frame), key, cb);
                break;
            }

            case BinaryFrame.T_PING: {
                Protocol.Ping ping = Protocol.parsePing(frame);
                if (ping.deviceId == null || ping.deviceId.equals(deviceId)) return;
                pc.send(Protocol.ping(deviceId, ping.probeId, System.currentTimeMillis(),
                        true, ping.utcMs));
                break;
            }

            case BinaryFrame.T_PONG: {
                Protocol.Ping pong = Protocol.parsePing(frame);
                Long sentAt = pendingPings.remove(pong.probeId);
                if (sentAt == null) return;
                long rtt = Math.max(0, System.currentTimeMillis() - sentAt);
                String peer = CorrelationLog.shortId(pong.deviceId);
                quality.onRttSample(peer, rtt);
                corr.event(CorrelationLog.EV_RTT, null, peer,
                        "channel", "tcp", "rtt_ms", String.valueOf(rtt),
                        "skew_ms", String.valueOf(pong.utcMs - pong.echoUtcMs - rtt / 2));
                Callback cb = callback;
                if (cb != null) cb.onRttSample(peer, rtt);
                break;
            }

            default:
                log.d(TAG + " unknown frame type: " + frame.type);
                break;
        }
    }

    private static String parseHeartbeatAck(BinaryFrame frame) {
        BinaryFrame.Reader r = new BinaryFrame.Reader(frame.payload);
        r.getString();   // deviceId
        r.getLong();     // utc
        return r.getString();
    }

    private void processPiggybackAcks(String acks, String peer, Callback cb) {
        for (String msgId : Protocol.splitAcks(acks)) {
            corr.event(CorrelationLog.EV_ACK_RX, msgId, peer, "channel", "tcp-piggyback");
            if (cb != null) cb.onAckReceived(peer, msgId);
        }
    }

    // ==================== ПОВТОРЫ И ACK ====================

    private final class PendingDelivery {
        final String peerId;
        final BinaryFrame frame;
        final String msgId;
        final long sentAt;
        final int priority;
        int attempt;
        volatile ScheduledFuture<?> future;

        PendingDelivery(String peerId, BinaryFrame frame, String msgId, long sentAt, int priority) {
            this.peerId = peerId;
            this.frame = frame;
            this.msgId = msgId;
            this.sentAt = sentAt;
            this.priority = priority;
        }

        void cancel() {
            ScheduledFuture<?> f = future;
            if (f != null) f.cancel(false);
        }
    }

    private void scheduleRetry(PendingDelivery delivery, int attempt) {
        ScheduledExecutorService s = scheduler;
        if (s == null || !running) return;

        long delay = LinkQuality.retryDelay(attempt);
        delivery.attempt = attempt;

        try {
            delivery.future = s.schedule(() -> {
                if (!running) return;

                String key = delivery.msgId + "@" + delivery.peerId;
                if (!awaitingAck.containsKey(key)) return;   // уже подтверждено

                if (attempt >= P2pConfig.RETRY_LADDER_MS.length) {
                    awaitingAck.remove(key);
                    quality.onLost(delivery.peerId);
                    log.w(TAG + " delivery failed after " + attempt + " retries: " + delivery.msgId);
                    corr.event(CorrelationLog.EV_RETRY, delivery.msgId, delivery.peerId,
                            "channel", "tcp", "attempt", String.valueOf(attempt), "result", "give-up");
                    Callback cb = callback;
                    if (cb != null) cb.onDeliveryFailed(delivery.peerId, delivery.msgId);
                    return;
                }

                PeerConnection pc = peers.get(delivery.peerId);
                if (pc == null) {
                    awaitingAck.remove(key);
                    Callback cb = callback;
                    if (cb != null) cb.onDeliveryFailed(delivery.peerId, delivery.msgId);
                    return;
                }

                pc.send(delivery.frame);
                corr.event(CorrelationLog.EV_RETRY, delivery.msgId, delivery.peerId,
                        "channel", "tcp", "attempt", String.valueOf(attempt + 1),
                        "delay_ms", String.valueOf(delay));
                scheduleRetry(delivery, attempt + 1);
            }, delay, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.w(TAG + " cannot schedule retry: " + e.getMessage());
        }
    }

    /** Взять до {@code MAX_ACKS_PER_RECORD} подтверждений для «попутной» отправки. */
    private String takePiggybackAcks() {
        synchronized (piggybackAcks) {
            if (piggybackAcks.isEmpty()) return "";
            StringBuilder sb = new StringBuilder();
            int count = 0;
            List<String> taken = new ArrayList<>();
            for (String msgId : piggybackAcks) {
                if (count++ >= P2pConfig.MAX_ACKS_PER_RECORD) break;
                if (sb.length() > 0) sb.append(',');
                sb.append(msgId);
                taken.add(msgId);
            }
            piggybackAcks.removeAll(taken);
            return sb.toString();
        }
    }

    /** Поставить ACK в очередь «попутной» отправки (B5). */
    public void queueAck(String msgId) {
        if (msgId == null) return;
        synchronized (piggybackAcks) {
            piggybackAcks.add(msgId);
        }
    }

    // ==================== KEEP-ALIVE И PROBE ====================

    private void keepAliveTick() {
        if (!running) return;
        String acks = takePiggybackAcks();
        BinaryFrame hb = Protocol.heartbeat(deviceId, System.currentTimeMillis(), acks);
        for (PeerConnection pc : peers.values()) {
            pc.send(hb);
        }
    }

    private void probeTick() {
        if (!running || peers.isEmpty()) return;
        for (String peer : peers.keySet()) {
            String probeId = deviceId + "t" + pingSeq.incrementAndGet();
            pendingPings.put(probeId, System.currentTimeMillis());
            PeerConnection pc = peers.get(peer);
            if (pc != null) {
                pc.send(Protocol.ping(deviceId, probeId, System.currentTimeMillis(), false, 0));
            }
        }
        // Не даём карте пингов расти, если ответы не приходят
        if (pendingPings.size() > 64) {
            pendingPings.clear();
        }
    }

    // ==================== ВСПОМОГАТЕЛЬНОЕ ====================

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(ServerSocket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    /** Одно TCP-соединение с собеседником. */
    private final class PeerConnection {

        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;
        private final Object sendLock = new Object();

        volatile String peerId;
        private volatile boolean closed;

        PeerConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.socket.setSoTimeout((int) P2pConfig.SOCKET_READ_TIMEOUT_MS);
            this.socket.setTcpNoDelay(true);
            this.in = new BufferedInputStream(socket.getInputStream());
            this.out = new BufferedOutputStream(socket.getOutputStream());
        }

        String address() {
            return socket.getInetAddress() != null
                    ? socket.getInetAddress().getHostAddress() : "-";
        }

        BinaryFrame read() throws IOException {
            return BinaryFrame.read(in);
        }

        void send(BinaryFrame frame) {
            if (closed || frame == null) return;
            try {
                synchronized (sendLock) {
                    BinaryFrame.write(out, frame);
                }
            } catch (IOException e) {
                log.w(TAG + " send failed (" + (peerId != null ? peerId : "unknown")
                        + "): " + e.getMessage());
                close();
            }
        }

        void close() {
            if (closed) return;
            closed = true;
            closeQuietly(socket);
        }
    }
}
