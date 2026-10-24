package com.example.directtest.transport;

import com.example.directtest.DiagnosticLogger;
import com.example.directtest.P2pConfig;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Дополнительный транспорт сообщений поверх P2P-группы Wi-Fi Direct.
 *
 * Зачем: DNS-SD TXT — «хрупкий» канал передачи полезной нагрузки.
 *  - одна запись ограничена 255 байтами (кириллица — 2 байта на символ);
 *  - supplicant агрессивно кэширует TXT, обновления теряются;
 *  - любое превышение лимита приводит к молчаливому отбрасыванию записи.
 *
 * Сокеты снимают все эти ограничения: длина сообщения не ограничена,
 * доставка подтверждается явно, ретрансмиссия управляемая.
 *
 * ВАЖНО: транспорт ДОПОЛНИТЕЛЬНЫЙ. Обнаружение устройств, presence и
 * резервная доставка продолжают работать через DNS-SD. Дубликаты сообщений
 * отсекаются по msgId на принимающей стороне.
 *
 * Включается флагом {@link P2pConfig#ENABLE_P2P_SOCKET_TRANSPORT}.
 */
public class P2pSocketTransport {

    private static final String TAG = "[Sock]";

    // ==================== ПРОТОКОЛ ====================
    // Формат строки (UTF-8, разделитель — перевод строки):
    //   WFD1|HELLO|<deviceId>|<sessionId>
    //   WFD1|MSG|<deviceId>|<msgId>|<textBase64>
    //   WFD1|ACK|<deviceId>|<msgId>
    //   WFD1|HB|<deviceId>
    // Поля не содержат '|': id — hex, текст — base64.

    private static final String PROTO = "WFD1";
    private static final String T_HELLO = "HELLO";
    private static final String T_MSG = "MSG";
    private static final String T_ACK = "ACK";
    private static final String T_HB = "HB";

    /**
     * Колбэки в сторону FastDiscoveryManager.
     */
    public interface Callback {
        void onPeerConnected(String deviceId);

        void onPeerDisconnected(String deviceId);

        void onMessageReceived(String senderDeviceId, String msgId, String text);

        void onAckReceived(String senderDeviceId, String msgId);
    }

    // ==================== СОСТОЯНИЕ ====================

    private final String deviceId;
    private final String sessionId;
    private final DiagnosticLogger log = DiagnosticLogger.getInstance();

    private final Map<String, PeerConnection> peers = new ConcurrentHashMap<>();

    private volatile boolean running = false;
    private volatile ServerSocket serverSocket;
    private volatile ExecutorService io;
    private volatile Callback callback;

    private Thread acceptThread;
    private Thread heartbeatThread;

    public P2pSocketTransport(String deviceId, String sessionId) {
        this.deviceId = deviceId;
        this.sessionId = sessionId;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    // ==================== LIFECYCLE ====================

    public synchronized void start() {
        if (running) return;

        running = true;
        io = Executors.newCachedThreadPool();

        acceptThread = new Thread(this::acceptLoop, "wfd-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        heartbeatThread = new Thread(this::heartbeatLoop, "wfd-hb");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();

        log.i(TAG + " transport started, port=" + P2pConfig.SOCKET_PORT);
    }

    public synchronized void stop() {
        if (!running) return;

        running = false;

        closeQuietly(serverSocket);
        serverSocket = null;

        for (PeerConnection pc : peers.values()) {
            pc.close();
        }
        peers.clear();

        ExecutorService executor = io;
        io = null;
        if (executor != null) {
            executor.shutdownNow();
        }

        log.i(TAG + " transport stopped");
    }

    public boolean isRunning() {
        return running;
    }

    public int getConnectedCount() {
        return peers.size();
    }

    public boolean isConnected(String targetDeviceId) {
        return targetDeviceId != null && peers.containsKey(normalizeId(targetDeviceId));
    }

    // ==================== ГРУППА ====================

    /**
     * Сообщить транспорту состояние P2P-группы.
     *
     * @param formed        сформирована ли группа
     * @param groupOwner    мы ли владелец группы
     * @param ownerAddress  адрес владельца группы (для клиента)
     */
    public void onGroupInfo(boolean formed, boolean groupOwner, InetAddress ownerAddress) {
        if (!running) return;

        if (!formed) {
            log.d(TAG + " group not formed, dropping peer sockets");
            for (PeerConnection pc : peers.values()) {
                pc.close();
            }
            peers.clear();
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

    /**
     * Подключиться к владельцу группы (клиентская сторона).
     */
    public void connectToOwner(InetAddress ownerAddress) {
        ExecutorService executor = io;
        if (!running || ownerAddress == null || executor == null) return;

        // Уже есть живое соединение — повторно не лезем
        if (!peers.isEmpty()) {
            log.d(TAG + " already connected to " + peers.keySet());
            return;
        }

        executor.execute(() -> {
            for (int attempt = 0; attempt < P2pConfig.SOCKET_CONNECT_ATTEMPTS && running; attempt++) {
                try {
                    Socket socket = new Socket();
                    socket.connect(
                            new InetSocketAddress(ownerAddress, P2pConfig.SOCKET_PORT),
                            P2pConfig.SOCKET_CONNECT_TIMEOUT_MS);

                    log.success(TAG + " connected to owner " + ownerAddress.getHostAddress());
                    setupPeer(socket);
                    return;
                } catch (IOException e) {
                    log.w(TAG + " connect attempt " + attempt + " failed: " + e.getMessage());
                    sleepQuietly(1000L << attempt);
                }
            }
            log.error(TAG + " could not connect to owner " + ownerAddress.getHostAddress()
                    + " — remaining on DNS-SD transport");
        });
    }

    // ==================== ОТПРАВКА ====================

    /**
     * Отправить сообщение по сокету.
     *
     * @param targetDeviceId ID получателя; null/пусто — всем подключённым
     * @return true если хотя бы одному получателю отправлено
     */
    public boolean sendMessage(String targetDeviceId, String msgId, String text) {
        String line = encode(T_MSG, new String[]{
                deviceId, msgId, Base64.getEncoder().encodeToString(
                (text == null ? "" : text).getBytes(StandardCharsets.UTF_8))
        });
        return sendTo(targetDeviceId, line);
    }

    /**
     * Отправить подтверждение по сокету.
     */
    public boolean sendAck(String targetDeviceId, String msgId) {
        return sendTo(targetDeviceId, encode(T_ACK, new String[]{deviceId, msgId}));
    }

    private boolean sendTo(String targetDeviceId, String line) {
        if (!running) return false;

        if (targetDeviceId == null || targetDeviceId.isEmpty()) {
            boolean sent = false;
            for (PeerConnection pc : peers.values()) {
                pc.send(line);
                sent = true;
            }
            return sent;
        }

        PeerConnection pc = peers.get(normalizeId(targetDeviceId));
        if (pc == null) return false;

        pc.send(line);
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
                    // Порт мог быть занят при пересоздании группы — повторяем
                    log.w(TAG + " accept loop error: " + e.getMessage() + " (retry in 2s)");
                    sleepQuietly(2000);
                }
            } finally {
                closeQuietly(server);
                if (serverSocket == server) {
                    serverSocket = null;
                }
            }
        }
    }

    private void setupPeer(Socket socket) {
        ExecutorService executor = io;
        if (!running || executor == null) {
            closeQuietly(socket);
            return;
        }

        try {
            PeerConnection pc = new PeerConnection(socket);
            executor.execute(() -> readLoop(pc));
            pc.send(encode(T_HELLO, new String[]{deviceId, sessionId}));
        } catch (IOException e) {
            log.w(TAG + " failed to set up peer: " + e.getMessage());
            closeQuietly(socket);
        }
    }

    private void readLoop(PeerConnection pc) {
        try {
            String line;
            while (running && (line = pc.readLine()) != null) {
                handleLine(pc, line);
            }
        } catch (IOException e) {
            if (running) {
                log.d(TAG + " read loop finished: " + e.getMessage());
            }
        } finally {
            pc.close();

            String peerId = pc.peerId;
            if (peerId != null) {
                peers.remove(peerId, pc);
                log.i(TAG + " peer disconnected: " + peerId);
                Callback cb = callback;
                if (cb != null) {
                    cb.onPeerDisconnected(peerId);
                }
            }
        }
    }

    private void handleLine(PeerConnection pc, String line) {
        if (line == null || line.isEmpty()) return;

        String[] parts = line.split("\\|", -1);
        if (parts.length < 3 || !PROTO.equals(parts[0])) {
            log.w(TAG + " malformed line from " + pc.socket.getInetAddress());
            return;
        }

        String type = parts[1];
        String senderId = parts[2];

        // Свои же записи игнорируем
        if (deviceId.equals(senderId)) return;

        Callback cb = callback;

        switch (type) {
            case T_HELLO:
                pc.peerId = senderId;
                peers.put(senderId, pc);
                log.success(TAG + " peer connected: " + senderId);
                if (cb != null) cb.onPeerConnected(senderId);
                break;

            case T_MSG:
                if (parts.length < 5) return;
                String msgId = parts[3];
                String text = decodeText(parts[4]);
                log.success(TAG + " MSG " + msgId + " from " + senderId);
                if (cb != null) cb.onMessageReceived(senderId, msgId, text);
                break;

            case T_ACK:
                if (parts.length < 4) return;
                String ackId = parts[3];
                log.success(TAG + " ACK " + ackId + " from " + senderId);
                if (cb != null) cb.onAckReceived(senderId, ackId);
                break;

            case T_HB:
                // Соединение живо — достаточно факта чтения строки
                if (pc.peerId == null) {
                    pc.peerId = senderId;
                    peers.put(senderId, pc);
                }
                break;

            default:
                log.d(TAG + " unknown message type: " + type);
                break;
        }
    }

    // ==================== HEARTBEAT ====================

    private void heartbeatLoop() {
        while (running) {
            sleepQuietly(P2pConfig.SOCKET_HB_INTERVAL_MS);
            if (!running) return;

            String line = encode(T_HB, new String[]{deviceId});
            for (PeerConnection pc : peers.values()) {
                pc.send(line);
            }
        }
    }

    // ==================== КОДИРОВАНИЕ ====================

    private static String encode(String type, String[] fields) {
        StringBuilder sb = new StringBuilder(PROTO).append('|').append(type);
        for (String field : fields) {
            sb.append('|').append(field == null ? "" : field);
        }
        return sb.toString();
    }

    private static String decodeText(String base64) {
        try {
            return new String(Base64.getDecoder().decode(base64), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return "";
        }
    }

    private static String normalizeId(String deviceId) {
        if (deviceId == null) return "";
        return deviceId.length() > 8 ? deviceId.substring(0, 8) : deviceId;
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

    /**
     * Одно TCP-соединение с собеседником.
     */
    private final class PeerConnection {

        private final Socket socket;
        private final BufferedReader in;
        private final BufferedWriter out;

        volatile String peerId;
        private volatile boolean closed;

        PeerConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.socket.setSoTimeout(P2pConfig.SOCKET_READ_TIMEOUT_MS);
            this.socket.setTcpNoDelay(true);
            this.in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            this.out = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        }

        String readLine() throws IOException {
            return in.readLine();
        }

        void send(String line) {
            if (closed) return;
            try {
                synchronized (this) {
                    out.write(line);
                    out.write('\n');
                    out.flush();
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
