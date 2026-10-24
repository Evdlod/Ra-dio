package com.example.directtest.fast;

import com.example.directtest.P2pConfig;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Сверхбыстрый канал внутри подсети Wi-Fi Direct группы (B2/B3).
 *
 * <p>Как только группа сформирована, между её участниками уже есть IP-сеть
 * (владелец группы — DHCP-сервер, клиенты получают 192.168.49.x). Внутри этой
 * сети можно обмениваться UDP-датаграммами <b>без всякого обнаружения</b>:
 * supplicant не участвует, DNS-SD не нужен, рукопожатия нет. Это самый
 * короткий путь «сообщение → собеседник»: одна датаграмма, единицы
 * миллисекунд.
 *
 * <p>Что даёт канал:
 * <ul>
 *   <li>presence-маячок владельца группы каждую секунду — собеседник видит нас
 *       за время до 1 с, а не за 4–8 с, как при {@code discoverServices};</li>
 *   <li>доставка сообщений и ACK без установления TCP-сессии;</li>
 *   <li>замер RTT: ping/pong в той же подсети — основа для адаптивных
 *       таймаутов (B8) и для синхронизации часов (A3).</li>
 * </ul>
 *
 * <p>Ограничения, заложенные в дизайн:
 * <ul>
 *   <li>датаграмма должна влезать в MTU ({@link P2pConfig#GROUP_LINK_MAX_DATAGRAM}),
 *       иначе отправляем по TCP — потеря одного фрагмента убивает весь пакет;</li>
 *   <li>UDP не гарантирует доставку, поэтому ACK и повторные попытки остаются
 *       на уровне приложения (B5/B6), а канал считается «оптимистичным»:
 *       не подтвердилось — уходим на TCP/TXT.</li>
 * </ul>
 */
public final class GroupLinkChannel {

    /** Колбэки в сторону менеджера. */
    public interface Callback {
        /** Пришёл кадр (T_MSG/T_ACK/T_SYNC). */
        void onFrame(String senderDeviceId, BinaryFrame frame, String fromAddress);

        /** Собеседник проявился в подсети группы (presence). */
        void onPeerSeen(String deviceId, String address);

        /** Измерен RTT до собеседника. */
        void onRtt(String peerDeviceId, long rttMs);

        /** Состояние канала. */
        void onLinkState(boolean up, String detail);
    }

    private final String deviceId;
    private final String sessionId;
    private final CorrelationLog log = CorrelationLog.get();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong rttSeq = new AtomicLong();
    private final Map<String, String> peerAddresses = new ConcurrentHashMap<>();
    private final Map<String, Long> pendingPings = new ConcurrentHashMap<>();

    private volatile DatagramSocket socket;
    private volatile boolean groupOwner;
    private volatile InetAddress ownerAddress;
    private volatile InetAddress localAddress;
    private volatile Callback callback;

    private Thread listenThread;
    private Thread beaconThread;

    public GroupLinkChannel(String deviceId, String sessionId) {
        this.deviceId = CorrelationLog.shortId(deviceId);
        this.sessionId = sessionId != null ? sessionId : "-";
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    public boolean isRunning() {
        return running.get();
    }

    public int getKnownPeerCount() {
        return peerAddresses.size();
    }

    // ==================== LIFECYCLE ====================

    /**
     * Поднять канал. Вызывается при формировании группы.
     *
     * @param groupOwner   мы владелец группы (тогда слушаем фиксированный порт)
     * @param ownerAddr    адрес владельца группы (для клиента)
     * @param localAddr    наш адрес в подсети группы
     */
    public synchronized void start(boolean groupOwner, InetAddress ownerAddr, InetAddress localAddr) {
        if (!P2pConfig.ENABLE_GROUP_LINK_CHANNEL) return;
        if (running.get()) {
            // Параметры могли измениться (пересоздание группы) — обновляем
            this.groupOwner = groupOwner;
            this.ownerAddress = ownerAddr;
            this.localAddress = localAddr;
            return;
        }

        this.groupOwner = groupOwner;
        this.ownerAddress = ownerAddr;
        this.localAddress = localAddr;

        try {
            DatagramSocket s = new DatagramSocket(null);
            s.setReuseAddress(true);
            s.setBroadcast(true);
            if (groupOwner) {
                s.bind(new InetSocketAddress(P2pConfig.GROUP_LINK_PORT));
            } else {
                s.bind(new InetSocketAddress(0));
            }
            s.setSoTimeout(500);
            socket = s;
        } catch (IOException e) {
            log.event(CorrelationLog.EV_ERROR, null, null,
                    "where", "grouplink-bind", "msg", e.getMessage());
            notifyState(false, "bind failed: " + e.getMessage());
            return;
        }

        running.set(true);

        listenThread = new Thread(this::listenLoop, "wfd-glink-rx");
        listenThread.setDaemon(true);
        listenThread.start();

        beaconThread = new Thread(this::beaconLoop, "wfd-glink-tx");
        beaconThread.setDaemon(true);
        beaconThread.start();

        log.event(CorrelationLog.EV_LINK_UP, null, null,
                "channel", "udp-group",
                "role", groupOwner ? "GO" : "client",
                "port", String.valueOf(socket.getLocalPort()),
                "owner", ownerAddr != null ? ownerAddr.getHostAddress() : "-",
                "local", localAddr != null ? localAddr.getHostAddress() : "-");
        notifyState(true, "udp " + (groupOwner ? "GO" : "client"));
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) return;

        DatagramSocket s = socket;
        socket = null;
        if (s != null) {
            try {
                s.close();
            } catch (Exception ignored) {
            }
        }

        peerAddresses.clear();
        pendingPings.clear();

        log.event(CorrelationLog.EV_LINK_DOWN, null, null, "channel", "udp-group");
        notifyState(false, "stopped");
    }

    // ==================== ОТПРАВКА ====================

    /** Влезает ли кадр в одну датаграмму. */
    public static boolean fitsDatagram(BinaryFrame frame) {
        return frame != null && frame.encode().length <= P2pConfig.GROUP_LINK_MAX_DATAGRAM;
    }

    /**
     * Отправить кадр конкретному собеседнику.
     *
     * @return true, если датаграмма ушла (адрес известен или мы GO)
     */
    public boolean send(String targetDeviceId, BinaryFrame frame) {
        DatagramSocket s = socket;
        if (s == null || frame == null || !fitsDatagram(frame)) return false;

        InetAddress address = resolve(targetDeviceId);
        if (address == null) {
            // Адрес ещё не известен: владелец группы может отправить в broadcast,
            // клиент — всегда знает адрес владельца.
            if (groupOwner) {
                return sendRaw(broadcastAddress(), frame);
            }
            if (ownerAddress != null) {
                return sendRaw(ownerAddress, frame);
            }
            return false;
        }
        return sendRaw(address, frame);
    }

    /** Разослать кадр всем в подсети группы. */
    public void broadcast(BinaryFrame frame) {
        if (frame == null) return;
        sendRaw(broadcastAddress(), frame);
        if (ownerAddress != null) {
            sendRaw(ownerAddress, frame);
        }
    }

    /** Измерить RTT до собеседника (ping/pong). */
    public void ping(String targetDeviceId) {
        if (!running.get()) return;
        String probeId = CorrelationLog.shortId(deviceId) + "p" + rttSeq.incrementAndGet();
        pendingPings.put(probeId, System.currentTimeMillis());
        send(targetDeviceId, Protocol.ping(deviceId, probeId, System.currentTimeMillis(), false, 0));
    }

    public boolean knows(String targetDeviceId) {
        return targetDeviceId != null
                && (peerAddresses.containsKey(CorrelationLog.shortId(targetDeviceId))
                || ownerAddress != null);
    }

    private boolean sendRaw(InetAddress address, BinaryFrame frame) {
        DatagramSocket s = socket;
        if (s == null || address == null) return false;
        try {
            byte[] bytes = frame.encode();
            s.send(new DatagramPacket(bytes, bytes.length, address, P2pConfig.GROUP_LINK_PORT));
            return true;
        } catch (IOException e) {
            log.event(CorrelationLog.EV_ERROR, null, null,
                    "where", "grouplink-send", "msg", String.valueOf(e.getMessage()));
            return false;
        }
    }

    private InetAddress resolve(String targetDeviceId) {
        if (targetDeviceId == null) return null;
        String ip = peerAddresses.get(CorrelationLog.shortId(targetDeviceId));
        if (ip != null) {
            try {
                return InetAddress.getByName(ip);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    /** Broadcast-адрес подсети. Подсеть группы всегда /24, поэтому считаем от своего IP. */
    private InetAddress broadcastAddress() {
        InetAddress base = localAddress != null ? localAddress : ownerAddress;
        if (base == null) {
            try {
                return InetAddress.getByName("255.255.255.255");
            } catch (Exception e) {
                return null;
            }
        }
        byte[] raw = base.getAddress();
        if (raw.length == 4) {
            raw[3] = (byte) 0xFF;
        }
        try {
            return InetAddress.getByAddress(raw);
        } catch (Exception e) {
            return base;
        }
    }

    // ==================== ПРИЁМ ====================

    private void listenLoop() {
        byte[] buffer = new byte[P2pConfig.GROUP_LINK_MAX_DATAGRAM + 64];

        while (running.get()) {
            DatagramSocket s = socket;
            if (s == null) break;

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                s.receive(packet);
            } catch (SocketTimeoutException timeout) {
                continue;
            } catch (IOException e) {
                if (!running.get()) break;
                log.event(CorrelationLog.EV_ERROR, null, null,
                        "where", "grouplink-rx", "msg", String.valueOf(e.getMessage()));
                sleepQuietly(200);
                continue;
            }

            handleDatagram(packet.getData(), packet.getOffset(), packet.getLength(),
                    packet.getAddress());
        }
    }

    private void handleDatagram(byte[] data, int offset, int length, InetAddress from) {
        BinaryFrame frame = BinaryFrame.decode(data, offset, length);
        if (frame == null) {
            // Мусор или кадр другой версии — не ломаемся, просто считаем ошибкой
            log.event(CorrelationLog.EV_ERROR, null, null, "where", "grouplink-decode");
            return;
        }

        String fromIp = from != null ? from.getHostAddress() : "-";

        switch (frame.type) {
            case BinaryFrame.T_PRESENCE: {
                Protocol.Presence presence = Protocol.parsePresence(frame);
                if (presence.deviceId == null || presence.deviceId.equals(deviceId)) return;

                String key = CorrelationLog.shortId(presence.deviceId);
                boolean isNew = peerAddresses.put(key, fromIp) == null;
                if (isNew) {
                    log.event(CorrelationLog.EV_LINK_UP, null, key,
                            "channel", "udp-group", "role", "peer", "addr", fromIp);
                }
                Callback cb = callback;
                if (cb != null) cb.onPeerSeen(key, fromIp);
                break;
            }

            case BinaryFrame.T_PING: {
                Protocol.Ping ping = Protocol.parsePing(frame);
                if (ping.deviceId == null || ping.deviceId.equals(deviceId)) return;
                peerAddresses.put(CorrelationLog.shortId(ping.deviceId), fromIp);
                BinaryFrame pong = Protocol.ping(deviceId, ping.probeId,
                        System.currentTimeMillis(), true, ping.utcMs);
                sendRaw(from, pong);
                break;
            }

            case BinaryFrame.T_PONG: {
                Protocol.Ping pong = Protocol.parsePing(frame);
                Long sent = pendingPings.remove(pong.probeId);
                if (sent == null) return;
                long rtt = Math.max(0, System.currentTimeMillis() - sent);
                peerAddresses.put(CorrelationLog.shortId(pong.deviceId), fromIp);
                Callback cb = callback;
                if (cb != null) cb.onRtt(CorrelationLog.shortId(pong.deviceId), rtt);
                break;
            }

            default: {
                String sender = senderOf(frame);
                if (sender == null || sender.equals(deviceId)) return;
                peerAddresses.put(sender, fromIp);
                Callback cb = callback;
                if (cb != null) cb.onFrame(sender, frame, fromIp);
                break;
            }
        }
    }

    private static String senderOf(BinaryFrame frame) {
        switch (frame.type) {
            case BinaryFrame.T_MSG:
                return Protocol.parseMessage(frame).deviceId;
            case BinaryFrame.T_ACK:
                return Protocol.parseAck(frame).deviceId;
            case BinaryFrame.T_HELLO:
                return Protocol.parseHello(frame).deviceId;
            default:
                return null;
        }
    }

    // ==================== МАЯЧОК ====================

    private void beaconLoop() {
        int tick = 0;
        while (running.get()) {
            sleepQuietly(P2pConfig.GROUP_LINK_BEACON_MS);
            if (!running.get()) return;

            BinaryFrame presence = Protocol.presence(deviceId, sessionId, System.currentTimeMillis());

            if (groupOwner) {
                // Владелец группы вещает на всю подсеть — клиенты узнают о нём
                // за время до одного интервала маячка.
                sendRaw(broadcastAddress(), presence);
            } else if (ownerAddress != null) {
                // Клиент тоже должен «проявиться», иначе GO не знает его адрес
                sendRaw(ownerAddress, presence);
            }

            // Раз в 5 секунд — замер RTT до каждого известного собеседника
            if (++tick % 5 == 0) {
                for (String peer : peerAddresses.keySet()) {
                    ping(peer);
                }
            }
        }
    }

    private void notifyState(boolean up, String detail) {
        Callback cb = callback;
        if (cb != null) cb.onLinkState(up, detail);
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
