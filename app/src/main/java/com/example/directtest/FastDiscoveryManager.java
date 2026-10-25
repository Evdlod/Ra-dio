package com.example.directtest;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.net.wifi.p2p.WifiP2pDevice;
import android.net.wifi.p2p.WifiP2pGroup;
import android.net.wifi.p2p.WifiP2pManager;
import android.net.wifi.p2p.nsd.WifiP2pDnsSdServiceInfo;
import android.net.wifi.p2p.nsd.WifiP2pDnsSdServiceRequest;
import android.net.wifi.p2p.nsd.WifiP2pServiceInfo;
import android.net.wifi.p2p.nsd.WifiP2pServiceRequest;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;

import com.example.directtest.model.DiscoveredDevice;
import com.example.directtest.model.PendingMessage;
import com.example.directtest.model.SlotInfo;
import com.example.directtest.sync.DeviceState;
import com.example.directtest.sync.DeviceStateRepository;
import com.example.directtest.sync.SyncManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import android.net.wifi.WifiManager;
import android.net.wifi.WpsInfo;
import android.net.wifi.p2p.WifiP2pConfig;
import android.net.wifi.p2p.WifiP2pInfo;

import com.example.directtest.fast.BinaryFrame;
import com.example.directtest.fast.BloomFilter;
import com.example.directtest.fast.CorrelationLog;
import com.example.directtest.fast.GroupLinkChannel;
import com.example.directtest.fast.HwGuard;
import com.example.directtest.fast.LinkQuality;
import com.example.directtest.fast.PeerClock;
import com.example.directtest.fast.Protocol;
import com.example.directtest.fast.RelayCache;
import com.example.directtest.fast.SyncWindow;
import com.example.directtest.transport.P2pSocketTransport;

public class FastDiscoveryManager {

    private static final String TAG = "FastDiscovery";

    /**
     * Интервал сброса DNS-SD запросов для очистки кэша (мс)
     */
    private static final long DNS_CACHE_RESET_INTERVAL = 45_000;

    /**
     * Интервал перерегистрации сервисов для обновления DNS кэша (мс)
     */
    private static final long SERVICE_REREGISTER_INTERVAL = 30_000;

    // ==================== STATE ====================
    private final Context context;
    private WifiP2pManager manager;
    private WifiP2pManager.Channel channel;
    private BroadcastReceiver receiver;

    // WiFi Lock для предотвращения засыпания WiFi
    private WifiManager.WifiLock wifiLock;

    // Диагностика lifecycle
    private long lastDiscoveryTime = 0;
    private long lastPeersFoundTime = 0;
    private int discoveryRestartCount = 0;

    private final String deviceId;
    private final String shortDeviceId;
    private final String sessionId;
    private String macAddress = "02:00:00:00:00:00";

    private WifiP2pDnsSdServiceInfo mainServiceInfo;
    private WifiP2pDnsSdServiceInfo ackServiceInfo;
    private WifiP2pDnsSdServiceInfo syncServiceInfo;
    private final Map<Integer, SlotInfo> messageSlots = new ConcurrentHashMap<>();
    private final List<WifiP2pServiceRequest> serviceRequests = new ArrayList<>();

    private final Map<String, DiscoveredDevice> deviceCache = new ConcurrentHashMap<>();

    /**
     * Индекс deviceId -> MAC, под которым запись лежит в deviceCache.
     *
     * Кэш ключуется MAC/P2P-адресом, но на Android 10+ адрес Wi-Fi Direct
     * рандомизируется (а свой отдаётся как 02:00:00:00:00:00). Одно и то же
     * устройство после перезагрузки получало новую запись, плодились дубли
     * и терялась история. Индекс позволяет склеивать их по deviceId.
     */
    private final Map<String, String> deviceIdToAddress = new ConcurrentHashMap<>();

    private final Map<String, PendingMessage> pendingMessages = new ConcurrentHashMap<>();
    private final Set<String> processedMessageIds = Collections.synchronizedSet(new LinkedHashSet<>());

    // ACK tracking
    private final Map<String, Long> activeIncomingMessages = new ConcurrentHashMap<>();
    private final Set<String> pendingAcksToSend = Collections.synchronizedSet(new LinkedHashSet<>());

    /** Сколько раз ACK уже был опубликован (защита от бесконечного вещания) */
    private final Map<String, Integer> ackPublishAttempts = new ConcurrentHashMap<>();
    private static final int MAX_ACK_ATTEMPTS = 12;

    // Дедупликация TXT записей
    private final Map<String, Long> recentTxtRecords = new ConcurrentHashMap<>();

    // Трекинг обработанных ACK
    private final Set<String> processedAcks = Collections.synchronizedSet(new LinkedHashSet<>());

    // SYNC system
    private DeviceStateRepository stateRepository;
    private SyncManager syncManager;

    // Сокетный транспорт поверх P2P-группы (дополнительный канал доставки)
    private P2pSocketTransport socketTransport;
    private volatile boolean groupFormed = false;
    private volatile boolean groupOwner = false;
    private long lastGroupAttempt = 0;

    private final AtomicLong heartbeatSeq = new AtomicLong(0);
    private final AtomicInteger messageIdCounter = new AtomicInteger(0);
    private final AtomicInteger txtRecordsReceived = new AtomicInteger(0);
    private final AtomicInteger serviceResponsesReceived = new AtomicInteger(0);

    private volatile boolean isRunning = false;
    private volatile boolean discoveryInProgress = false;
    private volatile long lastDiscoverCall = 0;
    private volatile boolean mainServicePublishInFlight = false;
    private volatile long mainServicePublishSince = 0;
    private long lastNewDeviceTime = 0;
    private long currentInterval = P2pConfig.NORMAL_INTERVAL;
    private long lastHeartbeatSentTime = 0;

    private DiscoveryListener listener;
    private final Handler handler = new Handler(Looper.getMainLooper());
    private final DiagnosticLogger log = DiagnosticLogger.getInstance();

    // ==================== БЫСТРЫЙ ПУТЬ И КОРРЕЛЯЦИЯ ====================

    /** Структурированный журнал для сопоставления логов двух телефонов. */
    private final CorrelationLog corr = CorrelationLog.get();

    /** Расхождение часов с каждым собеседником — базис для окон поиска (A3). */
    private final PeerClock peerClock = new PeerClock();

    /** Качество быстрого канала: RTT/потери → адаптивные таймауты (B8). */
    private final LinkQuality linkQuality = new LinkQuality();

    /** Защита от лавины при ретрансляции (C6). */
    private final RelayCache relayCache = new RelayCache();

    /** UDP-канал внутри подсети группы (B2/B3). */
    private GroupLinkChannel groupLink;

    /** Отложенные публикации TXT-слотов: если быстрый канал успел — не публикуем (B1). */
    private final Map<String, Runnable> pendingTxtFallbacks = new ConcurrentHashMap<>();

    /**
     * Слоты, зарезервированные под сообщения, которые ещё идут быстрым путём.
     * Держим их отдельно от {@code messageSlots}: там запись означает уже
     * зарегистрированный DNS-SD сервис, а освобождение записи удаляет
     * {@code PendingMessage} — то есть отменило бы ожидание ACK.
     */
    private final Set<Integer> reservedSlots = Collections.synchronizedSet(new HashSet<>());

    /** Счётчик фаз поиска для чередования discoverPeers/discoverServices (A2). */
    private volatile int discoveryPhase = 0;

    // ==================== CONSTRUCTOR ====================

    public FastDiscoveryManager(Context context) {
        this.context = context.getApplicationContext();
        this.deviceId = generateDeviceId();
        this.shortDeviceId = deviceId.substring(0, 8);
        this.sessionId = generateSessionId();

        // Инициализация SYNC системы
        stateRepository = new DeviceStateRepository(context);
        stateRepository.initialize(deviceId, sessionId);

        syncManager = new SyncManager(stateRepository);
        syncManager.setCallback(new SyncManager.SyncCallback() {
            @Override
            public void onPublishSync(String targetDeviceId, List<String> mySentIds, List<String> myRecvIds) {
                registerSyncService(targetDeviceId, mySentIds, myRecvIds);
            }

            @Override
            public void onResendMessage(String targetDeviceId, String msgId, String text) {
                resendMessage(targetDeviceId, msgId, text);
            }

            @Override
            public void onSyncComplete(String deviceId) {
                log.success("SYNC complete with " + deviceId);
            }
        });

        // ДОБАВЛЕНО: Загрузка сохранённых устройств в кэш
        loadSavedDevicesToCache();

        log.divider("FastDiscoveryManager INIT");
        log.i("Device ID: " + shortDeviceId);
        log.i("Session ID: " + sessionId);
        log.i("Loaded " + stateRepository.getAll().size() + " saved states, " + deviceCache.size() + " devices in cache");
        log.i("Android: " + Build.VERSION.RELEASE + " (API " + Build.VERSION.SDK_INT + ")");
        log.i("Model: " + Build.MODEL);

        // Журнал корреляции открываем сразу: дальше каждое событие попадёт в
        // файл с общим trace-идентификатором, пригодным для склейки логов
        // двух телефонов.
        corr.init(context, deviceId, sessionId);
        corr.event(CorrelationLog.EV_START, null, null,
                "role", "manager",
                "model", Build.MANUFACTURER + "_" + Build.MODEL,
                "api", String.valueOf(Build.VERSION.SDK_INT),
                "proto", P2pConfig.PROTOCOL_VERSION,
                "fast_path", P2pConfig.SOCKET_IS_PRIMARY_PATH ? "1" : "0",
                "group_link", P2pConfig.ENABLE_GROUP_LINK_CHANNEL ? "1" : "0");
        corr.event(CorrelationLog.EV_ENV, null, null, "env", HwGuard.describe(context));
    }

    /**
     * Загрузить сохранённые устройства из репозитория в deviceCache
     */
    private void loadSavedDevicesToCache() {
        Map<String, DeviceState> savedStates = stateRepository.getAll();
        int loaded = 0;

        for (Map.Entry<String, DeviceState> entry : savedStates.entrySet()) {
            DeviceState state = entry.getValue();

            // Пропускаем устройства без адреса
            if (state.address == null || state.address.isEmpty()) {
                log.w("Skipping device without address: " + state.deviceId);
                continue;
            }

            // Проверяем что устройство ещё не в кэше
            if (deviceCache.containsKey(state.address)) {
                continue;
            }

            DiscoveredDevice dd = new DiscoveredDevice();
            dd.deviceId = state.deviceId;
            dd.address = state.address;
            dd.name = state.name != null ? state.name : "Saved Device";
            dd.hasOurApp = true;
            dd.sessionId = state.lastSessionId;
            dd.firstSeen = state.firstSeen;
            dd.lastSeen = state.lastSeen;

            // Загружаем историю отправленных сообщений
            for (DeviceState.MessageRecord m : state.sentMessages) {
                dd.addSentMessageFromState(m.msgId, m.text, m.timestamp, m.acked, m.ackTime);
            }

            // Загружаем историю полученных сообщений
            for (DeviceState.MessageRecord m : state.recvMessages) {
                dd.addReceivedMessageFromState(m.msgId, m.text, m.timestamp, m.acked);
            }

            deviceCache.put(state.address, dd);
            if (state.deviceId != null) {
                deviceIdToAddress.put(state.deviceId, state.address);
            }
            loaded++;

            log.d("Restored device: " + dd.getShortId() + " (" + dd.name + ") addr=" + state.address);
        }

        log.i("Loaded " + loaded + " devices from repository to cache");
    }

    private String generateDeviceId() {
        String androidId = android.provider.Settings.Secure.getString(
                context.getContentResolver(),
                android.provider.Settings.Secure.ANDROID_ID
        );
        if (androidId == null || androidId.isEmpty()) {
            androidId = "emu_" + Build.MODEL + "_" + Build.FINGERPRINT.hashCode();
        }
        return UUID.nameUUIDFromBytes(androidId.getBytes()).toString().replace("-", "");
    }

    /**
     * Генерация sessionId на основе timestamp.
     * Формат: hex-encoded секунды (8 символов).
     * Позволяет сравнивать sessionId для определения более новой сессии.
     */
    private String generateSessionId() {
        long timestampSec = System.currentTimeMillis() / 1000;
        return Long.toHexString(timestampSec);
    }

    // ==================== SESSION ID VALIDATION ====================

    /**
     * Сравнить два sessionId.
     * @return true если newSid новее (больше) чем oldSid
     */
    private boolean isNewerSessionId(String newSid, String oldSid) {
        if (newSid == null) return false;
        if (oldSid == null) return true;
        if (newSid.equals(oldSid)) return false;

        try {
            // Сравниваем как hex timestamp
            long newTs = Long.parseLong(newSid, 16);
            long oldTs = Long.parseLong(oldSid, 16);
            return newTs > oldTs;
        } catch (NumberFormatException e) {
            // Один или оба не в hex формате
            try {
                // Если newSid - hex timestamp, а oldSid - нет, считаем новый новее
                Long.parseLong(newSid, 16);
                return true;
            } catch (NumberFormatException e2) {
                // newSid тоже не hex - сравниваем как строки
                return newSid.compareTo(oldSid) > 0;
            }
        }
    }

    /**
     * Проверить и обновить sessionId устройства.
     * @return true если запись валидна и должна быть обработана
     */
    private boolean validateSessionId(String senderId, String incomingSid, DiscoveredDevice dd) {
        if (incomingSid == null || incomingSid.isEmpty()) {
            return true; // Старый формат без sessionId
        }

        if (dd.sessionId == null) {
            // Первый контакт - запоминаем
            dd.sessionId = incomingSid;
            log.i("Session set for " + senderId + ": " + incomingSid);

            // Проверяем в репозитории
            DeviceState state = stateRepository.getOrCreate(senderId);
            if (state.lastSessionId != null && isNewerSessionId(incomingSid, state.lastSessionId)) {
                log.w("New session for " + senderId + " (repo): " + state.lastSessionId + " -> " + incomingSid);
                state.clearMessages();
                dd.clearHistory();
            }
            state.lastSessionId = incomingSid;
            stateRepository.save();
            return true;
        }

        if (incomingSid.equals(dd.sessionId)) {
            // Совпадает - OK
            return true;
        }

        if (isNewerSessionId(incomingSid, dd.sessionId)) {
            // Новый sessionId больше - устройство перезапустилось
            log.w("Newer session for " + senderId + ": " + dd.sessionId + " -> " + incomingSid);
            dd.sessionId = incomingSid;
            dd.clearHistory();

            DeviceState state = stateRepository.get(senderId);
            if (state != null) {
                state.clearMessages();
                state.lastSessionId = incomingSid;
                stateRepository.save();
            }
            return true;
        }

        // Старый sessionId - это кэш, игнорируем
        log.d("Ignoring older cached record from " + senderId +
                " sid=" + incomingSid + " (current: " + dd.sessionId + ")");
        return false;
    }

    // ==================== PUBLIC API ====================

    public void start(DiscoveryListener listener) {
        log.divider("START");
        this.listener = listener;

        manager = (WifiP2pManager) context.getSystemService(Context.WIFI_P2P_SERVICE);
        if (manager == null) {
            log.error("WiFi P2P not supported!");
            notifyError("WiFi P2P not supported");
            return;
        }
        log.success("WifiP2pManager obtained");

        channel = manager.initialize(context, Looper.getMainLooper(), () -> {
            log.w("Channel disconnected!");
            logDiagnosticState("CHANNEL_DISCONNECTED");
            if (isRunning) {
                handler.postDelayed(this::softReconnect, 2000);
            }
        });
        log.success("Channel initialized");

        // Получаем WiFi Lock для предотвращения засыпания
        acquireWifiLock();

        isRunning = true;
        lastDiscoverCall = 0;
        discoveryRestartCount = 0;
        txtRecordsReceived.set(0);
        serviceResponsesReceived.set(0);

        setDeviceNameWithMarker();
        startSocketTransport();
        registerReceiver();
        checkEnvironment();
        initializeAndStart();

        log.i("Service started, WiFi Lock: " + (wifiLock != null && wifiLock.isHeld()));
        notifyStatus("Starting...");
    }

    public void stop() {
        log.divider("STOP");
        logDiagnosticState("STOPPING");

        isRunning = false;

        // Явно останавливаем периодические задачи
        handler.removeCallbacks(dnsCacheResetRunnable);  // ДОБАВИТЬ (опционально)

        handler.removeCallbacksAndMessages(null);

        // Принудительное сохранение состояния
        if (stateRepository != null) {
            stateRepository.saveNow();
            log.i("State saved");
        }

        if (receiver != null) {
            try { context.unregisterReceiver(receiver); } catch (Exception e) {}
            receiver = null;
        }

        if (manager != null && channel != null) {
            manager.clearLocalServices(channel, null);
            manager.clearServiceRequests(channel, null);
            manager.stopPeerDiscovery(channel, null);
            manager.removeGroup(channel, null);
        }

        // Останавливаем сокетный транспорт и UDP-канал группы
        stopSocketTransport();
        stopGroupLink();

        // Снимаем отложенные публикации TXT — иначе они сработают после stop()
        for (Runnable fallback : pendingTxtFallbacks.values()) {
            handler.removeCallbacks(fallback);
        }
        pendingTxtFallbacks.clear();
        relayCache.clear();

        corr.event(CorrelationLog.EV_STOP, null, null,
                "devices", String.valueOf(deviceCache.size()),
                "restarts", String.valueOf(discoveryRestartCount));
        corr.flush();

        // Освобождаем WiFi Lock
        releaseWifiLock();

        messageSlots.clear();
        reservedSlots.clear();
        pendingMessages.clear();
        serviceRequests.clear();
        deviceCache.clear();
        deviceIdToAddress.clear();
        activeIncomingMessages.clear();
        pendingAcksToSend.clear();
        ackPublishAttempts.clear();
        processedMessageIds.clear();
        recentTxtRecords.clear();
        processedAcks.clear();

        log.i("Stop complete. Discovery restarts: " + discoveryRestartCount);
        notifyStatus("Stopped");
    }

    /**
     * Проверка окружения (D1–D5).
     *
     * Главное: на Xiaomi/Redmi/vivo при выключенном Location Mode
     * discoverPeers отдаёт пустой список без ошибки. Пользователь видит
     * «приложение не находит второй телефон», хотя дело в системной настройке.
     * Поэтому проверяем явно, пишем в журнал и говорим текстом, что включить.
     */
    private void checkEnvironment() {
        String problem = HwGuard.environmentProblem(context);
        corr.event(CorrelationLog.EV_ENV, null, null,
                "env", HwGuard.describe(context),
                "problem", problem == null ? "none" : problem);

        if (problem == null) {
            log.success("[ENV] " + HwGuard.describe(context));
            return;
        }

        log.error("[ENV] " + problem);
        notifyStatus(problem);
        if (!HwGuard.isLocationEnabled(context)) {
            notifyError(problem);
            // Подсказка по автозапуску — вторая по частоте причина «замолкания»
            log.i("[ENV] " + HwGuard.autostartHint());
        }
    }

    /**
     * Получить WiFi Lock для предотвращения засыпания WiFi
     */
    private void acquireWifiLock() {
        try {
            WifiManager wifiManager = (WifiManager) context.getApplicationContext()
                    .getSystemService(Context.WIFI_SERVICE);
            if (wifiManager == null) return;

            // Wi-Fi выключен — приём невозможен, о проблеме нужно сказать явно
            if (!wifiManager.isWifiEnabled()) {
                log.error("Wi-Fi is DISABLED — discovery will not work");
                notifyError("Wi-Fi выключен — включите его для работы рации");
            }

            // WIFI_MODE_FULL_HIGH_PERF устарел с API 29; на части прошивок он не
            // мешает supplicant уходить в power-save при погашенном экране.
            int mode = Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q
                    ? WifiManager.WIFI_MODE_FULL_LOW_LATENCY
                    : WifiManager.WIFI_MODE_FULL_HIGH_PERF;

            wifiLock = wifiManager.createWifiLock(mode, "DirectTest:DiscoveryLock");
            wifiLock.setReferenceCounted(false);
            wifiLock.acquire();
            log.success("WiFi Lock acquired (mode=" + mode + ")");
        } catch (Exception e) {
            log.w("Failed to acquire WiFi Lock: " + e.getMessage());
        }
    }

    /**
     * Освободить WiFi Lock
     */
    private void releaseWifiLock() {
        try {
            if (wifiLock != null && wifiLock.isHeld()) {
                wifiLock.release();
                log.i("WiFi Lock released");
            }
        } catch (Exception e) {
            log.w("Failed to release WiFi Lock: " + e.getMessage());
        }
        wifiLock = null;
    }

    public void clearAll() {
        log.divider("CLEAR ALL");

        // ВНИМАНИЕ: removeCallbacksAndMessages(null) снимает ВСЕ задачи,
        // включая цепочку discovery. Раньше после «Очистить» больше никто не
        // вызывал discoverPeers/discoverServices, и приём не возвращался до
        // перезапуска сервиса. Поэтому в конце метода цикл поднимается заново.
        handler.removeCallbacksAndMessages(null);

        for (SlotInfo slot : messageSlots.values()) {
            if (slot.serviceInfo != null && slot.isRegistered) {
                manager.removeLocalService(channel, slot.serviceInfo, null);
            }
        }
        messageSlots.clear();
        reservedSlots.clear();

        if (ackServiceInfo != null) {
            manager.removeLocalService(channel, ackServiceInfo, null);
            ackServiceInfo = null;
        }

        if (syncServiceInfo != null) {
            manager.removeLocalService(channel, syncServiceInfo, null);
            syncServiceInfo = null;
        }

        pendingMessages.clear();
        activeIncomingMessages.clear();
        pendingAcksToSend.clear();
        ackPublishAttempts.clear();
        processedMessageIds.clear();
        recentTxtRecords.clear();
        processedAcks.clear();

        for (DiscoveredDevice dd : deviceCache.values()) {
            dd.clearHistory();
        }

        messageIdCounter.set(0);
        txtRecordsReceived.set(0);
        serviceResponsesReceived.set(0);
        heartbeatSeq.set(0);

        // Очищаем состояния в репозитории
        if (stateRepository != null) {
            for (DeviceState state : stateRepository.getAll().values()) {
                state.clearMessages();
            }
            stateRepository.saveNow();
        }

        if (syncManager != null) {
            syncManager.clearAllPending();
        }

        DiagnosticLogger.getInstance().clear();

        log.success("All cleared");
        log.divider("FRESH START");

        schedulePeriodicTasks();

        // Перезапуск цикла обнаружения: main-сервис мог остаться от прошлой
        // публикации, но таймеры и запросы discovery были снесены выше.
        lastDiscoverCall = 0;
        mainServicePublishInFlight = false;
        if (isRunning) {
            if (mainServiceInfo != null) {
                requestDiscovery("after-clear");
            } else {
                registerMainService(() -> requestDiscovery("after-clear"));
            }
        }

        notifyStatus("Cleared. Devices: " + deviceCache.size());
    }

    public String sendMessage(String message, String targetDeviceId) {
        if (!isRunning) return null;

        // ДОБАВЛЕНО: Проверка лимита неподтверждённых сообщений
        int pendingCount = countPendingMessagesTo(targetDeviceId);
        if (pendingCount >= P2pConfig.MAX_MSG_SLOTS) {
            String error = "Лимит: " + P2pConfig.MAX_MSG_SLOTS + " неподтверждённых сообщений";
            log.w(error + " to " + targetDeviceId);
            notifyError(error);
            return null;
        }

        int freeSlot = findFreeSlot();
        // ИЗМЕНЕНО: Вместо вытеснения - ошибка
        if (freeSlot < 0) {
            String error = "Нет свободных слотов для отправки";
            log.w(error);
            notifyError(error);
            return null;
        }

        String msgId = shortDeviceId + "_" + sessionId + "_" + messageIdCounter.incrementAndGet();
        PendingMessage pending = new PendingMessage(msgId, message, targetDeviceId, freeSlot);
        pendingMessages.put(msgId, pending);

        // Сохранение в репозиторий для SYNC
        if (targetDeviceId != null) {
            DeviceState state = stateRepository.getOrCreate(targetDeviceId);
            state.addSentMessage(msgId, message);
            stateRepository.save();
            log.d("Saved to state repository: " + msgId + " -> " + targetDeviceId);
        }

        // Записываем в историю устройства (для UI)
        for (DiscoveredDevice dd : deviceCache.values()) {
            if (targetDeviceId != null) {
                if (targetDeviceId.equals(dd.deviceId) ||
                        (dd.deviceId != null && dd.deviceId.startsWith(targetDeviceId))) {
                    dd.addSentMessage(msgId, message, freeSlot);
                    break;
                }
            } else if (dd.hasOurApp) {
                dd.addSentMessage(msgId, message, freeSlot);
            }
        }

        // B1: сначала быстрый путь. Если он принял сообщение, публикация в
        // TXT-слот откладывается — при мгновенном ACK эфир вообще не трогаем.
        boolean fastPath = sendViaFastPath(msgId, message, targetDeviceId);

        corr.event(CorrelationLog.EV_MSG_SEND, msgId, targetDeviceId,
                "slot", String.valueOf(freeSlot),
                "fast", fastPath ? "1" : "0",
                "len", String.valueOf(message != null ? message.length() : 0));

        if (fastPath && P2pConfig.SOCKET_IS_PRIMARY_PATH) {
            scheduleTxtFallback(freeSlot, msgId, message, targetDeviceId);
        } else {
            registerMessageSlot(freeSlot, msgId, message, targetDeviceId);
        }

        log.i("SEND MESSAGE: " + msgId + " slot=" + freeSlot +
                (targetDeviceId != null ? " to=" + targetDeviceId : " (broadcast)") +
                (fastPath ? " [fast path]" : " [txt path]"));

        if (listener != null) {
            listener.onMessageSent(msgId, message, targetDeviceId);
        }
        return msgId;
    }

    /**
     * Отправка быстрым путём (B1): сначала UDP внутри группы, затем TCP.
     *
     * <p>UDP — самый короткий путь: одна датаграмма, без сессии и без
     * обнаружения. TCP добавлен как надёжная страховка (подтверждения и
     * повторы). Оба канала несут один и тот же {@code msgId}, поэтому
     * получатель отбросит второй экземпляр.
     */
    private boolean sendViaFastPath(String msgId, String message, String targetDeviceId) {
        boolean sent = false;

        if (groupLink != null && targetDeviceId != null && groupLink.knows(targetDeviceId)) {
            BinaryFrame frame = Protocol.message(shortDeviceId, sessionId, msgId,
                    message, "", System.currentTimeMillis(), P2pConfig.RELAY_TTL);
            if (GroupLinkChannel.fitsDatagram(frame) && groupLink.send(targetDeviceId, frame)) {
                log.success("UDP send OK: " + msgId + " to " + targetDeviceId);
                corr.event(CorrelationLog.EV_MSG_TX, msgId, targetDeviceId,
                        "channel", "udp-group", "bytes", String.valueOf(frame.payload.length));
                sent = true;
            }
        }

        if (socketTransport != null
                && socketTransport.sendMessage(targetDeviceId, msgId, message,
                P2pConfig.PRIORITY_NORMAL)) {
            log.success("SOCKET send OK: " + msgId
                    + (targetDeviceId != null ? " to " + targetDeviceId : " (broadcast)"));
            sent = true;
        }

        return sent;
    }

    /**
     * Отложенная публикация TXT-слота (B1).
     *
     * <p>Раньше сообщение всегда публиковалось в DNS-SD: это гарантировало
     * доставку, но добавляло к каждой отправке регистрацию сервиса, обновление
     * DNS-кэша собеседника и «мигание» записи. Теперь при живом быстром канале
     * публикация ждёт {@link P2pConfig#TXT_FALLBACK_DELAY_MS}: если ACK пришёл
     * быстрее — TXT не нужен вовсе.
     */
    private void scheduleTxtFallback(int slotIndex, String msgId, String message, String targetDeviceId) {
        // Резервируем слот сразу, иначе следующее сообщение займёт тот же индекс
        reservedSlots.add(slotIndex);

        Runnable fallback = () -> {
            pendingTxtFallbacks.remove(msgId);
            if (!isRunning) {
                reservedSlots.remove(slotIndex);
                return;
            }

            if (isMessageAcked(msgId)) {
                reservedSlots.remove(slotIndex);
                log.d("TXT fallback skipped (acked): " + msgId);
                corr.event(CorrelationLog.EV_TXT_PUB, msgId, targetDeviceId,
                        "result", "skipped-acked");
                return;
            }

            log.w("Fast path gave no ACK for " + msgId + " — publishing TXT slot");
            corr.event(CorrelationLog.EV_TXT_PUB, msgId, targetDeviceId,
                    "result", "published", "reason", "no-fast-ack");
            reservedSlots.remove(slotIndex);
            registerMessageSlot(slotIndex, msgId, message, targetDeviceId);
        };

        pendingTxtFallbacks.put(msgId, fallback);
        handler.postDelayed(fallback, P2pConfig.TXT_FALLBACK_DELAY_MS);
    }

    /** Отменить отложенную публикацию TXT — сообщение уже подтверждено. */
    private void cancelTxtFallback(String msgId) {
        Runnable fallback = pendingTxtFallbacks.remove(msgId);
        if (fallback != null) {
            handler.removeCallbacks(fallback);
            log.d("TXT fallback cancelled: " + msgId);
        }
        // Резерв снимаем сразу — иначе индекс «залипнет» и слотов станет меньше.
        PendingMessage pending = pendingMessages.get(msgId);
        if (pending != null) {
            reservedSlots.remove(pending.slotIndex);
        }
    }

    /** Есть ли живой быстрый канал хотя бы до кого-то. */
    private boolean fastPathAvailable() {
        return (socketTransport != null && socketTransport.getConnectedCount() > 0)
                || (groupLink != null && groupLink.getKnownPeerCount() > 0);
    }

    /**
     * Подсчёт неподтверждённых сообщений для конкретного устройства
     */
    private int countPendingMessagesTo(String targetDeviceId) {
        int count = 0;
        for (PendingMessage pm : pendingMessages.values()) {
            if (targetDeviceId == null) {
                // Broadcast - считаем все
                count++;
            } else if (targetDeviceId.equals(pm.targetDeviceId)) {
                count++;
            }
        }
        return count;
    }



    /**
     * Принудительное обновление эфира.
     *
     * ВАЖНО: здесь нельзя вызывать stopPeerDiscovery()/clearServiceRequests().
     * Раньше forceRefresh() вызывался при каждом onServiceConnected() (вход в
     * приложение, поворот экрана) и сносил зарегистрированные service requests:
     * на время их отсутствия TXT-записи не приходили, входящие сообщения и
     * ACK терялись, а supplicant отвечал BUSY.
     */
    public void forceRefresh() {
        if (!isRunning) return;
        log.divider("FORCE REFRESH");
        lastDiscoverCall = 0;            // разрешаем немедленный проход
        handler.removeCallbacks(discoveryRunnable);
        requestDiscovery("forceRefresh");
    }

    // ==================== GETTERS ====================

    public String getDeviceId() { return deviceId; }
    public String getShortDeviceId() { return shortDeviceId; }
    public String getSessionId() { return sessionId; }
    public String getMacAddress() { return macAddress; }
    public boolean isRunning() { return isRunning; }
    public int getDeviceCount() { return deviceCache.size(); }
    public long getHeartbeatSeq() { return heartbeatSeq.get(); }
    public long getLastHeartbeatSentTime() { return lastHeartbeatSentTime; }
    public int getTxtRecordsReceived() { return txtRecordsReceived.get(); }
    public int getServiceResponsesReceived() { return serviceResponsesReceived.get(); }
    public int getPendingAcksCount() { return pendingAcksToSend.size(); }

    public int getOnlineDeviceCount() {
        int count = 0;
        for (DiscoveredDevice d : deviceCache.values()) {
            if (d.isOnline()) count++;
        }
        return count;
    }

    public List<DiscoveredDevice> getAllDevices() {
        return new ArrayList<>(deviceCache.values());
    }

    public DeviceStateRepository getStateRepository() {
        return stateRepository;
    }

    public String getDiagnosticInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══ DEVICE INFO ═══\n");
        sb.append("My ID: ").append(shortDeviceId).append("\n");
        sb.append("Session: ").append(sessionId).append("\n");
        sb.append("MAC: ").append(macAddress).append("\n");
        sb.append("Heartbeat: ").append(heartbeatSeq.get()).append("\n");
        sb.append("TXT received: ").append(txtRecordsReceived.get()).append("\n");
        sb.append("Pending ACKs: ").append(pendingAcksToSend.size()).append("\n");
        sb.append("Active incoming: ").append(activeIncomingMessages.size()).append("\n");
        sb.append("Saved devices: ").append(stateRepository.getAll().size()).append("\n");
        sb.append("\n═══ DEVICES ═══\n");
        for (DiscoveredDevice dd : deviceCache.values()) {
            sb.append("• ").append(dd.name).append(" [").append(dd.getShortId()).append("]\n");
            sb.append("  Session: ").append(dd.sessionId).append("\n");
            sb.append("  Visible msgs: ").append(dd.currentVisibleMsgIds).append("\n");
            sb.append("  Pending ACKs: ").append(dd.getPendingAckMessageIds()).append("\n");

            DeviceState state = stateRepository.get(dd.deviceId);
            if (state != null) {
                sb.append("  [Repo] Sent: ").append(state.getSentMessageIds()).append("\n");
                sb.append("  [Repo] Recv: ").append(state.getRecvMessageIds()).append("\n");
            }
        }
        return sb.toString();
    }

    // ==================== DEVICE NAME MARKER ====================

    private void setDeviceNameWithMarker() {
        try {
            String markedName = P2pConfig.APP_MARKER + shortDeviceId + " " + Build.MODEL;
            if (markedName.length() > 32) markedName = markedName.substring(0, 32);

            java.lang.reflect.Method setDeviceName = manager.getClass().getMethod(
                    "setDeviceName", WifiP2pManager.Channel.class, String.class, WifiP2pManager.ActionListener.class);

            String finalName = markedName;
            setDeviceName.invoke(manager, channel, markedName, new WifiP2pManager.ActionListener() {
                @Override public void onSuccess() { log.success("Device name set: " + finalName); }
                @Override public void onFailure(int reason) { log.w("Failed to set device name"); }
            });
        } catch (Exception e) {
            log.w("setDeviceName not available");
        }
    }

    // ==================== INITIALIZATION ====================

//    private void reconnect() {
//        log.w("Reconnecting...");
//        stop();
//        start(listener);
//    }

    /**
     * Мягкое переподключение без потери кэша устройств
     */
    private void softReconnect() {
        log.divider("SOFT RECONNECT");
        discoveryRestartCount++;
        logDiagnosticState("RECONNECTING");

        // Сохраняем текущие данные
        Map<String, DiscoveredDevice> savedCache = new HashMap<>(deviceCache);
        int savedCacheSize = savedCache.size();

        // Переинициализируем канал
        channel = manager.initialize(context, Looper.getMainLooper(), () -> {
            log.w("Channel disconnected again!");
            if (isRunning) {
                handler.postDelayed(this::softReconnect, 2000);
            }
        });

        // Восстанавливаем кэш
        deviceCache.clear();
        deviceCache.putAll(savedCache);

        log.i("Soft reconnect: restored " + deviceCache.size() + "/" + savedCacheSize + " devices");

        // Перезапускаем discovery
        initializeAndStart();
    }

    private void initializeAndStart() {
        manager.removeGroup(channel, new WifiP2pManager.ActionListener() {
            @Override public void onSuccess() { clearAndSetup(); }
            @Override public void onFailure(int r) { clearAndSetup(); }
        });
    }

    private void clearAndSetup() {
        manager.clearLocalServices(channel, new WifiP2pManager.ActionListener() {
            @Override public void onSuccess() { clearRequestsAndSetup(); }
            @Override public void onFailure(int r) { clearRequestsAndSetup(); }
        });
    }

    private void clearRequestsAndSetup() {
        manager.clearServiceRequests(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                serviceRequests.clear();
                setupDiscovery();
            }
            @Override
            public void onFailure(int r) {
                serviceRequests.clear();
                setupDiscovery();
            }
        });
    }

    private void setupDiscovery() {
        log.divider("SETUP DISCOVERY");
        setupServiceListeners();
        registerMainService(() -> {
            addServiceRequests(() -> {
                startDiscoverySequence();
            });
        });
    }

    private void startDiscoverySequence() {
        schedulePeriodicTasks();
        requestDiscovery("start");
    }

    // ==================== SOCKET TRANSPORT (ЭТАП 4) ====================

    /**
     * Поднять сокетный транспорт, если он включён в конфигурации.
     */
    private void startSocketTransport() {
        if (!P2pConfig.ENABLE_P2P_SOCKET_TRANSPORT) {
            log.i("Socket transport disabled (P2pConfig.ENABLE_P2P_SOCKET_TRANSPORT=false)");
            return;
        }

        socketTransport = new P2pSocketTransport(shortDeviceId, sessionId);
        socketTransport.setCallback(new P2pSocketTransport.Callback() {
            @Override
            public void onPeerConnected(String deviceId) {
                log.success("SOCKET peer connected: " + deviceId);
                notifyStatus("Socket peer: " + deviceId);
            }

            @Override
            public void onPeerDisconnected(String deviceId) {
                log.w("SOCKET peer disconnected: " + deviceId);
                notifyDeviceUpdatedLater(deviceId);
            }

            @Override
            public void onMessageReceived(String senderDeviceId, String msgId, String text) {
                handleSocketMessage(senderDeviceId, msgId, text, "tcp");
            }

            @Override
            public void onAckReceived(String senderDeviceId, String msgId) {
                handleSocketAck(senderDeviceId, msgId);
            }

            @Override
            public void onDelivered(String peerDeviceId, String msgId, long rttMs) {
                // Быстрый канал подтвердил доставку — TXT-дубль не нужен (B1)
                cancelTxtFallback(msgId);
                corr.event(CorrelationLog.EV_MSG_TX, msgId, peerDeviceId,
                        "channel", "tcp", "state", "delivered", "rtt_ms", String.valueOf(rttMs));
            }

            @Override
            public void onDeliveryFailed(String peerDeviceId, String msgId) {
                // Все быстрые попытки исчерпаны — гарантированно уходим в TXT
                log.w("Fast delivery failed for " + msgId + " → TXT fallback");
                Runnable fallback = pendingTxtFallbacks.get(msgId);
                if (fallback != null) {
                    handler.removeCallbacks(fallback);
                    handler.post(fallback);
                }
            }

            @Override
            public void onRttSample(String peerDeviceId, long rttMs) {
                linkQuality.onRttSample(peerDeviceId, rttMs);
            }

            @Override
            public void onPeerSeen(String deviceId, String address) {
                DiscoveredDevice dd = findDeviceByDeviceId(deviceId);
                if (dd == null) return;
                dd.lastSeen = System.currentTimeMillis();
                dd.hasOurApp = true;
            }
        });
        socketTransport.start();
        log.success("Socket transport started on port " + P2pConfig.SOCKET_PORT
                + " | primary=" + P2pConfig.SOCKET_IS_PRIMARY_PATH
                + " | retry ladder=" + java.util.Arrays.toString(P2pConfig.RETRY_LADDER_MS));
    }

    private void stopSocketTransport() {
        if (socketTransport != null) {
            socketTransport.stop();
            socketTransport = null;
        }
        groupFormed = false;
        groupOwner = false;
    }

    /**
     * Приём сообщения по сокету. Логика та же, что для TXT-слота
     * (дедупликация по msgId, история, ACK), но без ограничения длины.
     */
    private void handleSocketMessage(String senderDeviceId, String msgId, String text) {
        handleSocketMessage(senderDeviceId, msgId, text, "tcp");
    }

    private void handleSocketMessage(String senderDeviceId, String msgId, String text, String channel) {
        if (msgId == null || senderDeviceId == null) return;

        DiscoveredDevice dd = findDeviceByDeviceId(senderDeviceId);
        if (dd == null) {
            dd = createSocketOnlyDevice(senderDeviceId);
            corr.event(CorrelationLog.EV_PEER_FOUND, null, senderDeviceId,
                    "channel", channel);
        }

        dd.lastSeen = System.currentTimeMillis();
        dd.hasOurApp = true;

        if (!processedMessageIds.add(msgId)) {
            // Уже принято другим каналом — фиксируем дубликат, чтобы при
            // разборе логов было видно, какой канал пришёл первым.
            corr.event(CorrelationLog.EV_MSG_DUP, msgId, senderDeviceId, "channel", channel);
            return;
        }
        trimSet(processedMessageIds, P2pConfig.MAX_PROCESSED_IDS);

        dd.addReceivedMessage(msgId, text);

        DeviceState state = stateRepository.getOrCreate(senderDeviceId);
        state.addRecvMessage(msgId, text);
        state.name = dd.name;
        state.address = dd.address;
        if (dd.sessionId != null) {
            state.lastSessionId = dd.sessionId;
        }
        stateRepository.save();

        log.success("MESSAGE VIA " + channel.toUpperCase() + ": " + msgId + " from " + senderDeviceId);
        log.i("Content: " + text);

        corr.event(CorrelationLog.EV_MSG_RX, msgId, senderDeviceId,
                "channel", channel,
                "len", String.valueOf(text != null ? text.length() : 0),
                "rtt_ms", String.valueOf(linkQuality.srtt(senderDeviceId)));

        if (listener != null) {
            // Локальная копия: dd переприсваивалась выше, в лямбду такую
            // переменную захватить нельзя.
            final DiscoveredDevice device = dd;
            final String body = text;
            handler.post(() -> listener.onMessageReceived(device, msgId, body));
        }

        // B5: подтверждаем сразу и по всем доступным каналам. Дубликаты ACK
        // безопасны — они отсекаются на стороне отправителя.
        boolean ackSent = false;
        if (groupLink != null && groupLink.knows(senderDeviceId)) {
            ackSent = groupLink.send(senderDeviceId,
                    Protocol.ack(shortDeviceId, msgId, System.currentTimeMillis()));
        }
        if (socketTransport != null) {
            ackSent |= socketTransport.sendAck(senderDeviceId, msgId);
            socketTransport.queueAck(msgId);   // «попутно» — в ближайшем кадре
        }

        corr.event(CorrelationLog.EV_ACK_TX, msgId, senderDeviceId,
                "channel", ackSent ? "fast" : "txt-fallback",
                "sent", ackSent ? "1" : "0");

        if (!ackSent) {
            log.d("Fast ACK for " + msgId + " not sent — TXT path stays active");
        }

        notifyDeviceUpdated(dd);
    }

    /**
     * Приём ACK по сокету.
     */
    private void handleSocketAck(String senderDeviceId, String msgId) {
        DiscoveredDevice dd = findDeviceByDeviceId(senderDeviceId);
        if (dd == null) {
            dd = createSocketOnlyDevice(senderDeviceId);
        }
        processReceivedAcks(msgId, dd);
    }

    /**
     * Найти устройство по его deviceId (а не по MAC).
     */
    private DiscoveredDevice findDeviceByDeviceId(String deviceId) {
        if (deviceId == null) return null;

        String address = deviceIdToAddress.get(deviceId);
        if (address != null) {
            DiscoveredDevice dd = deviceCache.get(address);
            if (dd != null) return dd;
        }

        for (DiscoveredDevice dd : deviceCache.values()) {
            if (deviceId.equals(dd.deviceId)) return dd;
        }
        return null;
    }

    /**
     * Создать «сокетное» устройство, если TXT-обнаружение его ещё не увидело.
     */
    private DiscoveredDevice createSocketOnlyDevice(String deviceId) {
        DiscoveredDevice dd = new DiscoveredDevice();
        dd.deviceId = deviceId;
        dd.address = "p2p:" + deviceId;
        dd.name = "Socket " + deviceId;
        dd.hasOurApp = true;
        dd.firstSeen = System.currentTimeMillis();
        dd.lastSeen = System.currentTimeMillis();
        dd.seenCount = 1;

        deviceCache.put(dd.address, dd);
        deviceIdToAddress.put(deviceId, dd.address);
        notifyDeviceFound(dd);
        log.i("Created socket-only device entry: " + deviceId);
        return dd;
    }

    private void notifyDeviceUpdatedLater(String deviceId) {
        DiscoveredDevice dd = findDeviceByDeviceId(deviceId);
        if (dd != null) notifyDeviceUpdated(dd);
    }

    /**
     * Попытка сформировать P2P-группу для сокетного транспорта.
     *
     * Инициирует только устройство с лексикографически меньшим deviceId —
     * иначе обе стороны одновременно вызывают connect() и получают ERROR.
     */
    private void maybeInitiateGroup() {
        if (socketTransport == null || !P2pConfig.ENABLE_P2P_SOCKET_TRANSPORT) return;
        if (groupFormed || socketTransport.getConnectedCount() > 0) return;

        long now = System.currentTimeMillis();
        if (now - lastGroupAttempt < P2pConfig.GROUP_RETRY_INTERVAL_MS) return;

        DiscoveredDevice best = null;
        for (DiscoveredDevice dd : deviceCache.values()) {
            if (!dd.hasOurApp || dd.deviceId == null || dd.deviceId.length() < 8) continue;
            if (!dd.isOnline()) continue;
            if (dd.address == null || dd.address.startsWith("p2p:")) continue;
            if (best == null || dd.deviceId.compareTo(best.deviceId) < 0) {
                best = dd;
            }
        }
        if (best == null) return;

        // Тай-брейк: инициирует только «меньший» ID
        if (shortDeviceId.compareTo(best.deviceId) > 0) {
            return;
        }

        lastGroupAttempt = now;

        // best переприсваивается в цикле выше, поэтому в анонимный
        // ActionListener его захватить нельзя («must be final or effectively
        // final»). Копируем в final-переменную: дальше используется только она.
        final DiscoveredDevice peer = best;

        log.i("Initiating P2P group with " + peer.getShortId() + " (" + peer.address + ")");

        WifiP2pConfig config = buildGroupConfig(peer);

        corr.event(CorrelationLog.EV_GRP_UP, null, peer.deviceId,
                "state", "connecting",
                "persistent", String.valueOf(P2pConfig.ENABLE_PERSISTENT_GROUP && HwGuard.supportsPersistentGroup()),
                "band", String.valueOf(P2pConfig.PREFERRED_GROUP_BAND),
                "addr", String.valueOf(peer.address));

        manager.connect(channel, config, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                log.success("P2P connect initiated to " + peer.getShortId());
            }

            @Override
            public void onFailure(int reason) {
                log.w("P2P connect failed: " + reasonToString(reason));
                corr.event(CorrelationLog.EV_GRP_DOWN, null, peer.deviceId,
                        "state", "connect-failed", "reason", reasonToString(reason));
            }
        });
    }

    /**
     * Конфигурация группы (A5/A6/D5).
     *
     * <p>С API 29 доступен {@code WifiP2pConfig.Builder}: явные SSID и пароль
     * означают, что повторное соединение с уже знакомым телефоном проходит
     * <b>без WPS-негоциации</b> (она и занимает основные 3–9 секунд), а
     * {@code enablePersistentMode(true)} сохраняет группу между сессиями.
     *
     * <p>Имя сети строим из двух коротких ID в отсортированном порядке: обе
     * стороны независимо получают одно и то же имя, поэтому «знакомый»
     * телефон узнаётся с любой стороны.
     */
    private WifiP2pConfig buildGroupConfig(DiscoveredDevice peer) {
        if (HwGuard.supportsPersistentGroup()) {
            try {
                // ВАЖНО: setDeviceAddress принимает android.net.MacAddress,
                // а не строку. Раньше сюда передавался peer.address (String) —
                // это ошибка компиляции.
                WifiP2pConfig.Builder builder = new WifiP2pConfig.Builder()
                        .setNetworkName(groupNetworkName(peer))
                        .setPassphrase(P2pConfig.PERSISTENT_GROUP_PASSPHRASE);

                if (isValidMac(peer.address)) {
                    try {
                        builder.setDeviceAddress(android.net.MacAddress.fromString(peer.address));
                    } catch (IllegalArgumentException badMac) {
                        log.w("Bad peer MAC, joining by SSID only: " + peer.address);
                    }
                }
                if (HwGuard.supportsBandSelection()) {
                    builder.setGroupOperatingBand(mapOperatingBand(P2pConfig.PREFERRED_GROUP_BAND));
                }
                if (P2pConfig.ENABLE_PERSISTENT_GROUP) {
                    builder.enablePersistentMode(true);
                }

                WifiP2pConfig config = builder.build();
                config.groupOwnerIntent = 15;   // хотим быть владельцем группы
                log.i("Group config: ssid=" + groupNetworkName(peer)
                        + " persistent=" + P2pConfig.ENABLE_PERSISTENT_GROUP
                        + " band=" + P2pConfig.PREFERRED_GROUP_BAND);
                return config;
            } catch (Exception e) {
                // Прошивка может не поддержать Builder/пароль — падаем на PBC
                log.w("Builder config failed (" + e.getMessage() + "), fallback to PBC");
            }
        }

        WifiP2pConfig config = new WifiP2pConfig();
        config.deviceAddress = peer.address;
        config.wps.setup = WpsInfo.PBC;
        config.groupOwnerIntent = 15;
        return config;
    }

    /**
     * Похоже ли значение на настоящий MAC-адрес.
     *
     * <p>На Android 10+ настоящий P2P-адрес собеседника в списке пиров есть,
     * но записи, созданные «по сокету» ({@code p2p:<id>}), MAC не содержат —
     * для них {@code MacAddress.fromString} бросит исключение.
     */
    private static boolean isValidMac(String address) {
        if (address == null) return false;
        return address.matches("^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$");
    }

    /** Одинаковое имя сети на обоих телефонах: {@code DIRECT-RD-<меньший>-<больший>}. */
    private String groupNetworkName(DiscoveredDevice peer) {
        String other = peer.deviceId != null
                ? CorrelationLog.shortId(peer.deviceId) : "unknown";
        String a = shortDeviceId.compareTo(other) <= 0 ? shortDeviceId : other;
        String b = shortDeviceId.compareTo(other) <= 0 ? other : shortDeviceId;
        String name = P2pConfig.PERSISTENT_GROUP_PREFIX + a + "-" + b;
        return name.length() > 32 ? name.substring(0, 32) : name;
    }

    /** Значения P2pConfig → константы {@code WifiP2pConfig}. */
    private static int mapOperatingBand(int band) {
        switch (band) {
            case P2pConfig.GROUP_BAND_24:
                return WifiP2pConfig.GROUP_OWNER_BAND_2GHZ;
            case P2pConfig.GROUP_BAND_5:
                return WifiP2pConfig.GROUP_OWNER_BAND_5GHZ;
            default:
                return WifiP2pConfig.GROUP_OWNER_BAND_AUTO;
        }
    }

    /**
     * Обработать изменение состояния P2P-соединения для сокетного транспорта.
     */
    private void handleGroupConnection() {
        manager.requestConnectionInfo(channel, (WifiP2pInfo info) -> {
            if (info == null) {
                groupFormed = false;
                log.d("Connection info: null");
                return;
            }

            groupFormed = info.groupFormed;
            groupOwner = info.isGroupOwner;

            log.i("Connection info: formed=" + info.groupFormed
                    + " isGO=" + info.isGroupOwner
                    + " owner=" + (info.groupOwnerAddress != null
                    ? info.groupOwnerAddress.getHostAddress() : "null"));

            if (socketTransport != null) {
                socketTransport.onGroupInfo(
                        info.groupFormed, info.isGroupOwner, info.groupOwnerAddress);
            }

            if (info.groupFormed) {
                // B2/B3: внутри группы уже есть IP-сеть — поднимаем UDP-канал,
                // который не зависит от supplicant'а и даёт задержку в единицы мс.
                startGroupLink(info);
                corr.event(CorrelationLog.EV_GRP_UP, null, myPeerId(),
                        "role", info.isGroupOwner ? "GO" : "client",
                        "owner", info.groupOwnerAddress != null
                                ? info.groupOwnerAddress.getHostAddress() : "-");
            } else {
                stopGroupLink();
                corr.event(CorrelationLog.EV_GRP_DOWN, null, null, "state", "lost");

                // A7: группа распалась — не ждём полминуты. Постоянная группа
                // пересоздаётся примерно за секунду, поэтому пробуем скоро.
                long delay = P2pConfig.ENABLE_PERSISTENT_GROUP && HwGuard.supportsPersistentGroup()
                        ? 2_000
                        : P2pConfig.GROUP_RETRY_INTERVAL_MS;
                handler.postDelayed(FastDiscoveryManager.this::maybeInitiateGroup, delay);
            }
        });
    }

    // ==================== КАНАЛ ВНУТРИ ГРУППЫ (B2/B3) ====================

    /**
     * Поднять UDP-канал в подсети группы.
     *
     * <p>Владелец группы слушает фиксированный порт и вещает presence-маячок,
     * клиент шлёт свои presence-пакеты на адрес владельца. Дальше сообщения
     * идут датаграммами — без обнаружения, без DNS-SD, без TCP-сессии.
     */
    private void startGroupLink(WifiP2pInfo info) {
        if (!P2pConfig.ENABLE_GROUP_LINK_CHANNEL) return;

        if (groupLink == null) {
            groupLink = new GroupLinkChannel(shortDeviceId, sessionId);
            groupLink.setCallback(new GroupLinkChannel.Callback() {
                @Override
                public void onFrame(String senderDeviceId, BinaryFrame frame, String fromAddress) {
                    handleFastFrame(senderDeviceId, frame, "udp-group");
                }

                @Override
                public void onPeerSeen(String deviceId, String address) {
                    DiscoveredDevice dd = findDeviceByDeviceId(deviceId);
                    if (dd == null) {
                        dd = createSocketOnlyDevice(deviceId);
                        corr.event(CorrelationLog.EV_PEER_FOUND, null, deviceId,
                                "channel", "udp-presence", "addr", address);
                    }
                    dd.lastSeen = System.currentTimeMillis();
                    dd.hasOurApp = true;
                    notifyDeviceUpdated(dd);
                }

                @Override
                public void onRtt(String peerDeviceId, long rttMs) {
                    linkQuality.onRttSample(peerDeviceId, rttMs);
                    corr.event(CorrelationLog.EV_RTT, null, peerDeviceId,
                            "channel", "udp-group", "rtt_ms", String.valueOf(rttMs));
                }

                @Override
                public void onLinkState(boolean up, String detail) {
                    log.i("Group link " + (up ? "up" : "down") + ": " + detail);
                }
            });
        }

        java.net.InetAddress local = null;
        if (info.isGroupOwner) {
            try {
                local = java.net.InetAddress.getByName("192.168.49.1");   // адрес GO по умолчанию
            } catch (Exception ignored) {
            }
        }

        groupLink.start(info.isGroupOwner, info.groupOwnerAddress, local);
    }

    private void stopGroupLink() {
        if (groupLink != null) {
            groupLink.stop();
            groupLink = null;
        }
    }

    /**
     * Единая обработка кадров быстрых каналов (UDP внутри группы и TCP).
     * Здесь же ретрансляция (C6): кадр с {@code ttl > 0} пересылается дальше
     * один раз — так досягаемость растёт без Bluetooth.
     */
    private void handleFastFrame(String senderDeviceId, BinaryFrame frame, String channel) {
        switch (frame.type) {
            case BinaryFrame.T_MSG: {
                Protocol.Msg msg = Protocol.parseMessage(frame);
                if (msg.deviceId == null || shortDeviceId.equals(msg.deviceId)) return;

                // Оценка расхождения часов из метки отправителя
                peerClock.observe(msg.deviceId, msg.utcMs, System.currentTimeMillis(),
                        linkQuality.srtt(senderDeviceId));

                handleSocketMessage(senderDeviceId, msg.msgId, msg.text, channel);
                relayIfAllowed(frame, msg, senderDeviceId);
                break;
            }
            case BinaryFrame.T_ACK: {
                Protocol.Ack ack = Protocol.parseAck(frame);
                if (ack.deviceId == null || shortDeviceId.equals(ack.deviceId)) return;
                handleSocketAck(senderDeviceId, ack.msgId);
                break;
            }
            case BinaryFrame.T_SYNC: {
                // Диагностический кадр сверки — просто фиксируем факт
                corr.event(CorrelationLog.EV_SYNC, null, senderDeviceId,
                        "channel", channel, "state", "frame");
                break;
            }
            default:
                break;
        }
    }

    /**
     * Ретрансляция кадра дальше (C6).
     *
     * Условия: ретрансляция включена, в кадре остался TTL, кадр ещё не
     * пересылали (защита от лавины). Повторная доставка получателю безопасна:
     * он отбросит дубликат по {@code msgId}.
     */
    private void relayIfAllowed(BinaryFrame frame, Protocol.Msg msg, String senderDeviceId) {
        if (!P2pConfig.ENABLE_RELAY || msg.ttl <= 0) return;
        if (!relayCache.markIfNew(msg.msgId + "@" + senderDeviceId)) return;

        BinaryFrame relayed = Protocol.message(shortDeviceId, sessionId, msg.msgId,
                msg.text, "", msg.utcMs, msg.ttl - 1);

        boolean sent = false;
        if (groupLink != null && GroupLinkChannel.fitsDatagram(relayed)) {
            groupLink.broadcast(relayed);
            sent = true;
        }
        if (socketTransport != null) {
            socketTransport.sendFrame(null, relayed);
            sent = true;
        }

        corr.event(CorrelationLog.EV_RELAY, msg.msgId, senderDeviceId,
                "ttl", String.valueOf(msg.ttl - 1), "sent", sent ? "1" : "0");
    }

    // ==================== MAIN SERVICE ====================

    private void registerMainService(Runnable onComplete) {
        registerMainService(onComplete, 0);
    }

    private void registerMainService(Runnable onComplete, int attempt) {
        Map<String, String> record = buildMainServiceRecord();

        log.divider("REGISTER MAIN SERVICE");
        log.i("Record: " + record.toString());
        fitTxtRecord(record, "Main(register)");

        WifiP2pDnsSdServiceInfo info = WifiP2pDnsSdServiceInfo.newInstance(
                P2pConfig.MAIN_SERVICE_NAME, P2pConfig.SERVICE_TYPE, record);

        manager.addLocalService(channel, info, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                // Указатель выставляем только для реально зарегистрированного сервиса
                mainServiceInfo = info;
                confirmAcksSent(record.get("ack"));
                log.success("MAIN SERVICE REGISTERED: " + shortDeviceId);
                onComplete.run();
            }
            @Override
            public void onFailure(int reason) {
                log.error("FAILED to register main service: " + reasonToString(reason)
                        + " (attempt " + attempt + ")");
                long delay = P2pConfig.DISCOVER_RETRY_BASE_MS << Math.min(attempt, 3);
                if (attempt < 5) {
                    handler.postDelayed(() -> registerMainService(onComplete, attempt + 1), delay);
                } else {
                    log.error("Main service registration abandoned after " + attempt + " attempts");
                }
            }
        });
    }

    private Map<String, String> buildMainServiceRecord() {
        Map<String, String> record = new HashMap<>();
        record.put("id", shortDeviceId);
        record.put("t", String.valueOf(System.currentTimeMillis() / 1000));
        record.put("hb", String.valueOf(heartbeatSeq.get()));
        record.put("v", P2pConfig.PROTOCOL_VERSION);
        record.put("sid", sessionId);

        String acks = collectAckString();
        if (!acks.isEmpty()) {
            record.put("ack", acks);
        }

        return record;
    }

    /**
     * Собрать список ACK для публикации.
     *
     * ВАЖНО: метод НЕ меняет состояние. Раньше он сразу помечал ACK
     * отправленными, хотя запись могла не попасть в эфир (переполнение TXT,
     * ERROR от addLocalService) — счётчик ackSendCount рос, а доставки не было.
     * Подтверждение отправки делает confirmAcksSent() только после onSuccess.
     *
     * Приоритет у тех ACK, что ждут дольше всего; лимит — MAX_ACKS_PER_RECORD.
     */
    private String collectAckString() {
        List<String> candidates = new ArrayList<>();
        List<String> drop = new ArrayList<>();
        synchronized (pendingAcksToSend) {
            for (String msgId : pendingAcksToSend) {
                Integer attempts = ackPublishAttempts.get(msgId);
                if (attempts != null && attempts >= MAX_ACK_ATTEMPTS) {
                    drop.add(msgId);
                    continue;
                }
                // Владелец мог исчезнуть из кэша (устройство ушло) — иначе
                // «мёртвые» ID занимают лимит MAX_ACKS_PER_RECORD в каждой
                // записи и новые ACK в эфир не попадают.
                if (findReceivedMessageOwner(msgId) == null) {
                    drop.add(msgId);
                    continue;
                }
                candidates.add(msgId);
            }
        }
        for (String msgId : drop) {
            pendingAcksToSend.remove(msgId);
            ackPublishAttempts.remove(msgId);
            log.d("ACK dropped from queue (exhausted or device gone): " + msgId);
        }
        if (candidates.isEmpty()) return "";

        final long now = System.currentTimeMillis();
        candidates.sort((a, b) -> Long.compare(
                activeIncomingMessages.getOrDefault(a, now),
                activeIncomingMessages.getOrDefault(b, now)));

        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (String msgId : candidates) {
            if (count >= P2pConfig.MAX_ACKS_PER_RECORD) break;
            if (sb.length() > 0) sb.append(",");
            sb.append(msgId);
            count++;
        }
        return sb.toString();
    }

    /**
     * Отметить ACK как реально опубликованные (вызывается только из onSuccess).
     */
    private void confirmAcksSent(String acks) {
        if (acks == null || acks.isEmpty()) return;

        for (String part : acks.split(",")) {
            String msgId = part.trim();
            if (msgId.isEmpty()) continue;

            // Не подтверждаем «свежие» ACK: если запись с ack не ушла в эфир,
            // мы не должны терять её из очереди.
            DiscoveredDevice owner = findReceivedMessageOwner(msgId);
            if (owner == null) continue;

            owner.markReceivedMessageAckSent(msgId);
            int attempts = ackPublishAttempts.merge(msgId, 1, Integer::sum);
            log.d("ACK published #" + attempts + ": " + msgId + " -> " + owner.getShortId());
        }
    }

    /**
     * Найти устройство, от которого получено сообщение с указанным ID.
     */
    private DiscoveredDevice findReceivedMessageOwner(String msgId) {
        for (DiscoveredDevice dd : deviceCache.values()) {
            for (DiscoveredDevice.ReceivedMessage rm : dd.receivedMessages) {
                if (msgId.equals(rm.messageId)) return dd;
            }
        }
        return null;
    }

    private void updateMainService() {
        if (!isRunning) return;

        // Если сервис не зарегистрирован (например, все попытки провалились),
        // heartbeat должен восстановить его, а не молча простаивать.
        if (mainServiceInfo == null) {
            log.w("Main service missing on heartbeat, re-registering");
            registerMainService(() -> requestDiscovery("heartbeat-reregister"));
            return;
        }

        // Предыдущая публикация ещё в полёте — не наслаиваем remove/add,
        // иначе supplicant отвечает BUSY и станция «пропадает» из эфира.
        if (mainServicePublishInFlight) {
            long stuckMs = System.currentTimeMillis() - mainServicePublishSince;
            if (stuckMs < 15_000) {
                log.d("Main service publish in flight, skipping heartbeat update");
                return;
            }
            // Колбэк потерялся (бывает при перезапуске supplicant) — снимаем блокировку
            log.w("Main service publish stuck " + stuckMs + "ms, resetting flag");
            mainServicePublishInFlight = false;
        }

        heartbeatSeq.incrementAndGet();
        lastHeartbeatSentTime = System.currentTimeMillis();

        publishMainService(0);
    }

    /**
     * Публикация WFD_Main с повторами.
     *
     * Ключевое отличие от прежней реализации: mainServiceInfo переключается на
     * новую запись ТОЛЬКО после успешного addLocalService. Иначе указатель
     * указывал на незарегистрированный сервис, следующий removeLocalService
     * не находил его — и станция исчезала из эфира навсегда, продолжая
     * считать heartbeat.
     */
    private void publishMainService(int attempt) {
        if (!isRunning) return;

        Map<String, String> record = buildMainServiceRecord();
        String acks = record.get("ack");
        if (acks != null && !acks.isEmpty()) {
            log.success("Including ACKs in HB: " + acks);
        }
        fitTxtRecord(record, "Main");

        WifiP2pDnsSdServiceInfo newInfo = WifiP2pDnsSdServiceInfo.newInstance(
                P2pConfig.MAIN_SERVICE_NAME, P2pConfig.SERVICE_TYPE, record);
        WifiP2pDnsSdServiceInfo oldInfo = mainServiceInfo;

        mainServicePublishInFlight = true;
        mainServicePublishSince = System.currentTimeMillis();

        WifiP2pManager.ActionListener addListener = new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                mainServicePublishInFlight = false;
                mainServiceInfo = newInfo;
                confirmAcksSent(record.get("ack"));
                log.d("Main service updated OK, hb=" + heartbeatSeq.get());
            }

            @Override
            public void onFailure(int r) {
                mainServicePublishInFlight = false;
                log.w("Main service add failed: " + reasonToString(r) + " (attempt " + attempt + ")");

                // Возвращаем указатель на старую (возможно всё ещё валидную) запись
                mainServiceInfo = oldInfo;

                if (attempt < P2pConfig.DISCOVER_MAX_RETRIES) {
                    long delay = P2pConfig.DISCOVER_RETRY_BASE_MS << attempt;
                    log.d("Main service retry in " + delay + "ms");
                    handler.postDelayed(() -> publishMainService(attempt + 1), delay);
                } else {
                    log.error("Main service publish failed permanently — re-registering from scratch");
                    handler.postDelayed(() -> registerMainService(() -> {}), 1000);
                }
            }
        };

        if (oldInfo == null) {
            manager.addLocalService(channel, newInfo, addListener);
            return;
        }

        manager.removeLocalService(channel, oldInfo, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                manager.addLocalService(channel, newInfo, addListener);
            }

            @Override
            public void onFailure(int r) {
                // Старую запись снять не удалось — возможно, её уже нет.
                // Пробуем добавить новую, не теряя указатель на старую до успеха.
                log.d("Main service remove failed: " + reasonToString(r) + ", adding anyway");
                manager.addLocalService(channel, newInfo, addListener);
            }
        });
    }

    // ==================== ACK SERVICE ====================

    private void updateAckService() {
        if (!isRunning) return;

        // D7: пока быстрый канал жив, ACK уходит по нему — отдельный DNS-SD
        // сервис не нужен. Меньше сервисов → меньше нагрузки на supplicant,
        // меньше шансов получить BUSY и «потерять» запись.
        if (P2pConfig.SOCKET_IS_PRIMARY_PATH && fastPathAvailable()) {
            if (ackServiceInfo != null) {
                manager.removeLocalService(channel, ackServiceInfo, null);
                ackServiceInfo = null;
                log.d("ACK service removed: fast path is up (D7)");
            }
            return;
        }

        Set<String> allAcks = new HashSet<>();
        for (DiscoveredDevice dd : deviceCache.values()) {
            allAcks.addAll(dd.getPendingAckMessageIds());
        }

        if (allAcks.isEmpty()) {
            if (ackServiceInfo != null) {
                manager.removeLocalService(channel, ackServiceInfo, null);
                ackServiceInfo = null;
                log.d("ACK service removed (no pending)");
            }
            return;
        }

        Map<String, String> record = new HashMap<>();
        record.put("id", shortDeviceId);
        record.put("t", String.valueOf(System.currentTimeMillis() / 1000));
        record.put("sid", sessionId);

        StringBuilder ackSb = new StringBuilder();
        int count = 0;
        for (String ack : allAcks) {
            if (count >= P2pConfig.MAX_ACKS_PER_RECORD) break;
            if (ackSb.length() > 0) ackSb.append(",");
            ackSb.append(ack);
            count++;
        }
        record.put("ack", ackSb.toString());

        log.success("ACK Service update: " + ackSb.toString());
        fitTxtRecord(record, "Ack");

        WifiP2pDnsSdServiceInfo newAckService = WifiP2pDnsSdServiceInfo.newInstance(
                P2pConfig.ACK_SERVICE_NAME, P2pConfig.SERVICE_TYPE, record);

        if (ackServiceInfo != null) {
            manager.removeLocalService(channel, ackServiceInfo, new WifiP2pManager.ActionListener() {
                @Override
                public void onSuccess() {
                    manager.addLocalService(channel, newAckService, new WifiP2pManager.ActionListener() {
                        @Override public void onSuccess() {
                            ackServiceInfo = newAckService;
                            log.d("ACK service updated OK");
                        }
                        @Override public void onFailure(int r) {
                            ackServiceInfo = newAckService;
                        }
                    });
                }
                @Override
                public void onFailure(int r) {
                    log.w("ACK service remove failed: " + reasonToString(r) + ", adding anyway");
                    manager.addLocalService(channel, newAckService, new WifiP2pManager.ActionListener() {
                        @Override public void onSuccess() {
                            ackServiceInfo = newAckService;
                            log.success("ACK service created: " + ackSb.toString());
                        }
                        @Override public void onFailure(int r2) {
                            log.w("Failed to create ACK service: " + reasonToString(r2));
                        }
                    });
                }
            });
        } else {
            manager.addLocalService(channel, newAckService, new WifiP2pManager.ActionListener() {
                @Override public void onSuccess() {
                    ackServiceInfo = newAckService;
                    log.success("ACK service created: " + ackSb.toString());
                }
                @Override public void onFailure(int r) {
                    log.w("Failed to create ACK service: " + reasonToString(r));
                }
            });
        }
    }

    // ==================== SYNC SERVICE ====================

    private void registerSyncService(String targetDeviceId, List<String> mySentIds, List<String> myRecvIds) {
        String shortTargetId = targetDeviceId.length() > 8
                ? targetDeviceId.substring(0, 8)
                : targetDeviceId;

        Map<String, String> record = new HashMap<>();
        record.put("id", shortDeviceId);
        record.put("to", shortTargetId);
        record.put("sent", SyncManager.formatIdList(mySentIds));
        record.put("recv", SyncManager.formatIdList(myRecvIds));
        record.put("t", String.valueOf(System.currentTimeMillis() / 1000));
        record.put("sid", sessionId);

        // C2: Bloom-фильтр всех известных ID (32 hex-символа вместо списка).
        // Позволяет собеседнику одной проверкой понять, есть ли у нас его
        // сообщение, не раздувая TXT-запись.
        BloomFilter bloom = BloomFilter.forMessageIds();
        for (String id : mySentIds) bloom.add(id);
        for (String id : myRecvIds) bloom.add(id);
        record.put("bf", bloom.toHex());

        corr.event(CorrelationLog.EV_SYNC, null, shortTargetId,
                "sent", String.valueOf(mySentIds.size()),
                "recv", String.valueOf(myRecvIds.size()),
                "bloom_bits", String.valueOf(bloom.setBitCount()));

        log.i("Publishing SYNC to " + shortTargetId +
                " | sent=" + mySentIds + " | recv=" + myRecvIds);
        fitTxtRecord(record, "Sync");

        WifiP2pDnsSdServiceInfo newSyncService = WifiP2pDnsSdServiceInfo.newInstance(
                P2pConfig.SYNC_SERVICE_NAME, P2pConfig.SERVICE_TYPE, record);

        if (syncServiceInfo != null) {
            manager.removeLocalService(channel, syncServiceInfo, new WifiP2pManager.ActionListener() {
                @Override
                public void onSuccess() {
                    addSyncService(newSyncService);
                }
                @Override
                public void onFailure(int r) {
                    addSyncService(newSyncService);
                }
            });
        } else {
            addSyncService(newSyncService);
        }
    }

    private void addSyncService(WifiP2pDnsSdServiceInfo service) {
        manager.addLocalService(channel, service, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                syncServiceInfo = service;
                log.success("SYNC service registered");

                handler.postDelayed(() -> {
                    if (syncServiceInfo == service) {
                        manager.removeLocalService(channel, syncServiceInfo, null);
                        syncServiceInfo = null;
                        log.d("SYNC service removed (timeout)");
                    }
                }, P2pConfig.SYNC_SERVICE_LIFETIME);
            }
            @Override
            public void onFailure(int r) {
                log.w("Failed to register SYNC service: " + reasonToString(r));
            }
        });
    }

    private void handleSyncServiceRecord(Map<String, String> record, WifiP2pDevice device) {
        String senderId = record.get("id");
        String targetId = record.get("to");
        String theirSent = record.get("sent");
        String theirRecv = record.get("recv");
        String sid = record.get("sid");

        if (senderId == null || shortDeviceId.equals(senderId)) return;

        if (targetId == null ||
                (!targetId.equalsIgnoreCase(shortDeviceId) &&
                        !shortDeviceId.startsWith(targetId))) {
            return;
        }

        log.i("SYNC received from " + senderId +
                " | theirSent=" + theirSent + " | theirRecv=" + theirRecv);

        // C2: Bloom-фильтр собеседника. «Нет» — точный ответ: сообщения у него
        // точно нет, его можно переотправить немедленно, не дожидаясь
        // следующего цикла сверки.
        String theirBloomHex = record.get("bf");
        BloomFilter theirBloom = theirBloomHex != null
                ? BloomFilter.fromHex(theirBloomHex, P2pConfig.BLOOM_BITS) : null;

        if (theirBloom != null) {
            int missing = 0;
            for (PendingMessage pm : pendingMessages.values()) {
                if (theirBloom.mightContain(pm.messageId)) continue;
                missing++;
                log.w("SYNC: peer has no " + pm.messageId + " — will resend");
                corr.event(CorrelationLog.EV_SYNC, pm.messageId, senderId,
                        "state", "missing-on-peer");
            }
            corr.event(CorrelationLog.EV_SYNC, null, senderId,
                    "bloom_recv", "1", "missing", String.valueOf(missing));
        }

        DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);

        if (!validateSessionId(senderId, sid, dd)) {
            dd.lastSeen = System.currentTimeMillis();
            return;
        }

        bindDeviceIdentity(dd, senderId);
        dd.hasOurApp = true;
        dd.lastSeen = System.currentTimeMillis();

        List<String> theirSentIds = SyncManager.parseIdList(theirSent);
        List<String> theirRecvIds = SyncManager.parseIdList(theirRecv);

        List<String> filteredTheirRecvIds = new ArrayList<>();
        for (String recvId : theirRecvIds) {
            if (recvId.startsWith(shortDeviceId + "_" + sessionId + "_")) {
                filteredTheirRecvIds.add(recvId);
            } else if (recvId.startsWith(shortDeviceId + "_")) {
                log.d("Ignoring old message in SYNC recv: " + recvId);
            }
        }

        // ДОБАВЛЕНО: Обработать theirRecvIds как ACK для UI
        for (String msgId : filteredTheirRecvIds) {
            processAckFromSync(msgId, dd);
        }

        syncManager.processIncomingSync(senderId, theirSentIds, filteredTheirRecvIds);
    }

    /**
     * Обработать ACK полученный через SYNC (а не через ACK service)
     */
    private void processAckFromSync(String msgId, DiscoveredDevice sender) {
        // Это вообще наше сообщение? Сессия НЕ проверяется: после перезапуска
        // приложения sessionId меняется, а SYNC переотправляет сообщения прошлых
        // сессий — их ACK раньше молча отбрасывался.
        if (!isOwnMessageId(msgId)) {
            return;
        }

        // Дедупликация
        if (processedAcks.contains(msgId)) {
            return;
        }

        PendingMessage pm = pendingMessages.get(msgId);
        if (pm == null) {
            // Сообщение уже не в полёте (обработано, истекло или из прошлой сессии)
            markDeliveredWithoutPending(msgId, sender, Collections.singletonList(msgId));
            return;
        }

        // Помечаем как обработанный
        processedAcks.add(msgId);
        trimSet(processedAcks, P2pConfig.MAX_PROCESSED_ACKS);

        log.success("ACK from SYNC for " + msgId + " from " + sender.getShortId());

        // Обновляем UI модель
        sender.markSentMessageAcked(msgId, Collections.singletonList(msgId));

        // Обновляем репозиторий
        if (sender.deviceId != null) {
            DeviceState state = stateRepository.get(sender.deviceId);
            if (state != null) {
                state.markAcked(msgId);
                stateRepository.save();
            }
        }

        // Очищаем pending
        pendingMessages.remove(msgId);
        releaseSlot(pm.slotIndex);

        // Уведомляем Activity
        if (listener != null) {
            handler.post(() -> listener.onAckReceived(sender, msgId));
        }
    }

    /**
     * Переотправка сообщения по результатам SYNC.
     *
     * Раньше здесь НЕ создавалась запись в pendingMessages, поэтому приходивший
     * ACK отбрасывался ("pm == null"), слот истекал, SYNC снова инициировал
     * переотправку — бесконечный цикл RESEND.
     */
    private void resendMessage(String targetDeviceId, String msgId, String text) {
        PendingMessage existing = pendingMessages.get(msgId);
        if (existing != null) {
            // Уже в полёте — не плодим дубли, просто освежаем запись в эфире
            log.d("RESEND skipped (already pending): " + msgId);
            republishSlot(messageSlots.get(existing.slotIndex));
            return;
        }

        int slot = findFreeSlot();
        if (slot < 0) slot = releaseOldestSlot();

        log.i("RESENDING " + msgId + " to " + targetDeviceId + " slot=" + slot);

        pendingMessages.put(msgId, new PendingMessage(msgId, text, targetDeviceId, slot));
        registerMessageSlot(slot, msgId, text, targetDeviceId);
    }

    /**
     * Переопубликовать запись слота (remove + add) — обновляет DNS-кэш
     * собеседника, если он «не увидел» сообщение с первого раза.
     */
    private void republishSlot(SlotInfo slot) {
        if (slot == null || slot.serviceInfo == null) return;

        final WifiP2pDnsSdServiceInfo info = slot.serviceInfo;

        WifiP2pManager.ActionListener addListener = new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                slot.isRegistered = true;
                log.d("Slot " + slot.slotIndex + " re-published: " + slot.messageId);
                requestDiscovery("slot-republished");
            }

            @Override
            public void onFailure(int r) {
                log.w("Slot " + slot.slotIndex + " re-publish failed: " + reasonToString(r));
            }
        };

        // Сначала снимаем запись: повторный addLocalService того же объекта
        // вернёт ERROR на большинстве прошивок.
        manager.removeLocalService(channel, info, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                manager.addLocalService(channel, info, addListener);
            }

            @Override
            public void onFailure(int r) {
                manager.addLocalService(channel, info, addListener);
            }
        });
    }

    // ==================== MESSAGE SLOTS ====================

    private int findFreeSlot() {
        for (int i = 0; i < P2pConfig.MAX_MSG_SLOTS; i++) {
            if (!messageSlots.containsKey(i) && !reservedSlots.contains(i)) return i;
        }
        return -1;
    }

    private int releaseOldestSlot() {
        int oldestSlot = 0;
        long oldestTime = Long.MAX_VALUE;
        for (Map.Entry<Integer, SlotInfo> entry : messageSlots.entrySet()) {
            if (entry.getValue().createdAt < oldestTime) {
                oldestTime = entry.getValue().createdAt;
                oldestSlot = entry.getKey();
            }
        }
        releaseSlot(oldestSlot);
        return oldestSlot;
    }

    private void releaseSlot(int slotIndex) {
        reservedSlots.remove(slotIndex);
        SlotInfo slot = messageSlots.remove(slotIndex);
        if (slot != null && slot.serviceInfo != null && slot.isRegistered) {
            manager.removeLocalService(channel, slot.serviceInfo, null);
            log.d("Slot " + slotIndex + " released: " + slot.messageId);
        }
        if (slot != null && slot.messageId != null) {
            pendingMessages.remove(slot.messageId);
        }
    }

    private void registerMessageSlot(int slotIndex, String msgId, String message, String targetDeviceId) {
        if (messageSlots.containsKey(slotIndex)) releaseSlot(slotIndex);

        Map<String, String> record = new HashMap<>();
        record.put("id", shortDeviceId);
        record.put("mid", msgId);
        record.put("msg", truncateMessage(message, P2pConfig.MAX_MESSAGE_LENGTH));
        record.put("s", String.valueOf(slotIndex));
        record.put("t", String.valueOf(System.currentTimeMillis() / 1000));
        record.put("sid", sessionId);

        if (targetDeviceId != null) {
            record.put("to", targetDeviceId.length() > 8 ? targetDeviceId.substring(0, 8) : targetDeviceId);
        }

        String acks = collectAckString();
        if (!acks.isEmpty()) {
            record.put("ack", acks);
        }

        fitTxtRecord(record, "Msg" + slotIndex);

        // ACK считаем отправленными только после успешной публикации записи
        final String acksToConfirm = record.get("ack");

        String serviceName = P2pConfig.MSG_SLOT_PREFIX + slotIndex;
        WifiP2pDnsSdServiceInfo slotService = WifiP2pDnsSdServiceInfo.newInstance(
                serviceName, P2pConfig.SERVICE_TYPE, record);

        SlotInfo slot = new SlotInfo(slotIndex);
        slot.serviceInfo = slotService;
        slot.messageId = msgId;
        slot.targetDeviceId = targetDeviceId;

        messageSlots.put(slotIndex, slot);

        manager.addLocalService(channel, slotService, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                slot.isRegistered = true;
                log.success("Message slot " + slotIndex + " registered: " + msgId);
                confirmAcksSent(acksToConfirm);
                requestDiscovery("slot-registered");
            }
            @Override
            public void onFailure(int r) {
                log.error("Failed to register slot " + slotIndex);
            }
        });

        // Первый таймаут: если ACK не пришёл — переопубликовать запись
        // (обновляет DNS-кэш собеседника), второй таймаут — освободить слот.
        scheduleSlotRenewal(slotIndex, msgId, 1);
    }

    private void scheduleSlotRenewal(int slotIndex, String msgId, int round) {
        handler.postDelayed(() -> {
            SlotInfo current = messageSlots.get(slotIndex);
            if (current == null || !msgId.equals(current.messageId)) return;

            if (isMessageAcked(msgId)) {
                releaseSlot(slotIndex);
                return;
            }

            if (round <= P2pConfig.SLOT_RENEW_ROUNDS) {
                log.w("Slot " + slotIndex + " renew #" + round + " - no ACK for " + msgId);
                republishSlot(current);
                scheduleSlotRenewal(slotIndex, msgId, round + 1);
            } else {
                log.w("Slot " + slotIndex + " released, still no ACK for " + msgId);
                releaseSlot(slotIndex);
                requestSyncForMessage(msgId);
            }
        }, P2pConfig.SLOT_RENEW_INTERVAL);
    }

    private boolean isMessageAcked(String msgId) {
        for (DiscoveredDevice dd : deviceCache.values()) {
            for (DiscoveredDevice.SentMessage sm : dd.sentMessages) {
                if (sm.messageId.equals(msgId)) return sm.acknowledged;
            }
        }
        return false;
    }

    /**
     * Слот освобождён без ACK — просим SYNC сверить состояние с собеседником,
     * не дожидаясь очередного минутного цикла.
     */
    private void requestSyncForMessage(String msgId) {
        String deviceId = findDeviceIdByMessageId(msgId);
        if (deviceId != null && syncManager != null) {
            syncManager.onDeviceBecameOnline(deviceId);
        }
    }

    private String truncateMessage(String msg, int maxLen) {
        if (msg == null) return "";
        return msg.length() <= maxLen ? msg : msg.substring(0, maxLen);
    }

    // ==================== TXT RECORD SIZE BUDGET ====================

    /**
     * Размер строки в UTF-8. Именно байты, а не символы, ограничивает DNS-SD
     * (кириллица — 2 байта на символ, эмодзи — 4).
     */
    private static int utf8Bytes(String s) {
        return s == null ? 0 : s.getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * Размер одной пары key=value в TXT-записи:
     * служебный байт длины + "key=" + value (+1 байт на сам value).
     */
    private static int txtEntrySize(String key, String value) {
        return 1 + utf8Bytes(key) + 1 + utf8Bytes(value);
    }

    /**
     * Полный размер TXT-записи, как её кодирует Android.
     */
    private static int txtRecordSize(Map<String, String> record) {
        if (record == null) return 0;
        int size = 0;
        for (Map.Entry<String, String> e : record.entrySet()) {
            size += txtEntrySize(e.getKey(), e.getValue());
        }
        return size;
    }

    /**
     * Обрезать строку до maxBytes в UTF-8, не разрывая суррогатные пары.
     */
    private String truncateUtf8(String s, int maxBytes) {
        if (s == null) return "";
        if (maxBytes <= 0) return "";
        if (utf8Bytes(s) <= maxBytes) return s;

        int end = s.length();
        while (end > 0 && utf8Bytes(s.substring(0, end)) > maxBytes) {
            end--;
            // не отрезаем половину суррогатной пары
            if (end > 0 && Character.isLowSurrogate(s.charAt(end))) {
                end--;
            }
        }
        return s.substring(0, end);
    }

    /**
     * Подогнать TXT-запись под лимит DNS-SD.
     *
     * Порядок действий:
     *  1. если запись влезает — ничего не меняем;
     *  2. если мешает ACK — сначала выбрасываем его (ACK переотправится в
     *     следующем heartbeat, а текст сообщения терять нельзя);
     *  3. затем урезаем текст и обязательно пишем об этом в лог.
     *
     * Без этой подгонки запись с кириллицей уходит за 255 байт, и часть
     * телефонов просто выбрасывает её — сообщение не доставляется молча.
     */
    private Map<String, String> fitTxtRecord(Map<String, String> record, String what) {
        if (record == null) return null;

        int budget = P2pConfig.MAX_TXT_RECORD_BYTES;
        int size = txtRecordSize(record);
        if (size <= budget) {
            log.d("TXT " + what + ": " + size + "B (ok, budget " + budget + "B)");
            return record;
        }

        log.w("TXT " + what + ": " + size + "B > " + budget + "B, fitting");

        // Отдельно считаем фиксированную часть и вклад msg/ack
        int fixed = 0;
        for (Map.Entry<String, String> e : record.entrySet()) {
            String k = e.getKey();
            if ("msg".equals(k) || "ack".equals(k)) continue;
            fixed += txtEntrySize(k, e.getValue());
        }

        String msg = record.get("msg");
        String ack = record.get("ack");
        // Бюджет под ЗНАЧЕНИЕ msg: вычитаем фиксированные поля, ack и служебные
        // байты самого поля ("msg" + '=' + длина), иначе запись вылезает за лимит
        // ровно на размер ключа.
        int msgKeyOverhead = txtEntrySize("msg", "");
        int ackSize = ack != null ? txtEntrySize("ack", ack) : 0;
        int msgBudget = budget - fixed - ackSize - msgKeyOverhead;

        if (msg != null && msgBudget < P2pConfig.MIN_TEXT_BYTES && ack != null) {
            record.remove("ack");
            ackSize = 0;
            msgBudget = budget - fixed - msgKeyOverhead;
            log.w("TXT " + what + ": ACK dropped to fit size limit");
        }

        if (msg != null) {
            String capped = truncateUtf8(msg, P2pConfig.MAX_TEXT_BYTES);
            if (!capped.equals(msg)) {
                log.w("TXT " + what + ": text capped " + utf8Bytes(msg) + "B -> "
                        + utf8Bytes(capped) + "B (max " + P2pConfig.MAX_TEXT_BYTES + "B)");
                msg = capped;
            }
            if (utf8Bytes(msg) > msgBudget) {
                String trimmed = truncateUtf8(msg, Math.max(0, msgBudget));
                log.w("TXT " + what + ": text truncated " + utf8Bytes(msg) + "B -> "
                        + utf8Bytes(trimmed) + "B to fit DNS-SD limit");
                msg = trimmed;
            }
            record.put("msg", msg);
        }

        // Если запись всё ещё не влезает — уменьшаем список ACK по одному
        // (актуально для WFD_Ack, где текста сообщения нет вовсе).
        while (txtRecordSize(record) > budget) {
            String ackList = record.get("ack");
            if (ackList == null) break;
            int lastComma = ackList.lastIndexOf(',');
            if (lastComma <= 0) {
                record.remove("ack");
                log.w("TXT " + what + ": ACK list dropped entirely to fit size limit");
                break;
            }
            record.put("ack", ackList.substring(0, lastComma));
            log.w("TXT " + what + ": ACK list reduced to fit size limit");
        }

        int finalSize = txtRecordSize(record);
        if (finalSize > budget) {
            log.e("TXT " + what + " still " + finalSize + "B > " + budget + "B — record may be dropped!");
        } else {
            log.d("TXT " + what + " fitted: " + size + "B -> " + finalSize + "B");
        }
        return record;
    }

    // ==================== TXT DEDUPLICATION ====================

    private String buildTxtDedupeKey(String senderId, String serviceName, Map<String, String> record) {
        StringBuilder sb = new StringBuilder();
        sb.append(senderId).append("|");
        sb.append(serviceName).append("|");

        String msgId = record.get("mid");
        if (msgId != null) {
            sb.append("mid=").append(msgId).append("|");
        }

        String ack = record.get("ack");
        if (ack != null) {
            sb.append("ack=").append(ack).append("|");
        }

        String hb = record.get("hb");
        if (hb != null) {
            sb.append("hb=").append(hb).append("|");
        }

        String sent = record.get("sent");
        if (sent != null) {
            sb.append("sent=").append(sent).append("|");
        }

        String recv = record.get("recv");
        if (recv != null) {
            sb.append("recv=").append(recv);
        }

        return sb.toString();
    }

    private boolean isDuplicateTxt(String dedupeKey) {
        long now = System.currentTimeMillis();

        recentTxtRecords.entrySet().removeIf(entry ->
                now - entry.getValue() > P2pConfig.TXT_DEDUP_WINDOW_MS);

        if (recentTxtRecords.size() >= P2pConfig.MAX_RECENT_TXT_RECORDS) {
            String oldestKey = null;
            long oldestTime = Long.MAX_VALUE;
            for (Map.Entry<String, Long> entry : recentTxtRecords.entrySet()) {
                if (entry.getValue() < oldestTime) {
                    oldestTime = entry.getValue();
                    oldestKey = entry.getKey();
                }
            }
            if (oldestKey != null) {
                recentTxtRecords.remove(oldestKey);
            }
        }

        Long lastSeen = recentTxtRecords.get(dedupeKey);
        if (lastSeen != null && now - lastSeen < P2pConfig.TXT_DEDUP_WINDOW_MS) {
            return true;
        }

        recentTxtRecords.put(dedupeKey, now);
        return false;
    }

    // ==================== SERVICE LISTENERS ====================

    private void setupServiceListeners() {
        log.divider("SETUP LISTENERS");

        WifiP2pManager.DnsSdTxtRecordListener txtListener = (fullDomain, record, device) -> {
            if (record == null || device == null) return;

            String senderId = record.get("id");
            if (shortDeviceId.equals(senderId)) return;

            String serviceName = extractServiceName(fullDomain);

            // Записи слотов сообщений не дедуплицируем: их содержимое не меняется
            // после публикации, и раньше повторные приходы глушились ключом
            // дедупликации. Из-за этого «видимость» сообщения истекала по
            // таймауту, и ACK переставал отправляться. Само сообщение всё равно
            // обрабатывается один раз — дедупликация по mid (processedMessageIds).
            boolean isSlotRecord = serviceName != null
                    && serviceName.toUpperCase().startsWith(P2pConfig.MSG_SLOT_PREFIX.toUpperCase());

            if (!isSlotRecord) {
                String dedupeKey = buildTxtDedupeKey(senderId, serviceName, record);
                if (isDuplicateTxt(dedupeKey)) {
                    return;
                }
            }

            int count = txtRecordsReceived.incrementAndGet();
            log.d("TXT #" + count + " | " + serviceName + " | from " + senderId);

            String acks = record.get("ack");
            if (acks != null && !acks.isEmpty()) {
                log.i("Found ACKs in " + serviceName + ": " + acks);
                DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);
                processReceivedAcks(acks, dd);
            }

            if (serviceName != null && P2pConfig.MAIN_SERVICE_NAME.equalsIgnoreCase(serviceName)) {
                handleMainServiceRecord(record, device, serviceName);
            } else if (serviceName != null && serviceName.toUpperCase().startsWith(P2pConfig.MSG_SLOT_PREFIX.toUpperCase())) {
                handleMessageSlotRecord(record, device, serviceName);
            } else if (serviceName != null && P2pConfig.ACK_SERVICE_NAME.equalsIgnoreCase(serviceName)) {
                handleAckServiceRecord(record, device);
            } else if (serviceName != null && P2pConfig.SYNC_SERVICE_NAME.equalsIgnoreCase(serviceName)) {
                handleSyncServiceRecord(record, device);
            }
        };

        WifiP2pManager.DnsSdServiceResponseListener serviceListener = (instanceName, regType, device) -> {
            serviceResponsesReceived.incrementAndGet();
            if (regType != null && regType.contains(P2pConfig.SERVICE_TYPE)) {
                handleServiceDiscovered(device, instanceName);
            }
        };

        manager.setDnsSdResponseListeners(channel, serviceListener, txtListener);
        log.success("DNS-SD listeners configured");
    }

    private void handleMainServiceRecord(Map<String, String> record, WifiP2pDevice device, String serviceName) {
        String senderId = record.get("id");
        if (senderId == null) return;

        DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);

        String sid = record.get("sid");

        if (!validateSessionId(senderId, sid, dd)) {
            dd.lastSeen = System.currentTimeMillis();
            return;
        }

        boolean justCameOnline = dd.checkAndUpdateOnlineTransition();

        bindDeviceIdentity(dd, senderId);
        dd.hasOurApp = true;
        dd.lastSeen = System.currentTimeMillis();
        dd.seenCount++;
        dd.lastServiceName = serviceName;

        String hbStr = record.get("hb");
        if (hbStr != null) {
            try { dd.updateHeartbeat(Long.parseLong(hbStr)); } catch (NumberFormatException e) {}
        }

        // Поправка часов собеседника: heartbeat публикуется каждые 5 секунд,
        // поэтому его t ≈ «сейчас» у отправителя. Без этой поправки телефон
        // с убежавшими часами выглядел как источник «старых» сообщений.
        String tStr = record.get("t");
        if (tStr != null) {
            try { dd.updateClockSkew(Long.parseLong(tStr) * 1000L); } catch (NumberFormatException e) {}
        }

        DeviceState state = stateRepository.getOrCreate(senderId);
        state.name = dd.name;
        state.address = dd.address;
        stateRepository.save();  // ДОБАВЛЕНО: Сохранение

        if (justCameOnline && dd.deviceId != null) {
            log.i("Device " + dd.getShortId() + " came online, triggering sync");
            syncManager.onDeviceBecameOnline(dd.deviceId);
        }

        notifyDeviceUpdated(dd);
    }

    private void handleMessageSlotRecord(Map<String, String> record, WifiP2pDevice device, String serviceName) {
        String senderId = record.get("id");
        String msgId = record.get("mid");
        String message = record.get("msg");
        String targetId = record.get("to");
        String slotStr = record.get("s");
        String sid = record.get("sid");
        String tStr = record.get("t");

        if (senderId == null || msgId == null) return;
        if (targetId != null && !targetId.equals(shortDeviceId)) return;

        DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);

        if (!validateSessionId(senderId, sid, dd)) {
            dd.lastSeen = System.currentTimeMillis();
            return;
        }

        bindDeviceIdentity(dd, senderId);
        dd.hasOurApp = true;
        dd.lastSeen = System.currentTimeMillis();
        dd.lastServiceName = serviceName;

        if (slotStr != null) {
            try { dd.lastSlotIndex = Integer.parseInt(slotStr); } catch (NumberFormatException e) {}
        }

        if (tStr != null) {
            try {
                long msgTsMs = Long.parseLong(tStr) * 1000L;

                // Возраст считаем в шкале времени ОТПРАВИТЕЛЯ (его «сейчас»),
                // иначе расхождение часов (даже на минуты) приводило к тому,
                // что все сообщения устройства молча отбрасывались.
                long peerNowMs = dd.hasClockSkew ? dd.peerNowMs() : msgTsMs;
                long ageSec = (peerNowMs - msgTsMs) / 1000;

                // «Будущее» сообщение — нормально в пределах небольшого дребезга
                if (ageSec > P2pConfig.MAX_MSG_AGE_SEC) {
                    log.w("Ignoring old message slot " + msgId +
                            " from " + senderId + " age=" + ageSec + "s");
                    return;
                }
                if (ageSec < -P2pConfig.MAX_MSG_AGE_SEC) {
                    log.w("Ignoring message with implausible future timestamp " + msgId +
                            " from " + senderId + " age=" + ageSec + "s");
                    return;
                }
            } catch (NumberFormatException e) {}
        }

        dd.currentVisibleMsgIds.add(msgId);
        activeIncomingMessages.put(msgId, System.currentTimeMillis());
        // ACK ставим в очередь сразу: раньше он зависел от «видимости» записи,
        // а она истекала по таймауту раньше, чем ACK успевал уйти.
        // Уже подтверждённые повторно не добавляем, иначе запись слота,
        // приходящая по кругу, заставляла бы вещать ACK бесконечно.
        DiscoveredDevice.ReceivedMessage known = findReceivedMessage(dd, msgId);
        if (known == null || !known.ackConfirmed) {
            pendingAcksToSend.add(msgId);
        }

        if (!processedMessageIds.contains(msgId)) {
            processedMessageIds.add(msgId);
            trimSet(processedMessageIds, P2pConfig.MAX_PROCESSED_IDS);

            dd.addReceivedMessage(msgId, message);

            DeviceState state = stateRepository.getOrCreate(senderId);
            state.addRecvMessage(msgId, message);
            state.name = dd.name;
            state.address = dd.address;
            if (sid != null) {
                state.lastSessionId = sid;
            }
            stateRepository.save();

            log.success("MESSAGE RECEIVED: " + msgId + " from " + senderId);
            log.i("Content: " + message);

            if (listener != null) {
                handler.post(() -> listener.onMessageReceived(dd, msgId, message));
            }
        }

        notifyDeviceUpdated(dd);
    }

    private void handleAckServiceRecord(Map<String, String> record, WifiP2pDevice device) {
        String senderId = record.get("id");
        String acks = record.get("ack");
        String sid = record.get("sid");

        if (senderId == null || shortDeviceId.equals(senderId)) return;

        DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);

        if (!validateSessionId(senderId, sid, dd)) {
            dd.lastSeen = System.currentTimeMillis();
            return;
        }

        bindDeviceIdentity(dd, senderId);
        dd.hasOurApp = true;
        dd.lastSeen = System.currentTimeMillis();

        log.success("ACK SERVICE received from " + senderId + ": " + acks);

        if (acks != null && !acks.isEmpty()) {
            processReceivedAcks(acks, dd);
        }
    }

    /**
     * Принадлежит ли msgId этому устройству (любой сессии).
     * Формат: shortDeviceId_sessionId_counter.
     */
    private boolean isOwnMessageId(String msgId) {
        return msgId != null && msgId.startsWith(shortDeviceId + "_");
    }

    /**
     * Обработать ACK на сообщение, которого нет в pendingMessages.
     * Такое бывает, если сообщение отправлено в прошлой сессии приложения:
     * sessionId сменился, но сообщение всё ещё лежит в репозитории и SYNC его
     * переотправляет. Раньше такой ACK игнорировался — сообщение «вечно
     * не доставлено», а отправитель продолжал его переотправлять.
     */
    private void markDeliveredWithoutPending(String msgId, DiscoveredDevice sender, List<String> batch) {
        // ACK повторяется в каждой записи собеседника — обрабатываем один раз
        if (!processedAcks.add(msgId)) return;
        trimSet(processedAcks, P2pConfig.MAX_PROCESSED_ACKS);

        // Освобождаем слот, если он всё ещё держит это сообщение
        releaseSlotForMessage(msgId);

        if (sender != null) {
            sender.markSentMessageAcked(msgId, batch);
        }

        String deviceId = sender != null ? sender.deviceId : null;
        if (deviceId == null) deviceId = findDeviceIdByMessageId(msgId);

        DeviceState state = deviceId != null ? stateRepository.get(deviceId) : null;
        if (state != null && state.findSentMessage(msgId) != null) {
            state.markAcked(msgId);
            stateRepository.save();
            log.success("ACK for non-pending message applied: " + msgId);
        } else {
            log.d("ACK for unknown message ignored: " + msgId);
        }
    }

    /**
     * Найти устройство, которому адресовано сообщение (по репозиторию).
     */
    private String findDeviceIdByMessageId(String msgId) {
        for (DeviceState st : stateRepository.getAll().values()) {
            if (st.findSentMessage(msgId) != null) return st.deviceId;
        }
        return null;
    }

    /**
     * Освободить слот, в котором опубликовано указанное сообщение.
     */
    private void releaseSlotForMessage(String msgId) {
        if (msgId == null) return;
        Integer slotIndex = null;
        for (Map.Entry<Integer, SlotInfo> e : messageSlots.entrySet()) {
            if (msgId.equals(e.getValue().messageId)) {
                slotIndex = e.getKey();
                break;
            }
        }
        if (slotIndex != null) {
            log.d("Releasing slot " + slotIndex + " for " + msgId);
            releaseSlot(slotIndex);
        }
    }

    private void processReceivedAcks(String acks, DiscoveredDevice sender) {
        String[] ackList = acks.split(",");
        List<String> ackBatch = new ArrayList<>();
        for (String ack : ackList) {
            ack = ack.trim();
            if (!ack.isEmpty()) ackBatch.add(ack);
        }

        for (String ack : ackBatch) {
            if (isOwnMessageId(ack)) {

                if (processedAcks.contains(ack)) {
                    continue;
                }

                PendingMessage pm = pendingMessages.get(ack);
                if (pm == null) {
                    // ACK на сообщение прошлой сессии или уже закрытое
                    markDeliveredWithoutPending(ack, sender, ackBatch);
                    continue;
                }

                processedAcks.add(ack);
                trimSet(processedAcks, P2pConfig.MAX_PROCESSED_ACKS);

                log.success("ACK received for " + ack + " from " + sender.getShortId());

                // Сообщение подтверждено — TXT-путь больше не нужен (B1)
                cancelTxtFallback(ack);

                corr.event(CorrelationLog.EV_ACK_RX, ack, sender.deviceId,
                        "channel", "txt",
                        "batch", String.valueOf(ackBatch.size()));

                sender.markSentMessageAcked(ack, ackBatch);

                if (sender.deviceId != null) {
                    DeviceState state = stateRepository.get(sender.deviceId);
                    if (state != null) {
                        state.markAcked(ack);
                        stateRepository.save();
                    }
                }

                pendingMessages.remove(ack);
                releaseSlot(pm.slotIndex);

                if (listener != null) {
                    String finalAck = ack;
                    handler.post(() -> listener.onAckReceived(sender, finalAck));
                }
            }
        }
    }

    private void handleServiceDiscovered(WifiP2pDevice device, String instanceName) {
        DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);
        dd.lastSeen = System.currentTimeMillis();

        if (instanceName != null) {
            String upper = instanceName.toUpperCase();
            if (upper.startsWith(P2pConfig.MAIN_SERVICE_NAME.toUpperCase()) ||
                    upper.startsWith(P2pConfig.MSG_SLOT_PREFIX.toUpperCase()) ||
                    upper.startsWith(P2pConfig.ACK_SERVICE_NAME.toUpperCase()) ||
                    upper.startsWith(P2pConfig.SYNC_SERVICE_NAME.toUpperCase())) {
                dd.hasOurApp = true;
            }
        }
        notifyDeviceUpdated(dd);
    }

    // ==================== VISIBILITY CHECK ====================

    /**
     * Обновить «видимость» входящих сообщений.
     *
     * ВАЖНО: раньше здесь по таймауту 10 с выставлялся флаг «доставлено»
     * (markReceivedMessageAckConfirmed) — даже если ACK ни разу не был
     * опубликован, а запись просто перестала приходить (собеседник уснул,
     * поиск прервался). Теперь доставка подтверждается только если ACK
     * действительно ушёл в эфир хотя бы один раз, иначе продолжаем вещать.
     */
    private void checkMessageVisibility() {
        long now = System.currentTimeMillis();
        long timeout = 10_000;

        for (DiscoveredDevice dd : deviceCache.values()) {
            Set<String> toRemove = new HashSet<>();

            for (String msgId : dd.currentVisibleMsgIds) {
                Long lastSeen = activeIncomingMessages.get(msgId);
                if (lastSeen == null || now - lastSeen > timeout) {
                    toRemove.add(msgId);

                    DiscoveredDevice.ReceivedMessage rm = findReceivedMessage(dd, msgId);
                    boolean ackPublished = rm != null && rm.ackSendCount > 0;

                    if (ackPublished) {
                        dd.markReceivedMessageAckConfirmed(msgId);
                        pendingAcksToSend.remove(msgId);
                        ackPublishAttempts.remove(msgId);
                        log.success("ACK delivered (published " + rm.ackSendCount + "x): " + msgId);
                    } else {
                        // Сообщение «ушло» из эфира, но подтверждение мы так и не
                        // отправили — оставляем его в очереди ACK.
                        log.w("Message " + msgId + " no longer visible, ACK still pending");
                    }
                }
            }

            dd.currentVisibleMsgIds.removeAll(toRemove);
            for (String msgId : toRemove) {
                activeIncomingMessages.remove(msgId);
            }
        }
    }

    private DiscoveredDevice.ReceivedMessage findReceivedMessage(DiscoveredDevice dd, String msgId) {
        for (DiscoveredDevice.ReceivedMessage rm : dd.receivedMessages) {
            if (msgId.equals(rm.messageId)) return rm;
        }
        return null;
    }

    // ==================== SERVICE REQUESTS ====================

    /**
     * Сброс service requests для очистки системного DNS кэша.
     * Решает проблему когда Android возвращает устаревшие TXT записи.
     */
    private void resetServiceRequests() {
        log.d("Resetting DNS-SD requests to clear cache");

        manager.clearServiceRequests(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                serviceRequests.clear();
                addServiceRequests(() -> {
                    manager.discoverServices(channel, new WifiP2pManager.ActionListener() {
                        @Override
                        public void onSuccess() {
                            log.d("DNS-SD cache reset complete");
                        }
                        @Override
                        public void onFailure(int r) {
                            log.w("discoverServices after reset failed: " + reasonToString(r));
                        }
                    });
                });
            }
            @Override
            public void onFailure(int r) {
                log.w("Failed to clear service requests: " + reasonToString(r));
            }
        });
    }

    private void addServiceRequests(Runnable onComplete) {
        List<WifiP2pServiceRequest> requests = new ArrayList<>();
        requests.add(WifiP2pDnsSdServiceRequest.newInstance(P2pConfig.SERVICE_TYPE));
        requests.add(WifiP2pDnsSdServiceRequest.newInstance());
        requests.add(WifiP2pServiceRequest.newInstance(WifiP2pServiceInfo.SERVICE_TYPE_ALL));

        addRequestsSequentially(requests, 0, onComplete);
    }

    private void addRequestsSequentially(List<WifiP2pServiceRequest> requests, int index, Runnable onComplete) {
        if (index >= requests.size()) {
            log.success("All service requests added: " + serviceRequests.size());
            onComplete.run();
            return;
        }

        WifiP2pServiceRequest req = requests.get(index);
        manager.addServiceRequest(channel, req, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                serviceRequests.add(req);
                addRequestsSequentially(requests, index + 1, onComplete);
            }
            @Override
            public void onFailure(int reason) {
                addRequestsSequentially(requests, index + 1, onComplete);
            }
        });
    }

    // ==================== DISCOVERY (ЕДИНЫЙ ПЛАНИРОВЩИК) ====================

    /**
     * Единственная точка входа для discoverPeers/discoverServices.
     *
     * Раньше в коде одновременно работали четыре независимых цикла
     * (burst, performDiscovery, PEERS_CHANGED, регистрация слота). Каждый
     * повторный discover* во время активного поиска возвращает BUSY, а на
     * части прошивок ещё и прерывает текущий поиск, из-за чего результаты
     * теряются. Здесь запросы сериализуются и троттлятся.
     */
    private void requestDiscovery(String reason) {
        requestDiscovery(reason, false);
    }

    /**
     * Запрос поиска с приоритетом (A1) и фазовым разделением (A2).
     *
     * <p>Отличие приоритетного запроса: нижняя граница интервала —
     * {@link P2pConfig#PRIORITY_DISCOVER_GAP_MS} (800 мс) вместо 4 с. Так
     * событийный поиск (появился пир, зарегистрирован слот, открылось окно
     * синхронизации) срабатывает почти сразу, а не «когда-нибудь».
     *
     * <p>Если пиры уже известны, приоритетный проход сразу идёт в
     * {@code discoverServices}: именно свежие TXT-записи несут сообщения и
     * ACK, а список пиров на этом шаге уже есть. Это A2 — одна команда
     * supplicant за проход вместо двух подряд.
     */
    private void requestDiscovery(String reason, boolean priority) {
        if (!isRunning || manager == null || channel == null) return;

        long now = System.currentTimeMillis();
        long sinceLast = now - lastDiscoverCall;
        long gap = priority ? P2pConfig.PRIORITY_DISCOVER_GAP_MS
                : P2pConfig.MIN_DISCOVER_GAP_MS;

        // D1: без включённого Location Mode discovery на MIUI/Funtouch
        // возвращает пустой список без ошибки — фиксируем это в журнале,
        // но попытку не отменяем (на части прошивок всё же работает).
        if (P2pConfig.REQUIRE_LOCATION_FOR_DISCOVERY && !HwGuard.isLocationEnabled(context)) {
            log.w("Location Mode is OFF — discovery may return an empty peer list (" + reason + ")");
            corr.event(CorrelationLog.EV_DISC_REQ, null, null,
                    "reason", reason, "result", "no-location");
        }

        if (lastDiscoverCall != 0 && sinceLast < gap) {
            log.d("Discovery throttled (" + reason + "), " + sinceLast + "ms since last");
            corr.event(CorrelationLog.EV_DISC_REQ, null, null,
                    "reason", reason, "gap_ms", String.valueOf(sinceLast),
                    "prio", priority ? "1" : "0", "result", "throttled");
            return;
        }

        lastDiscoverCall = now;
        discoveryInProgress = true;
        lastDiscoveryTime = now;
        discoveryPhase++;

        boolean servicesOnly = priority && !deviceCache.isEmpty();
        log.d("Discovery requested: " + reason
                + (priority ? " [priority]" : "")
                + (servicesOnly ? " [services-only]" : "")
                + " | window=" + SyncWindow.describe(now));

        corr.event(CorrelationLog.EV_DISC_REQ, null, null,
                "reason", reason,
                "prio", priority ? "1" : "0",
                "phase", String.valueOf(discoveryPhase),
                "window", SyncWindow.describe(now),
                "peers", String.valueOf(deviceCache.size()),
                "services_only", servicesOnly ? "1" : "0");

        // Сторож: если колбэк discoverServices не придёт (зависший supplicant,
        // перезагрузка Wi-Fi), цикл не должен остановиться навсегда.
        handler.removeCallbacks(discoveryWatchdog);
        handler.postDelayed(discoveryWatchdog, 20_000);

        if (servicesOnly) {
            discoverServicesWithRetry(reason, 0);
            return;
        }

        manager.discoverPeers(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                log.d("discoverPeers started (" + reason + ")");
                corr.event(CorrelationLog.EV_DISC_RES, null, null,
                        "phase", "peers", "result", "ok", "reason", reason);
                handler.postDelayed(() -> discoverServicesWithRetry(reason, 0),
                        P2pConfig.DISCOVER_SERVICES_DELAY_MS);
            }

            @Override
            public void onFailure(int r) {
                log.w("discoverPeers failed: " + reasonToString(r) + " (" + reason + ")");
                corr.event(CorrelationLog.EV_DISC_RES, null, null,
                        "phase", "peers", "result", reasonToString(r), "reason", reason);
                // peers не критичен: сразу пробуем services
                handler.postDelayed(() -> discoverServicesWithRetry(reason, 0), 300);
            }
        });
    }

    /**
     * discoverServices с повтором при BUSY и экспоненциальной паузой.
     * Повтор через 500 мс (как было) гарантированно попадал в тот же BUSY.
     */
    private void discoverServicesWithRetry(String reason, int attempt) {
        if (!isRunning) return;

        manager.discoverServices(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                discoveryInProgress = false;
                handler.removeCallbacks(discoveryWatchdog);
                log.d("discoverServices started (" + reason + ", attempt " + attempt + ")");
                corr.event(CorrelationLog.EV_DISC_RES, null, null,
                        "phase", "services", "result", "ok",
                        "attempt", String.valueOf(attempt), "reason", reason);
                notifyStatus("Devices: " + deviceCache.size() + " | TXT: " + txtRecordsReceived.get());
                scheduleNextDiscovery();
            }

            @Override
            public void onFailure(int r) {
                discoveryInProgress = false;
                log.w("discoverServices failed: " + reasonToString(r)
                        + " (" + reason + ", attempt " + attempt + ")");
                corr.event(CorrelationLog.EV_DISC_RES, null, null,
                        "phase", "services", "result", reasonToString(r),
                        "attempt", String.valueOf(attempt), "reason", reason);

                if (r == WifiP2pManager.BUSY && attempt < P2pConfig.DISCOVER_MAX_RETRIES) {
                    long delay = P2pConfig.DISCOVER_RETRY_BASE_MS << attempt;
                    log.d("Retrying discoverServices in " + delay + "ms");
                    handler.postDelayed(() -> discoverServicesWithRetry(reason, attempt + 1), delay);
                    return;
                }

                scheduleNextDiscovery();
            }
        });
    }

    /**
     * Перерегистрация всех активных сервисов для обновления DNS кэша.
     */
    private void reregisterAllServices() {
        log.d("Re-registering all services to refresh DNS cache");

        // Перерегистрируем main service
        if (mainServiceInfo != null) {
            reregisterMainService();
        }

        // Перерегистрируем активные слоты сообщений
        for (SlotInfo slot : messageSlots.values()) {
            if (slot.isRegistered && slot.serviceInfo != null) {
                reregisterSlot(slot);
            }
        }
    }

    /**
     * Перерегистрация main service
     */
    private void reregisterMainService() {
        final WifiP2pDnsSdServiceInfo oldInfo = mainServiceInfo;

        manager.removeLocalService(channel, oldInfo, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                Map<String, String> record = buildMainServiceRecord();
                WifiP2pDnsSdServiceInfo newInfo = WifiP2pDnsSdServiceInfo.newInstance(
                        P2pConfig.MAIN_SERVICE_NAME, P2pConfig.SERVICE_TYPE, record);

                manager.addLocalService(channel, newInfo, new WifiP2pManager.ActionListener() {
                    @Override
                    public void onSuccess() {
                        mainServiceInfo = newInfo;
                        log.d("Main service re-registered");
                    }
                    @Override
                    public void onFailure(int r) {
                        mainServiceInfo = newInfo;
                        log.w("Main service re-register addLocal failed: " + reasonToString(r));
                    }
                });
            }
            @Override
            public void onFailure(int r) {
                log.w("Main service re-register removeLocal failed: " + reasonToString(r));
                // Пробуем добавить без удаления
                Map<String, String> record = buildMainServiceRecord();
                mainServiceInfo = WifiP2pDnsSdServiceInfo.newInstance(
                        P2pConfig.MAIN_SERVICE_NAME, P2pConfig.SERVICE_TYPE, record);
                manager.addLocalService(channel, mainServiceInfo, null);
            }
        });
    }

    /**
     * Перерегистрация слота сообщения
     */
    private void reregisterSlot(SlotInfo slot) {
        if (slot.serviceInfo == null) return;

        final WifiP2pDnsSdServiceInfo serviceInfo = slot.serviceInfo;
        final int slotIndex = slot.slotIndex;

        manager.removeLocalService(channel, serviceInfo, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                manager.addLocalService(channel, serviceInfo, new WifiP2pManager.ActionListener() {
                    @Override
                    public void onSuccess() {
                        log.d("Slot " + slotIndex + " re-registered");
                    }
                    @Override
                    public void onFailure(int r) {
                        log.w("Slot " + slotIndex + " re-register addLocal failed");
                    }
                });
            }
            @Override
            public void onFailure(int r) {
                // Пробуем добавить без удаления
                manager.addLocalService(channel, serviceInfo, null);
            }
        });
    }

    // ==================== PERIODIC TASKS ====================

    private void schedulePeriodicTasks() {
        handler.removeCallbacks(heartbeatRunnable);
        handler.removeCallbacks(ackUpdateRunnable);
        handler.removeCallbacks(visibilityCheckRunnable);
        handler.removeCallbacks(onlineCheckRunnable);
        handler.removeCallbacks(syncCheckRunnable);
        handler.removeCallbacks(dnsCacheResetRunnable);  // ДОБАВИТЬ
        //handler.removeCallbacks(serviceReregisterRunnable);  // ДОБАВИТЬ

        handler.postDelayed(heartbeatRunnable, P2pConfig.HEARTBEAT_INTERVAL);
        handler.postDelayed(ackUpdateRunnable, P2pConfig.ACK_UPDATE_INTERVAL);
        handler.postDelayed(visibilityCheckRunnable, 5000);
        handler.postDelayed(onlineCheckRunnable, P2pConfig.DEVICE_ONLINE_THRESHOLD / 2);
        handler.postDelayed(syncCheckRunnable, P2pConfig.SYNC_CHECK_INTERVAL);

        // Новые быстрые циклы: окна синхронизированного поиска (A3) и дешёвый
        // опрос списка пиров (A4). Оба не запускают сканирование сами по себе.
        handler.removeCallbacks(syncWindowRunnable);
        handler.removeCallbacks(peersPollRunnable);
        handler.postDelayed(syncWindowRunnable, SyncWindow.msUntilOpen(System.currentTimeMillis()) + 50);
        handler.postDelayed(peersPollRunnable, P2pConfig.PEERS_POLL_INTERVAL_MS);

        // ВАЖНО: dnsCacheResetRunnable (clearServiceRequests каждые 45 с) и
        // serviceReregisterRunnable (remove+add всех сервисов каждые 30 с)
        // намеренно НЕ планируются.
        //  - clearServiceRequests: пока запросы сняты, TXT не приходят —
        //    входящие сообщения и ACK теряются, supplicant отвечает BUSY;
        //  - remove+add всех сервисов: окно без advertise + риск потерять
        //    указатель на зарегистрированный сервис (станция «исчезает»).
        // Свежесть WFD_Main обеспечивает heartbeat (publishMainService каждые 5 с),
        // это чаще требуемых 10 с (D8) — дополнительная перерегистрация не нужна.
    }

    /**
     * A3: окна синхронизированного поиска.
     *
     * Обе стороны считают границы окон от эпохи, поэтому окна совпадают на
     * обоих телефонах без переговоров. В начале окна запускаем приоритетный
     * поиск — так «встреча» происходит предсказуемо, а не через случайные
     * 0…8 секунд.
     */
    private final Runnable syncWindowRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;

            long now = System.currentTimeMillis();
            boolean open = SyncWindow.isOpen(now);

            if (open) {
                corr.event(CorrelationLog.EV_WINDOW, null, null,
                        "state", "open",
                        "phase_ms", String.valueOf(SyncWindow.phase(now)),
                        "skew_ms", String.valueOf(peerClock.offset(myPeerId())));
                requestDiscovery("sync-window", true);

                long delay = SyncWindow.msUntilClose(now)
                        + (P2pConfig.SYNC_WINDOW_PERIOD_MS - P2pConfig.SYNC_WINDOW_LENGTH_MS) + 50;
                handler.postDelayed(this, delay);
            } else {
                handler.postDelayed(this, SyncWindow.msUntilOpen(now) + 50);
            }
        }
    };

    /**
     * A4: дешёвый опрос пиров.
     *
     * {@code requestPeers} не запускает сканирование — это чтение уже
     * известного supplicant'у списка, BUSY не возвращается. Позволяет
     * заметить собеседника, которого нашёл он сам, не дожидаясь нашего
     * discover-цикла, и обновлять lastSeen без нагрузки на радио.
     */
    private final Runnable peersPollRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning || manager == null || channel == null) return;

            manager.requestPeers(channel, peers -> {
                int count = peers.getDeviceList().size();
                long now = System.currentTimeMillis();

                for (WifiP2pDevice device : peers.getDeviceList()) {
                    DiscoveredDevice dd = deviceCache.get(device.deviceAddress);
                    boolean isNew = dd == null;
                    if (isNew) {
                        dd = getOrCreateDevice(device.deviceAddress, device);
                    }
                    dd.lastSeen = now;

                    if (device.deviceName != null && device.deviceName.contains(P2pConfig.APP_MARKER)) {
                        dd.hasOurApp = true;
                        String extracted = extractDeviceIdFromName(device.deviceName);
                        if (extracted != null && dd.deviceId == null) {
                            bindDeviceIdentity(dd, extracted);
                        }
                        if (extracted != null) {
                            corr.event(CorrelationLog.EV_PEERS, null, extracted,
                                    "source", "requestPeers", "count", String.valueOf(count));
                        }
                    }
                }

                corr.event(CorrelationLog.EV_PEERS, null, null,
                        "count", String.valueOf(count), "known", String.valueOf(deviceCache.size()));
            });

            handler.postDelayed(this, P2pConfig.PEERS_POLL_INTERVAL_MS);
        }
    };


    private final Runnable heartbeatRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            updateMainService();
            handler.postDelayed(this, P2pConfig.HEARTBEAT_INTERVAL);
        }
    };

    private final Runnable serviceReregisterRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;

            reregisterAllServices();

            handler.postDelayed(this, SERVICE_REREGISTER_INTERVAL);
        }
    };

    private final Runnable ackUpdateRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            updateAckService();
            handler.postDelayed(this, P2pConfig.ACK_UPDATE_INTERVAL);
        }
    };

    private final Runnable visibilityCheckRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            checkMessageVisibility();
            handler.postDelayed(this, 5000);
        }
    };

    private final Runnable onlineCheckRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            cleanupExpiredDevices();
            maybeInitiateGroup();
            handler.postDelayed(this, P2pConfig.DEVICE_ONLINE_THRESHOLD / 2);
        }
    };

    private final Runnable syncCheckRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            syncManager.checkSyncNeeded();
            handler.postDelayed(this, P2pConfig.SYNC_CHECK_INTERVAL);
        }
    };

    private final Runnable dnsCacheResetRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;

            resetServiceRequests();

            handler.postDelayed(this, DNS_CACHE_RESET_INTERVAL);
        }
    };



    private void scheduleNextDiscovery() {
        handler.removeCallbacks(discoveryRunnable);
        handler.postDelayed(discoveryRunnable, currentInterval);
    }

    /**
     * «Главный» собеседник — тот, с кем есть быстрый канал, иначе первый
     * онлайн-пир с нашим приложением. Нужен для оценки расхождения часов и
     * для строки диагностики; на логику доставки не влияет.
     */
    private String myPeerId() {
        if (groupLink != null && groupLink.getKnownPeerCount() > 0) {
            for (DiscoveredDevice dd : deviceCache.values()) {
                if (dd.deviceId != null && groupLink.knows(dd.deviceId)) {
                    return dd.deviceId;
                }
            }
        }
        for (DiscoveredDevice dd : deviceCache.values()) {
            if (dd.hasOurApp && dd.isOnline() && dd.deviceId != null) {
                return dd.deviceId;
            }
        }
        return null;
    }

    private final Runnable discoveryWatchdog = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            if (!discoveryInProgress) return;

            log.w("Discovery watchdog: no callback within 20s, restarting cycle");
            discoveryInProgress = false;
            lastDiscoverCall = 0;
            scheduleNextDiscovery();
        }
    };

    private final Runnable discoveryRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            performDiscovery();
        }
    };

    private void performDiscovery() {
        requestDiscovery("periodic");

        // Страховка: если requestDiscovery был оттроттлен (например, только что
        // был forceRefresh), следующий проход всё равно должен быть запланирован.
        if (!discoveryInProgress) {
            scheduleNextDiscovery();
        }
    }

    /**
     * Получить расширенную диагностику (для UI)
     */
    public String getExtendedDiagnosticInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append(getDiagnosticInfo());

        sb.append("\n\n═══ RUNTIME STATE ═══\n");
        sb.append("Running: ").append(isRunning).append("\n");
        sb.append("Discovery active: ").append(discoveryInProgress).append("\n");
        sb.append("Restart count: ").append(discoveryRestartCount).append("\n");
        sb.append("WiFi Lock: ").append(wifiLock != null && wifiLock.isHeld()).append("\n");
        sb.append("Socket transport: ")
                .append(socketTransport != null ? "ON" : "off")
                .append(" | peers=")
                .append(socketTransport != null ? socketTransport.getConnectedCount() : 0)
                .append(" | group=").append(groupFormed)
                .append(" | GO=").append(groupOwner).append("\n");

        sb.append("\n═══ FAST PATH ═══\n");
        sb.append("Fast path primary: ").append(P2pConfig.SOCKET_IS_PRIMARY_PATH).append("\n");
        sb.append("Group link (UDP): ")
                .append(groupLink != null ? "ON, peers=" + groupLink.getKnownPeerCount() : "off")
                .append("\n");
        sb.append("TXT fallback pending: ").append(pendingTxtFallbacks.size()).append("\n");
        sb.append("Relay cache: ").append(relayCache.size()).append("\n");
        sb.append("Discovery window: ")
                .append(SyncWindow.describe(System.currentTimeMillis()))
                .append(" (period=").append(P2pConfig.SYNC_WINDOW_PERIOD_MS)
                .append("ms, len=").append(P2pConfig.SYNC_WINDOW_LENGTH_MS).append("ms)\n");
        String peer = myPeerId();
        if (peer != null) {
            sb.append("Peer ").append(CorrelationLog.shortId(peer)).append(": ")
                    .append(linkQuality.describe(peer))
                    .append(" | clock skew=").append(peerClock.offset(peer)).append("ms\n");
        }
        sb.append("Correlation log: ")
                .append(corr.isFileAvailable() ? corr.getFile().getName() : "unavailable")
                .append("\n");

        long now = System.currentTimeMillis();
        if (lastDiscoveryTime > 0) {
            sb.append("Last discovery: ").append((now - lastDiscoveryTime) / 1000).append("s ago\n");
        }
        if (lastPeersFoundTime > 0) {
            sb.append("Last peers: ").append((now - lastPeersFoundTime) / 1000).append("s ago\n");
        }

        return sb.toString();
    }
    // ==================== DEVICE MANAGEMENT ====================

    private DiscoveredDevice getOrCreateDevice(String address, WifiP2pDevice device) {
        DiscoveredDevice dd = deviceCache.get(address);

        if (dd == null) {
            dd = new DiscoveredDevice();
            dd.address = address;
            dd.name = device.deviceName;
            dd.device = device;
            dd.firstSeen = System.currentTimeMillis();
            dd.lastSeen = System.currentTimeMillis();
            dd.seenCount = 1;

            if (device.deviceName != null && device.deviceName.contains(P2pConfig.APP_MARKER)) {
                dd.hasOurApp = true;
                String nameWithoutMarker = device.deviceName.replace(P2pConfig.APP_MARKER, "");
                String[] parts = nameWithoutMarker.trim().split(" ", 2);
                if (parts.length > 0 && parts[0].length() >= 8) {
                    bindDeviceIdentity(dd, parts[0]);
                }
                dd.name = parts.length > 1 ? parts[1] : nameWithoutMarker;
            }

            deviceCache.put(address, dd);
            notifyDeviceFound(dd);
            onNewDeviceFound();
        } else {
            dd.device = device;
            dd.lastSeen = System.currentTimeMillis();
            dd.seenCount++;
        }

        return dd;
    }


    /**
     * Подрезать synchronized-множество до maxSize.
     * Итерация по Collections.synchronizedSet без synchronized(...) может дать
     * ConcurrentModificationException, если в это время пишет другой поток.
     */
    private static void trimSet(Set<String> set, int maxSize) {
        synchronized (set) {
            while (set.size() > maxSize) {
                Iterator<String> it = set.iterator();
                if (!it.hasNext()) break;
                it.next();
                it.remove();
            }
        }
    }

    /**
     * Привязать deviceId к записи устройства и склеить возможный дубль.
     *
     * Если то же deviceId уже встречалось под другим адресом (например, телефон
     * перезагрузился и получил новый рандомизированный P2P-адрес), старая запись
     * удаляется, а её история переносится в актуальную. Раньше обе записи жили
     * в кэше одновременно: в UI был дубль, история терялась, отправка могла
     * уйти «фантомному» устройству.
     */
    private void bindDeviceIdentity(DiscoveredDevice dd, String senderId) {
        if (dd == null || senderId == null || senderId.isEmpty()) return;

        dd.deviceId = senderId;

        String knownAddress = deviceIdToAddress.get(senderId);
        if (knownAddress != null && !knownAddress.equals(dd.address)) {
            DiscoveredDevice stale = deviceCache.get(knownAddress);
            if (stale != null && stale != dd) {
                mergeDeviceHistory(stale, dd);
                deviceCache.remove(knownAddress, stale);
                notifyDeviceLost(stale);
                log.i("Merged duplicate entry " + knownAddress + " -> " + dd.address
                        + " (" + senderId + ")");
            }
        }
        deviceIdToAddress.put(senderId, dd.address);
    }

    /**
     * Перенести историю сообщений из старой записи в актуальную.
     */
    private void mergeDeviceHistory(DiscoveredDevice stale, DiscoveredDevice target) {
        for (DiscoveredDevice.SentMessage sm : stale.sentMessages) {
            boolean exists = false;
            for (DiscoveredDevice.SentMessage m : target.sentMessages) {
                if (m.messageId.equals(sm.messageId)) { exists = true; break; }
            }
            if (!exists) target.sentMessages.add(sm);
        }

        for (DiscoveredDevice.ReceivedMessage rm : stale.receivedMessages) {
            boolean exists = false;
            for (DiscoveredDevice.ReceivedMessage m : target.receivedMessages) {
                if (m.messageId.equals(rm.messageId)) { exists = true; break; }
            }
            if (!exists) target.receivedMessages.add(rm);
        }

        target.currentVisibleMsgIds.addAll(stale.currentVisibleMsgIds);

        if (target.sessionId == null) target.sessionId = stale.sessionId;
        if (target.name == null || target.name.isEmpty()) target.name = stale.name;
        if (target.firstSeen == 0 || (stale.firstSeen > 0 && stale.firstSeen < target.firstSeen)) {
            target.firstSeen = stale.firstSeen;
        }
        target.hasOurApp |= stale.hasOurApp;
    }

    private void onNewDeviceFound() {
        lastNewDeviceTime = System.currentTimeMillis();

        // Быстрый режим ограничен по времени: раньше currentInterval
        // выставлялся в FAST_INTERVAL навсегда (раньше его сбрасывал
        // onBurstComplete, а вместе с бёрстом пропал и сброс).
        handler.removeCallbacks(fastModeRunnable);
        handler.postDelayed(fastModeRunnable, P2pConfig.FAST_MODE_DURATION);

        if (currentInterval != P2pConfig.FAST_INTERVAL) {
            currentInterval = P2pConfig.FAST_INTERVAL;
            handler.removeCallbacks(discoveryRunnable);
            scheduleNextDiscovery();
        }
    }

    private final Runnable fastModeRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isRunning) return;
            if (currentInterval != P2pConfig.NORMAL_INTERVAL) {
                log.d("Leaving fast discovery mode after " + P2pConfig.FAST_MODE_DURATION + "ms");
                currentInterval = P2pConfig.NORMAL_INTERVAL;
            }
        }
    };

//    private void cleanupExpiredDevices() {
//        long now = System.currentTimeMillis();
//        Iterator<Map.Entry<String, DiscoveredDevice>> it = deviceCache.entrySet().iterator();
//        while (it.hasNext()) {
//            Map.Entry<String, DiscoveredDevice> entry = it.next();
//            if (now - entry.getValue().lastSeen > P2pConfig.CACHE_TTL) {
//                notifyDeviceLost(entry.getValue());
//                it.remove();
//            }
//        }
//    }

    private void cleanupExpiredDevices() {
        long now = System.currentTimeMillis();
        int removedCount = 0;
        int keptCount = 0;

        Iterator<Map.Entry<String, DiscoveredDevice>> it = deviceCache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, DiscoveredDevice> entry = it.next();
            DiscoveredDevice dd = entry.getValue();
            long age = now - dd.lastSeen;

            if (age > P2pConfig.CACHE_TTL) {
                // Проверяем, есть ли устройство в репозитории (история общения)
                if (dd.deviceId != null && stateRepository.get(dd.deviceId) != null) {
                    // Не удаляем - есть история, просто помечаем как offline
                    keptCount++;
                    log.d("Keeping saved device (offline): " + dd.getShortId() +
                            " age=" + (age / 1000) + "s");
                    continue;
                }

                // Удаляем устройство без истории
                log.d("Removing expired device: " + dd.getShortId() +
                        " age=" + (age / 1000) + "s");
                notifyDeviceLost(dd);
                if (dd.deviceId != null) {
                    deviceIdToAddress.remove(dd.deviceId, dd.address);
                }
                it.remove();
                removedCount++;
            }
        }

        if (removedCount > 0 || keptCount > 0) {
            log.i("Cleanup: removed=" + removedCount + ", kept=" + keptCount +
                    ", total=" + deviceCache.size());
        }
    }

    // ==================== BROADCAST RECEIVER ====================

    private void registerReceiver() {
        IntentFilter filter = new IntentFilter();
        filter.addAction(WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION);
        filter.addAction(WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION);
        filter.addAction(WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION);
        filter.addAction(WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION);
        filter.addAction(WifiP2pManager.WIFI_P2P_THIS_DEVICE_CHANGED_ACTION);

        receiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context ctx, Intent intent) {
                handleBroadcast(intent);
            }
        };

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            context.registerReceiver(receiver, filter, Context.RECEIVER_NOT_EXPORTED);
        } else {
            context.registerReceiver(receiver, filter);
        }
    }

    private void handleBroadcast(Intent intent) {
        String action = intent.getAction();
        if (action == null) return;

        switch (action) {
            case WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION:
                int state = intent.getIntExtra(WifiP2pManager.EXTRA_WIFI_STATE, -1);
                boolean enabled = state == WifiP2pManager.WIFI_P2P_STATE_ENABLED;
                log.i("WiFi P2P state: " + (enabled ? "ENABLED" : "DISABLED"));

                if (!enabled) {
                    notifyError("WiFi P2P is disabled");
                    logDiagnosticState("WIFI_P2P_DISABLED");

                    // Останавливаем периодику: пока P2P выключен, запросы всё
                    // равно вернут ERROR, а таймеры зря жгут батарею.
                    handler.removeCallbacks(discoveryRunnable);
                    handler.removeCallbacks(discoveryWatchdog);
                    discoveryInProgress = false;
                } else {
                    // Раньше при включении Wi-Fi обратно ничего не происходило:
                    // ни discovery, ни сервисы не перезапускались, и приём
                    // не возвращался до перезапуска приложения.
                    log.success("WiFi P2P enabled — restarting discovery stack");
                    mainServiceInfo = null;
                    mainServicePublishInFlight = false;
                    lastDiscoverCall = 0;
                    handler.postDelayed(this::initializeAndStart, 1500);
                }
                break;

            case WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION:
                lastPeersFoundTime = System.currentTimeMillis();
                manager.requestPeers(channel, peers -> {
                    int peerCount = peers.getDeviceList().size();
                    log.d("Peers changed: " + peerCount + " devices");

                    for (WifiP2pDevice device : peers.getDeviceList()) {
                        DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);

                        // Быстрое определение по имени устройства
                        if (device.deviceName != null &&
                                device.deviceName.contains(P2pConfig.APP_MARKER)) {

                            if (!dd.hasOurApp) {
                                log.success("App detected by name: " + device.deviceName);
                            }
                            dd.hasOurApp = true;

                            String extractedId = extractDeviceIdFromName(device.deviceName);
                            if (extractedId != null && dd.deviceId == null) {
                                bindDeviceIdentity(dd, extractedId);
                                log.d("Extracted device ID: " + extractedId);
                            }

                            notifyDeviceUpdated(dd);
                        }
                    }

                    // Запрос сервисов после обнаружения peers — через общий
                    // троттлинг, иначе получаем BUSY на каждом событии
                    if (peerCount > 0) {
                        requestDiscovery("peers-changed");
                    }
                });
                break;

            case WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION:
                WifiP2pGroup group = intent.getParcelableExtra(WifiP2pManager.EXTRA_WIFI_P2P_GROUP);

                if (P2pConfig.ENABLE_P2P_SOCKET_TRANSPORT) {
                    // Группа нужна для сокетного транспорта: не рвём её, а
                    // сообщаем транспорту параметры (кто владелец, какой адрес).
                    handleGroupConnection();
                    break;
                }

                if (group != null) {
                    log.d("Connection changed: isGO=" + group.isGroupOwner() +
                            " clients=" + group.getClientList().size());
                    if (group.isGroupOwner()) {
                        manager.removeGroup(channel, null);
                    }
                }
                break;

            case WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION:
                int dState = intent.getIntExtra(WifiP2pManager.EXTRA_DISCOVERY_STATE, -1);
                boolean discovering = dState == WifiP2pManager.WIFI_P2P_DISCOVERY_STARTED;
                log.d("Discovery state: " + (discovering ? "STARTED" : "STOPPED"));

                if (!discovering) {
                    discoveryInProgress = false;
                    logDiagnosticState("DISCOVERY_STOPPED");
                }
                break;

            case WifiP2pManager.WIFI_P2P_THIS_DEVICE_CHANGED_ACTION:
                WifiP2pDevice thisDevice = intent.getParcelableExtra(WifiP2pManager.EXTRA_WIFI_P2P_DEVICE);
                if (thisDevice != null) {
                    macAddress = thisDevice.deviceAddress;
                    log.d("This device: " + thisDevice.deviceName + " [" + macAddress + "]");
                }
                break;
        }
    }

    /**
     * Извлечь deviceId из имени устройства с маркером
     * "[WFD]a1b2c3d4 Model Name" -> "a1b2c3d4"
     */
    private String extractDeviceIdFromName(String name) {
        if (name == null || !name.contains(P2pConfig.APP_MARKER)) {
            return null;
        }

        try {
            int markerEnd = name.indexOf(P2pConfig.APP_MARKER) + P2pConfig.APP_MARKER.length();
            String rest = name.substring(markerEnd).trim();

            int space = rest.indexOf(' ');
            String id = space > 0 ? rest.substring(0, space) : rest;

            // Проверяем что это похоже на ID (минимум 8 символов, hex)
            if (id.length() >= 8 && id.matches("[a-fA-F0-9]+")) {
                return id;
            }
        } catch (Exception e) {
            log.w("Failed to extract device ID from: " + name);
        }

        return null;
    }

    /**
     * Логирование диагностического состояния системы
     */
    private void logDiagnosticState(String trigger) {
        long now = System.currentTimeMillis();

        StringBuilder sb = new StringBuilder();
        sb.append("\n╔══════════════════════════════════════════╗\n");
        sb.append("║ DIAGNOSTIC STATE: ").append(trigger).append("\n");
        sb.append("╠══════════════════════════════════════════╣\n");

        // Основное состояние
        sb.append("║ Running: ").append(isRunning).append("\n");
        sb.append("║ Discovery in progress: ").append(discoveryInProgress).append("\n");
        sb.append("║ Discovery restarts: ").append(discoveryRestartCount).append("\n");

        // WiFi Lock
        sb.append("║ WiFi Lock held: ").append(wifiLock != null && wifiLock.isHeld()).append("\n");

        // Таймеры
        if (lastDiscoveryTime > 0) {
            sb.append("║ Last discovery: ").append((now - lastDiscoveryTime) / 1000).append("s ago\n");
        }
        if (lastPeersFoundTime > 0) {
            sb.append("║ Last peers found: ").append((now - lastPeersFoundTime) / 1000).append("s ago\n");
        }
        if (lastHeartbeatSentTime > 0) {
            sb.append("║ Last heartbeat: ").append((now - lastHeartbeatSentTime) / 1000).append("s ago\n");
        }

        // Устройства
        int total = deviceCache.size();
        int online = 0;
        int withApp = 0;
        for (DiscoveredDevice dd : deviceCache.values()) {
            if (dd.isOnline()) online++;
            if (dd.hasOurApp) withApp++;
        }
        sb.append("║ Devices: total=").append(total)
                .append(" online=").append(online)
                .append(" withApp=").append(withApp).append("\n");

        // Сообщения
        sb.append("║ Pending messages: ").append(pendingMessages.size()).append("\n");
        sb.append("║ Pending ACKs: ").append(pendingAcksToSend.size()).append("\n");
        sb.append("║ TXT received: ").append(txtRecordsReceived.get()).append("\n");

        // Слоты
        sb.append("║ Active slots: ").append(messageSlots.size()).append("/")
                .append(P2pConfig.MAX_MSG_SLOTS).append("\n");

        sb.append("╚══════════════════════════════════════════╝");

        log.i(sb.toString());
    }

    // ==================== HELPERS ====================

    private String extractServiceName(String fullDomain) {
        if (fullDomain == null) return null;
        int firstDot = fullDomain.indexOf('.');
        if (firstDot > 0) return fullDomain.substring(0, firstDot);
        return fullDomain;
    }

    private String reasonToString(int reason) {
        switch (reason) {
            case WifiP2pManager.P2P_UNSUPPORTED: return "P2P_UNSUPPORTED";
            case WifiP2pManager.ERROR: return "ERROR";
            case WifiP2pManager.BUSY: return "BUSY";
            default: return "UNKNOWN(" + reason + ")";
        }
    }

    // ==================== NOTIFICATIONS ====================

    private void notifyDeviceFound(DiscoveredDevice d) {
        handler.post(() -> { if (listener != null) listener.onDeviceFound(d); });
    }

    private void notifyDeviceUpdated(DiscoveredDevice d) {
        handler.post(() -> { if (listener != null) listener.onDeviceUpdated(d); });
    }

    private void notifyDeviceLost(DiscoveredDevice d) {
        handler.post(() -> { if (listener != null) listener.onDeviceLost(d); });
    }

    private void notifyStatus(String status) {
        handler.post(() -> { if (listener != null) listener.onStatusChanged(status); });
    }

    private void notifyError(String message) {
        log.error(message);
        handler.post(() -> { if (listener != null) listener.onError(message); });
    }
}