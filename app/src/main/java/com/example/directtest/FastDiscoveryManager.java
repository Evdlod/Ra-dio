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

    private final Map<String, PendingMessage> pendingMessages = new ConcurrentHashMap<>();
    private final Set<String> processedMessageIds = Collections.synchronizedSet(new LinkedHashSet<>());

    // ACK tracking
    private final Map<String, Long> activeIncomingMessages = new ConcurrentHashMap<>();
    private final Set<String> pendingAcksToSend = Collections.synchronizedSet(new LinkedHashSet<>());

    // Дедупликация TXT записей
    private final Map<String, Long> recentTxtRecords = new ConcurrentHashMap<>();

    // Трекинг обработанных ACK
    private final Set<String> processedAcks = Collections.synchronizedSet(new LinkedHashSet<>());

    // SYNC system
    private DeviceStateRepository stateRepository;
    private SyncManager syncManager;

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
        registerReceiver();
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

        // Освобождаем WiFi Lock
        releaseWifiLock();

        messageSlots.clear();
        pendingMessages.clear();
        serviceRequests.clear();
        deviceCache.clear();
        activeIncomingMessages.clear();
        pendingAcksToSend.clear();
        processedMessageIds.clear();
        recentTxtRecords.clear();
        processedAcks.clear();

        log.i("Stop complete. Discovery restarts: " + discoveryRestartCount);
        notifyStatus("Stopped");
    }

    /**
     * Получить WiFi Lock для предотвращения засыпания WiFi
     */
    private void acquireWifiLock() {
        try {
            WifiManager wifiManager = (WifiManager) context.getApplicationContext()
                    .getSystemService(Context.WIFI_SERVICE);
            if (wifiManager != null) {
                wifiLock = wifiManager.createWifiLock(
                        WifiManager.WIFI_MODE_FULL_HIGH_PERF,
                        "DirectTest:DiscoveryLock"
                );
                wifiLock.setReferenceCounted(false);
                wifiLock.acquire();
                log.success("WiFi Lock acquired");
            }
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

        registerMessageSlot(freeSlot, msgId, message, targetDeviceId);

        log.i("SEND MESSAGE: " + msgId + " slot=" + freeSlot +
                (targetDeviceId != null ? " to=" + targetDeviceId : " (broadcast)"));

        if (listener != null) {
            listener.onMessageSent(msgId, message, targetDeviceId);
        }
        return msgId;
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

        String acks = buildAckString();
        if (!acks.isEmpty()) {
            record.put("ack", acks);
        }

        return record;
    }

    private String buildAckString() {
        StringBuilder sb = new StringBuilder();
        int count = 0;

        for (DiscoveredDevice dd : deviceCache.values()) {
            List<String> deviceAcks = dd.getPendingAckMessageIds();
            for (String msgId : deviceAcks) {
                if (count >= P2pConfig.MAX_ACKS_PER_RECORD) break;
                if (sb.length() > 0) sb.append(",");
                sb.append(msgId);
                dd.markReceivedMessageAckSent(msgId);
                pendingAcksToSend.add(msgId);
                count++;
            }
            if (count >= P2pConfig.MAX_ACKS_PER_RECORD) break;
        }

        return sb.toString();
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

        DiscoveredDevice dd = getOrCreateDevice(device.deviceAddress, device);

        if (!validateSessionId(senderId, sid, dd)) {
            dd.lastSeen = System.currentTimeMillis();
            return;
        }

        dd.deviceId = senderId;
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
        // Проверяем что это наше сообщение текущей сессии
        if (!msgId.startsWith(shortDeviceId + "_" + sessionId + "_")) {
            return;
        }

        // Дедупликация
        if (processedAcks.contains(msgId)) {
            return;
        }

        PendingMessage pm = pendingMessages.get(msgId);
        if (pm == null) {
            // Сообщение уже обработано или не существует - но всё равно обновим UI
            sender.markSentMessageAcked(msgId, Collections.singletonList(msgId));
            return;
        }

        // Помечаем как обработанный
        processedAcks.add(msgId);
        if (processedAcks.size() > P2pConfig.MAX_PROCESSED_ACKS) {
            Iterator<String> it = processedAcks.iterator();
            if (it.hasNext()) { it.next(); it.remove(); }
        }

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

    private void resendMessage(String targetDeviceId, String msgId, String text) {
        int slot = findFreeSlot();
        if (slot < 0) slot = releaseOldestSlot();

        log.i("RESENDING " + msgId + " to " + targetDeviceId + " slot=" + slot);
        registerMessageSlot(slot, msgId, text, targetDeviceId);
    }

    // ==================== MESSAGE SLOTS ====================

    private int findFreeSlot() {
        for (int i = 0; i < P2pConfig.MAX_MSG_SLOTS; i++) {
            if (!messageSlots.containsKey(i)) return i;
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

        String acks = buildAckString();
        if (!acks.isEmpty()) {
            record.put("ack", acks);
        }

        fitTxtRecord(record, "Msg" + slotIndex);

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
                requestDiscovery("slot-registered");
            }
            @Override
            public void onFailure(int r) {
                log.error("Failed to register slot " + slotIndex);
            }
        });

        handler.postDelayed(() -> {
            SlotInfo current = messageSlots.get(slotIndex);
            if (current != null && msgId.equals(current.messageId)) {
                boolean acked = false;
                for (DiscoveredDevice dd : deviceCache.values()) {
                    for (DiscoveredDevice.SentMessage sm : dd.sentMessages) {
                        if (sm.messageId.equals(msgId) && sm.acknowledged) {
                            acked = true;
                            break;
                        }
                    }
                    if (acked) break;
                }

                if (acked) {
                    releaseSlot(slotIndex);
                } else {
                    log.w("Slot " + slotIndex + " extended - no ACK for " + msgId);
                    handler.postDelayed(() -> releaseSlot(slotIndex), P2pConfig.SLOT_TIMEOUT);
                }
            }
        }, P2pConfig.SLOT_TIMEOUT);
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

            String dedupeKey = buildTxtDedupeKey(senderId, serviceName, record);
            if (isDuplicateTxt(dedupeKey)) {
                return;
            }

            int count = txtRecordsReceived.incrementAndGet();
            log.i("TXT #" + count + " | " + serviceName + " | from " + senderId);

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

        dd.deviceId = senderId;
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

        dd.deviceId = senderId;
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

        if (!processedMessageIds.contains(msgId)) {
            processedMessageIds.add(msgId);
            if (processedMessageIds.size() > P2pConfig.MAX_PROCESSED_IDS) {
                Iterator<String> it = processedMessageIds.iterator();
                if (it.hasNext()) { it.next(); it.remove(); }
            }

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

        dd.deviceId = senderId;
        dd.hasOurApp = true;
        dd.lastSeen = System.currentTimeMillis();

        log.success("ACK SERVICE received from " + senderId + ": " + acks);

        if (acks != null && !acks.isEmpty()) {
            processReceivedAcks(acks, dd);
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
            if (ack.startsWith(shortDeviceId + "_" + sessionId + "_")) {

                if (processedAcks.contains(ack)) {
                    continue;
                }

                PendingMessage pm = pendingMessages.get(ack);
                if (pm == null) {
                    continue;
                }

                processedAcks.add(ack);
                if (processedAcks.size() > P2pConfig.MAX_PROCESSED_ACKS) {
                    Iterator<String> it = processedAcks.iterator();
                    if (it.hasNext()) { it.next(); it.remove(); }
                }

                log.success("ACK received for " + ack + " from " + sender.getShortId());

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

    private void checkMessageVisibility() {
        long now = System.currentTimeMillis();
        long timeout = 10_000;

        for (DiscoveredDevice dd : deviceCache.values()) {
            Set<String> toRemove = new HashSet<>();

            for (String msgId : dd.currentVisibleMsgIds) {
                Long lastSeen = activeIncomingMessages.get(msgId);
                if (lastSeen == null || now - lastSeen > timeout) {
                    toRemove.add(msgId);
                    dd.markReceivedMessageAckConfirmed(msgId);
                    pendingAcksToSend.remove(msgId);
                    log.success("ACK confirmed delivered for: " + msgId);
                }
            }

            dd.currentVisibleMsgIds.removeAll(toRemove);
            for (String msgId : toRemove) {
                activeIncomingMessages.remove(msgId);
            }
        }
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
        if (!isRunning || manager == null || channel == null) return;

        long now = System.currentTimeMillis();
        long sinceLast = now - lastDiscoverCall;
        if (lastDiscoverCall != 0 && sinceLast < P2pConfig.MIN_DISCOVER_GAP_MS) {
            log.d("Discovery throttled (" + reason + "), " + sinceLast + "ms since last");
            return;
        }
        lastDiscoverCall = now;
        discoveryInProgress = true;
        lastDiscoveryTime = now;
        log.d("Discovery requested: " + reason);

        // Сторож: если колбэк discoverServices не придёт (зависший supplicant,
        // перезагрузка Wi-Fi), цикл не должен остановиться навсегда.
        handler.removeCallbacks(discoveryWatchdog);
        handler.postDelayed(discoveryWatchdog, 20_000);

        manager.discoverPeers(channel, new WifiP2pManager.ActionListener() {
            @Override
            public void onSuccess() {
                log.d("discoverPeers started (" + reason + ")");
                handler.postDelayed(() -> discoverServicesWithRetry(reason, 0),
                        P2pConfig.DISCOVER_SERVICES_DELAY_MS);
            }

            @Override
            public void onFailure(int r) {
                log.w("discoverPeers failed: " + reasonToString(r) + " (" + reason + ")");
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
                notifyStatus("Devices: " + deviceCache.size() + " | TXT: " + txtRecordsReceived.get());
                scheduleNextDiscovery();
            }

            @Override
            public void onFailure(int r) {
                discoveryInProgress = false;
                log.w("discoverServices failed: " + reasonToString(r)
                        + " (" + reason + ", attempt " + attempt + ")");

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
        // ВАЖНО: dnsCacheResetRunnable (clearServiceRequests каждые 45 с) и
        // serviceReregisterRunnable (remove+add всех сервисов каждые 30 с)
        // намеренно НЕ планируются.
        //  - clearServiceRequests: пока запросы сняты, TXT не приходят —
        //    входящие сообщения и ACK теряются, supplicant отвечает BUSY;
        //  - remove+add всех сервисов: окно без advertise + риск потерять
        //    указатель на зарегистрированный сервис (станция «исчезает»).
        // Свежесть WFD_Main обеспечивает heartbeat (publishMainService каждые 5 с).
    }

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
                    dd.deviceId = parts[0];
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
                                dd.deviceId = extractedId;
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