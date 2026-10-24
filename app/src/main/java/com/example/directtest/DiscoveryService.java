package com.example.directtest;

import android.app.Service;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.os.Binder;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;

import androidx.annotation.Nullable;


import com.example.directtest.model.DiscoveredDevice;

import java.util.List;

public class DiscoveryService extends Service implements DiscoveryListener {

    private static final String TAG = "DiscoveryService";

    // Компоненты
    private FastDiscoveryManager discoveryManager;
    private NotificationHelper notificationHelper;
    private final Handler handler = new Handler(Looper.getMainLooper());

    // Binder для связи с Activity
    private final IBinder binder = new LocalBinder();

    // Callback для Activity
    private ServiceCallback serviceCallback;

    // Периодическое обновление уведомления
    private final Runnable statusUpdateRunnable = new Runnable() {
        @Override
        public void run() {
            updateNotificationStatus();
            handler.postDelayed(this, 3000); // Каждые 3 секунды
        }
    };

    // ==================== BINDER ====================

    public class LocalBinder extends Binder {
        public DiscoveryService getService() {
            return DiscoveryService.this;
        }
    }

    public interface ServiceCallback {
        void onDeviceFound(DiscoveredDevice device);
        void onDeviceUpdated(DiscoveredDevice device);
        void onDeviceLost(DiscoveredDevice device);
        void onMessageSent(String messageId, String message, String targetDeviceId);
        void onMessageReceived(DiscoveredDevice device, String messageId, String message);
        void onAckReceived(DiscoveredDevice device, String ackedMessageId);
        void onError(String message);
    }

    // ==================== LIFECYCLE ====================

    @Override
    public void onCreate() {
        super.onCreate();
        log("onCreate - initializing service");

        notificationHelper = new NotificationHelper(this);
        discoveryManager = new FastDiscoveryManager(this);

        startForegroundService();
        discoveryManager.start(this);

        handler.postDelayed(statusUpdateRunnable, 1000);

        log("Service created and discovery started");
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        String action = intent != null ? intent.getAction() : "null";
        log("onStartCommand - action=" + action + " flags=" + flags);

        if (intent != null && NotificationHelper.ACTION_STOP_SERVICE.equals(intent.getAction())) {
            log("Stop command received");
            stopSelf();
            return START_NOT_STICKY;
        }

        return START_STICKY;
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        log("onBind - client connecting");
        return binder;
    }

    @Override
    public boolean onUnbind(Intent intent) {
        log("onUnbind - client disconnected, service continues");
        return super.onUnbind(intent);
    }

    @Override
    public void onDestroy() {
        log("onDestroy - stopping service");

        handler.removeCallbacksAndMessages(null);

        if (discoveryManager != null) {
            discoveryManager.stop();
            discoveryManager = null;
        }

        super.onDestroy();
        log("Service destroyed");
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        log("onTaskRemoved - app swiped from recents, keeping radio alive");

        // Сервис не должен умирать вместе с задачей: START_STICKY вернёт его,
        // но лучше явно перезапустить обновление уведомления после возможного
        // пересоздания процесса.
        handler.removeCallbacks(statusUpdateRunnable);
        handler.postDelayed(statusUpdateRunnable, 1000);

        super.onTaskRemoved(rootIntent);
    }

    @Override
    public void onTrimMemory(int level) {
        super.onTrimMemory(level);
        log("onTrimMemory - level=" + level);
    }



    // ==================== FOREGROUND SERVICE ====================

    private void startForegroundService() {
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(
                        NotificationHelper.NOTIFICATION_ID,
                        notificationHelper.buildForegroundNotification(),
                        ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE
                );
            } else {
                startForeground(
                        NotificationHelper.NOTIFICATION_ID,
                        notificationHelper.buildForegroundNotification()
                );
            }
            log("Foreground service started");
        } catch (Exception e) {
            log("Error starting foreground: " + e.getMessage());
        }
    }

    private void updateNotificationStatus() {
        if (discoveryManager == null || notificationHelper == null) return;

        int total = discoveryManager.getDeviceCount();
        int onlineWithApp = countOnlineWithApp();

        notificationHelper.updateStatus(total, onlineWithApp);
    }

    private int countOnlineWithApp() {
        if (discoveryManager == null) return 0;

        int count = 0;
        for (DiscoveredDevice device : discoveryManager.getAllDevices()) {
            if (device.isOnline() && device.hasOurApp) {
                count++;
            }
        }
        return count;
    }

    // ==================== PUBLIC API (для Activity) ====================

    /**
     * Получить FastDiscoveryManager
     */
    public FastDiscoveryManager getDiscoveryManager() {
        return discoveryManager;
    }

    /**
     * Установить callback для Activity
     */
    public void setServiceCallback(ServiceCallback callback) {
        this.serviceCallback = callback;
    }

    /**
     * Удалить callback
     */
    public void removeServiceCallback() {
        this.serviceCallback = null;
    }

    /**
     * Получить список устройств
     */
    public List<DiscoveredDevice> getAllDevices() {
        if (discoveryManager != null) {
            return discoveryManager.getAllDevices();
        }
        return java.util.Collections.emptyList();
    }

    /**
     * Отправить сообщение
     */
    public String sendMessage(String message, String targetDeviceId) {
        if (discoveryManager != null) {
            return discoveryManager.sendMessage(message, targetDeviceId);
        }
        return null;
    }

    /**
     * Принудительное обновление
     */
    public void forceRefresh() {
        if (discoveryManager != null) {
            discoveryManager.forceRefresh();
        }
    }

    /**
     * Очистить всё
     */
    public void clearAll() {
        if (discoveryManager != null) {
            discoveryManager.clearAll();
        }
    }

    /**
     * Проверить, работает ли сервис
     */
    public boolean isRunning() {
        return discoveryManager != null && discoveryManager.isRunning();
    }

    /**
     * Получить ID устройства
     */
    public String getShortDeviceId() {
        return discoveryManager != null ? discoveryManager.getShortDeviceId() : "???";
    }

    /**
     * Получить Session ID
     */
    public String getSessionId() {
        return discoveryManager != null ? discoveryManager.getSessionId() : "???";
    }

    /**
     * Получить счётчик heartbeat
     */
    public long getHeartbeatSeq() {
        return discoveryManager != null ? discoveryManager.getHeartbeatSeq() : 0;
    }

    /**
     * Получить количество TXT записей
     */
    public int getTxtRecordsReceived() {
        return discoveryManager != null ? discoveryManager.getTxtRecordsReceived() : 0;
    }

    /**
     * Получить количество pending ACK
     */
    public int getPendingAcksCount() {
        return discoveryManager != null ? discoveryManager.getPendingAcksCount() : 0;
    }

    // ==================== DISCOVERY LISTENER ====================

    @Override
    public void onDeviceFound(DiscoveredDevice device) {
        log("Device found: " + device.getShortId());
        updateNotificationStatus();

        if (serviceCallback != null) {
            handler.post(() -> serviceCallback.onDeviceFound(device));
        }
    }

    @Override
    public void onDeviceUpdated(DiscoveredDevice device) {
        updateNotificationStatus();

        if (serviceCallback != null) {
            handler.post(() -> serviceCallback.onDeviceUpdated(device));
        }
    }

    @Override
    public void onDeviceLost(DiscoveredDevice device) {
        log("Device lost: " + device.getShortId());
        updateNotificationStatus();

        if (serviceCallback != null) {
            handler.post(() -> serviceCallback.onDeviceLost(device));
        }
    }

    @Override
    public void onDeviceOnlineStatusChanged(DiscoveredDevice device, boolean isOnline) {
        log("Device " + device.getShortId() + " online: " + isOnline);
        updateNotificationStatus();
    }

    @Override
    public void onStatusChanged(String status) {
        log("Status: " + status);
    }

    @Override
    public void onError(String message) {
        log("Error: " + message);

        if (serviceCallback != null) {
            handler.post(() -> serviceCallback.onError(message));
        }
    }

    @Override
    public void onMessageSent(String messageId, String message, String targetDeviceId) {
        log("Message sent: " + messageId);

        if (serviceCallback != null) {
            handler.post(() -> serviceCallback.onMessageSent(messageId, message, targetDeviceId));
        }
    }

    @Override
    public void onMessageReceived(DiscoveredDevice device, String messageId, String message) {
        log("Message received: " + messageId + " from " + device.getShortId());

        if (serviceCallback != null) {
            handler.post(() -> serviceCallback.onMessageReceived(device, messageId, message));
        }
    }

    @Override
    public void onAckReceived(DiscoveredDevice device, String ackedMessageId) {
        log("ACK received: " + ackedMessageId);

        if (serviceCallback != null) {
            handler.post(() -> serviceCallback.onAckReceived(device, ackedMessageId));
        }
    }

    // ==================== LOGGING ====================

    private void log(String message) {
        DiagnosticLogger.getInstance().i("[Service] " + message);
    }
}