package com.example.directtest;

import android.widget.EditText;
import android.view.inputmethod.InputMethodManager;
import android.Manifest;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.directtest.model.DiscoveredDevice;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class MainActivity extends AppCompatActivity implements DiscoveryService.ServiceCallback {

    private static final int PERMISSION_REQUEST_CODE = 1001;
    private static final int NOTIFICATION_PERMISSION_CODE = 1002;

    // Сервис
    private DiscoveryService discoveryService;
    private boolean serviceBound = false;

    // UI
    private DeviceAdapter adapter;
    private final List<DiscoveredDevice> devices = new ArrayList<>();
    private TextView tvStatus;
    private Button btnStop;

    /** D1: предупреждение о выключенной геолокации показываем один раз за запуск. */
    private boolean locationWarningShown = false;

    /** Диалог «Wi-Fi выключен» — один раз за запуск. */
    private boolean wifiDialogShown = false;

    // Периодическое обновление UI
    private final Handler uiHandler = new Handler(Looper.getMainLooper());
    private final Runnable uiUpdateRunnable = new Runnable() {
        @Override
        public void run() {
            if (adapter != null) {
                adapter.notifyDataSetChanged();
            }
            updateStatusBar();
            uiHandler.postDelayed(this, 1000);
        }
    };

    // ==================== SERVICE CONNECTION ====================

    private final ServiceConnection serviceConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder binder) {
            log("onServiceConnected");

            DiscoveryService.LocalBinder localBinder = (DiscoveryService.LocalBinder) binder;
            discoveryService = localBinder.getService();
            serviceBound = true;

            // Устанавливаем callback
            discoveryService.setServiceCallback(MainActivity.this);

            // Загружаем текущие устройства
            refreshDeviceList();

            // Обновляем UI
            updateStatusBar();
            updateStopButton();

            // Принудительный refresh - будит discovery после сна
            log("Triggering forceRefresh after bind");
            discoveryService.forceRefresh();

            Toast.makeText(MainActivity.this, "Service connected", Toast.LENGTH_SHORT).show();
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            log("onServiceDisconnected");
            discoveryService = null;
            serviceBound = false;
            updateStopButton();
        }
    };

    // ==================== LIFECYCLE ====================

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        log("onCreate");
        setContentView(R.layout.activity_main);

        initViews();
        checkAndRequestPermissions();
    }

    @Override
    protected void onStart() {
        super.onStart();
        log("onStart - binding to service");
        bindToServiceIfRunning();
    }

    @Override
    protected void onResume() {
        super.onResume();
        log("onResume - serviceBound=" + serviceBound);
        uiHandler.post(uiUpdateRunnable);

        if (serviceBound) {
            refreshDeviceList();
        }
    }

    @Override
    protected void onPause() {
        super.onPause();
        log("onPause");
        uiHandler.removeCallbacks(uiUpdateRunnable);
    }

    @Override
    protected void onStop() {
        super.onStop();
        log("onStop - unbinding, devices=" + devices.size());
        unbindFromService();
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        log("onDestroy");
        uiHandler.removeCallbacksAndMessages(null);
    }

    /**
     * Логирование с тегом Activity
     */
    private void log(String message) {
        DiagnosticLogger.getInstance().i("[MainActivity] " + message);
    }

    // ==================== INIT ====================

    private void initViews() {
        tvStatus = findViewById(R.id.tv_status);
        Button btnRefresh = findViewById(R.id.btn_refresh);
        Button btnClear = findViewById(R.id.btn_clear);
        Button btnLog = findViewById(R.id.btn_log);

        // Находим или создаём кнопку Stop
        // Если её нет в layout, можно использовать btnClear как toggle
        btnStop = findViewById(R.id.btn_stop);
        if (btnStop == null) {
            // Используем btnClear для остановки через долгое нажатие
            btnClear.setOnLongClickListener(v -> {
                stopDiscoveryService();
                return true;
            });
        }

        RecyclerView recyclerView = findViewById(R.id.recycler_devices);
        recyclerView.setLayoutManager(new LinearLayoutManager(this));
        adapter = new DeviceAdapter(devices, this::onDeviceClick);
        recyclerView.setAdapter(adapter);

        // Кнопка обновления
        btnRefresh.setOnClickListener(v -> {
            if (serviceBound && discoveryService != null) {
                discoveryService.forceRefresh();
                Toast.makeText(this, "Обновление...", Toast.LENGTH_SHORT).show();
            } else {
                // Сервис не запущен - запускаем
                startDiscoveryService();
            }
        });

        // Кнопка очистки
        btnClear.setOnClickListener(v -> {
            new AlertDialog.Builder(this)
                    .setTitle("Очистка")
                    .setMessage("Очистить все сообщения, подтверждения и логи?")
                    .setPositiveButton("Да", (d, w) -> {
                        if (serviceBound && discoveryService != null) {
                            discoveryService.clearAll();
                            adapter.notifyDataSetChanged();
                            Toast.makeText(this, "Очищено", Toast.LENGTH_SHORT).show();
                        }
                    })
                    .setNegativeButton("Нет", null)
                    .show();
        });

        // Кнопка логов
        btnLog.setOnClickListener(v -> startActivity(new Intent(this, DiagnosticActivity.class)));

        // Кнопка остановки (если есть)
        if (btnStop != null) {
            btnStop.setOnClickListener(v -> {
                if (serviceBound) {
                    stopDiscoveryService();
                } else {
                    startDiscoveryService();
                }
            });
        }
    }

    private void updateStopButton() {
        if (btnStop != null) {
            if (serviceBound) {
                btnStop.setText("Stop");
                btnStop.setEnabled(true);
            } else {
                btnStop.setText("Start");
                btnStop.setEnabled(true);
            }
        }
    }

    // ==================== PERMISSIONS ====================

    private void checkAndRequestPermissions() {
        List<String> perms = new ArrayList<>();

        // Location permission
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION)
                != PackageManager.PERMISSION_GRANTED) {
            perms.add(Manifest.permission.ACCESS_FINE_LOCATION);
        }

        // Nearby WiFi devices (Android 13+)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.NEARBY_WIFI_DEVICES)
                    != PackageManager.PERMISSION_GRANTED) {
                perms.add(Manifest.permission.NEARBY_WIFI_DEVICES);
            }
        }

        if (perms.isEmpty()) {
            // Основные разрешения есть - проверяем уведомления
            checkNotificationPermission();
        } else {
            ActivityCompat.requestPermissions(this, perms.toArray(new String[0]), PERMISSION_REQUEST_CODE);
        }
    }

    private void checkNotificationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS)
                    != PackageManager.PERMISSION_GRANTED) {
                // Запрашиваем разрешение на уведомления
                ActivityCompat.requestPermissions(this,
                        new String[]{Manifest.permission.POST_NOTIFICATIONS},
                        NOTIFICATION_PERMISSION_CODE);
                return;
            }
        }
        // Все разрешения есть - запускаем сервис
        startDiscoveryService();
        offerBatteryOptimizationExemption();
    }

    /**
     * На Xiaomi/Huawei/Samsung/Oppo агрессивный менеджер батареи убивает
     * foreground service через несколько минут — «рация» замолкает именно на
     * этих телефонах. Просим пользователя добавить приложение в исключения.
     */
    private void offerBatteryOptimizationExemption() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return;

        android.os.PowerManager pm =
                (android.os.PowerManager) getSystemService(Context.POWER_SERVICE);
        if (pm == null || pm.isIgnoringBatteryOptimizations(getPackageName())) return;

        new AlertDialog.Builder(this)
                .setTitle("Стабильная работа в фоне")
                .setMessage("Чтобы приём не прекращался при погашенном экране, "
                        + "разрешите приложению работать без оптимизации батареи.")
                .setPositiveButton("Разрешить", (d, w) -> {
                    try {
                        Intent intent = new Intent(
                                android.provider.Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS);
                        intent.setData(android.net.Uri.parse("package:" + getPackageName()));
                        startActivity(intent);
                    } catch (Exception e) {
                        log("Battery optimization intent failed: " + e.getMessage());
                    }
                })
                .setNegativeButton("Позже", null)
                .show();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] results) {
        super.onRequestPermissionsResult(requestCode, permissions, results);

        if (requestCode == PERMISSION_REQUEST_CODE) {
            boolean allGranted = true;
            for (int r : results) {
                if (r != PackageManager.PERMISSION_GRANTED) {
                    allGranted = false;
                    break;
                }
            }

            if (allGranted) {
                checkNotificationPermission();
            } else {
                tvStatus.setText("Permissions required");
                Toast.makeText(this, "Требуются разрешения для работы", Toast.LENGTH_LONG).show();
            }
        } else if (requestCode == NOTIFICATION_PERMISSION_CODE) {
            // Даже если отказали - запускаем сервис (уведомление может не показаться)
            startDiscoveryService();
        }
    }

    // ==================== SERVICE MANAGEMENT ====================

    /**
     * D1: проверка Location Mode перед стартом.
     *
     * <p>Разрешения могут быть выданы, но если режим геолокации выключен,
     * Android не отдаёт список Wi-Fi Direct устройств: приложение «не видит»
     * второй телефон и никакой ошибки не показывает. На MIUI/Funtouch это
     * самая частая причина. Предупреждаем и даём кнопку в настройки.
     */
    private void ensureLocationModeBeforeStart() {
        if (!P2pConfig.REQUIRE_LOCATION_FOR_DISCOVERY) return;
        if (com.example.directtest.fast.HwGuard.isLocationEnabled(this)) return;
        if (locationWarningShown) return;
        locationWarningShown = true;

        new AlertDialog.Builder(this)
                .setTitle("Включите геолокацию")
                .setMessage("Android не отдаёт список Wi-Fi Direct устройств, пока "
                        + "выключен режим определения местоположения — даже если "
                        + "разрешения выданы. Рация не найдёт второй телефон.\n\n"
                        + "Включите «Геолокацию» в системных настройках.")
                .setPositiveButton("Открыть настройки", (d, w) -> {
                    try {
                        com.example.directtest.fast.HwGuard.openLocationSettings(this);
                    } catch (Exception ignored) {
                    }
                })
                .setNegativeButton("Позже", null)
                .show();
    }

    private void startDiscoveryService() {
        ensureLocationModeBeforeStart();

        tvStatus.setText("Starting service...");

        Intent intent = new Intent(this, DiscoveryService.class);

        // Запускаем как foreground service
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(intent);
        } else {
            startService(intent);
        }

        // Привязываемся
        bindToService();
    }

    private void stopDiscoveryService() {
        // Отвязываемся
        unbindFromService();

        // Останавливаем сервис
        Intent intent = new Intent(this, DiscoveryService.class);
        stopService(intent);

        // Очищаем UI
        devices.clear();
        adapter.notifyDataSetChanged();
        tvStatus.setText("Service stopped");

        Toast.makeText(this, "Service stopped", Toast.LENGTH_SHORT).show();
    }

    private void bindToService() {
        Intent intent = new Intent(this, DiscoveryService.class);
        bindService(intent, serviceConnection, Context.BIND_AUTO_CREATE);
    }

    private void bindToServiceIfRunning() {
        // Пытаемся привязаться к уже запущенному сервису
        Intent intent = new Intent(this, DiscoveryService.class);
        bindService(intent, serviceConnection, 0); // Без BIND_AUTO_CREATE - не создаём если нет
    }

    private void unbindFromService() {
        if (serviceBound) {
            if (discoveryService != null) {
                discoveryService.removeServiceCallback();
            }
            try {
                unbindService(serviceConnection);
            } catch (Exception e) {
                // Ignore
            }
            serviceBound = false;
            discoveryService = null;
        }
    }

    // ==================== UI UPDATES ====================

    private void refreshDeviceList() {
        if (discoveryService != null) {
            List<DiscoveredDevice> serviceDevices = discoveryService.getAllDevices();
            devices.clear();
            devices.addAll(serviceDevices);
            adapter.notifyDataSetChanged();
        }
    }

    private void updateStatusBar() {
        if (!serviceBound || discoveryService == null) {
            tvStatus.setText("Service not running\nTap REFRESH to start");
            return;
        }

        int total = devices.size();
        int online = 0, withApp = 0;
        for (DiscoveredDevice d : devices) {
            if (d.isOnline()) online++;
            if (d.hasOurApp) withApp++;
        }

        tvStatus.setText(String.format(Locale.getDefault(),
                "ID:%s SID:%s | Dev:%d On:%d App:%d\nHB#%d | TXT:%d | ACK:%d",
                discoveryService.getShortDeviceId(),
                discoveryService.getSessionId(),
                total, online, withApp,
                discoveryService.getHeartbeatSeq(),
                discoveryService.getTxtRecordsReceived(),
                discoveryService.getPendingAcksCount()));
    }

    private void updateDevice(DiscoveredDevice device) {
        int idx = -1;
        for (int i = 0; i < devices.size(); i++) {
            if (java.util.Objects.equals(devices.get(i).address, device.address)) {
                idx = i;
                break;
            }
        }
        if (idx >= 0) {
            devices.set(idx, device);
        } else {
            devices.add(device);
        }
        adapter.notifyDataSetChanged();
    }

    // ==================== DEVICE CLICK ====================

    private void onDeviceClick(DiscoveredDevice device) {
        if (!serviceBound || discoveryService == null) {
            Toast.makeText(this, "Сервис не запущен", Toast.LENGTH_SHORT).show();
            return;
        }

        // hasOurApp ставится по маркеру в имени (рефлексия setDeviceName, работает
        // не на всех прошивках) или по факту получения TXT-записи. Если deviceId
        // уже известен — приложение точно есть, отправлять можно.
        if (!device.hasOurApp && device.deviceId == null) {
            Toast.makeText(this, "На устройстве не обнаружено приложение", Toast.LENGTH_SHORT).show();
            return;
        }

        if (!device.isOnline()) {
            Toast.makeText(this, "Устройство оффлайн", Toast.LENGTH_SHORT).show();
            return;
        }

        // ДОБАВЛЕНО: Проверка лимита на стороне UI
        int pendingCount = device.getPendingSentMessagesCount();
        if (pendingCount >= P2pConfig.MAX_MSG_SLOTS) {
            Toast.makeText(this,
                    "Лимит: " + P2pConfig.MAX_MSG_SLOTS + " неподтверждённых сообщения.\nДождитесь подтверждения.",
                    Toast.LENGTH_LONG).show();
            return;
        }

        // ИЗМЕНЕНО: Диалог ввода сообщения вместо автогенерации
        showMessageInputDialog(device);
    }

    /**
     * Показать диалог для ввода сообщения
     */
    private void showMessageInputDialog(DiscoveredDevice device) {
        EditText input = new EditText(this);
        input.setHint("Введите сообщение");
        input.setSingleLine(false);
        input.setMaxLines(3);

        // Добавляем отступы
        int padding = (int) (16 * getResources().getDisplayMetrics().density);
        input.setPadding(padding, padding, padding, padding);

        new AlertDialog.Builder(this)
                .setTitle("Сообщение для " + device.getShortId())
                .setMessage("Устройство: " + (device.name != null ? device.name : "Unknown"))
                .setView(input)
                .setPositiveButton("Отправить", (dialog, which) -> {
                    String text = input.getText().toString().trim();
                    if (text.isEmpty()) {
                        Toast.makeText(this, "Сообщение не может быть пустым", Toast.LENGTH_SHORT).show();
                        return;
                    }

                    String msgId = discoveryService.sendMessage(text, device.deviceId);
                    if (msgId != null) {
                        Toast.makeText(this, "→ " + device.getShortId() + "\n" + msgId, Toast.LENGTH_SHORT).show();
                    }
                    // Ошибка показывается через onError callback
                })
                .setNegativeButton("Отмена", null)
                .show();

        // Автоматически показать клавиатуру
        input.requestFocus();
        input.postDelayed(() -> {
            InputMethodManager imm = (InputMethodManager) getSystemService(Context.INPUT_METHOD_SERVICE);
            if (imm != null) {
                imm.showSoftInput(input, InputMethodManager.SHOW_IMPLICIT);
            }
        }, 200);
    }

    // ==================== SERVICE CALLBACK ====================

    @Override
    public void onDeviceFound(DiscoveredDevice device) {
        updateDevice(device);
    }

    @Override
    public void onDeviceUpdated(DiscoveredDevice device) {
        updateDevice(device);
    }

    @Override
    public void onDeviceLost(DiscoveredDevice device) {
        devices.remove(device);
        adapter.notifyDataSetChanged();
    }

    @Override
    public void onMessageSent(String messageId, String message, String targetDeviceId) {
        Toast.makeText(this, "→ " + messageId, Toast.LENGTH_SHORT).show();
        adapter.notifyDataSetChanged();
    }

    @Override
    public void onMessageReceived(DiscoveredDevice device, String messageId, String message) {
        Toast.makeText(this, "← " + device.getShortId() + ": " + message, Toast.LENGTH_LONG).show();
        adapter.notifyDataSetChanged();
    }

    @Override
    public void onAckReceived(DiscoveredDevice device, String ackedMessageId) {
        Toast.makeText(this, "✓ " + ackedMessageId, Toast.LENGTH_SHORT).show();
        adapter.notifyDataSetChanged();
    }

    @Override
    public void onError(String message) {
        Toast.makeText(this, message, Toast.LENGTH_SHORT).show();

        // Wi-Fi выключен — это единственная ошибка, которую пользователь может
        // исправить одним нажатием. Показываем диалог с переходом в настройки,
        // но не чаще одного раза за запуск (иначе он перекроет экран).
        if (!wifiDialogShown && message != null
                && (message.contains("Wi-Fi") || message.toLowerCase(Locale.US).contains("wifi"))) {
            wifiDialogShown = true;
            new AlertDialog.Builder(this)
                    .setTitle("Wi-Fi выключен")
                    .setMessage("Wi-Fi Direct работает только при включённом Wi-Fi. "
                            + "Включите Wi-Fi — поиск и приём сообщений возобновятся автоматически.")
                    .setPositiveButton("Открыть настройки", (d, w) -> {
                        try {
                            com.example.directtest.fast.HwGuard.openWifiSettings(this);
                        } catch (Exception ignored) {
                        }
                    })
                    .setNegativeButton("Позже", null)
                    .show();
        }
    }

    // ==================== ADAPTER ====================

    static class DeviceAdapter extends RecyclerView.Adapter<DeviceAdapter.VH> {

        private final List<DiscoveredDevice> list;
        private final OnClick listener;

        interface OnClick {
            void onClick(DiscoveredDevice d);
        }

        DeviceAdapter(List<DiscoveredDevice> list, OnClick l) {
            this.list = list;
            this.listener = l;
        }

        @NonNull
        @Override
        public VH onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
            View v = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_device, parent, false);
            return new VH(v);
        }

        @Override
        public void onBindViewHolder(@NonNull VH h, int position) {
            DiscoveredDevice d = list.get(position);
            long now = System.currentTimeMillis();

            // Header
            h.tvName.setText(d.name != null ? d.name : "Unknown");
            h.tvStatus.setText(d.isOnline() ? "● ONLINE" : "○ OFFLINE");
            h.tvStatus.setTextColor(d.isOnline() ? 0xFF4CAF50 : 0xFF9E9E9E);
            h.tvName.setTextColor(d.isOnline() ? 0xFF2E7D32 : 0xFF757575);

            // ID
            String id = d.deviceId != null ? d.deviceId.substring(0, Math.min(8, d.deviceId.length())) : "?";
            h.tvId.setText(String.format("ID: %s | MAC: %s", id, d.address));

            // Session ID
            if (h.tvSessionId != null) {
                if (d.sessionId != null) {
                    h.tvSessionId.setVisibility(View.VISIBLE);
                    h.tvSessionId.setText(String.format("SID: %s", d.sessionId));
                } else {
                    h.tvSessionId.setVisibility(View.GONE);
                }
            }

            // App status
            h.tvApp.setText(String.format("%s | Seen: %dx | %ds ago",
                    d.hasOurApp ? "✅App" : "❌NoApp", d.seenCount, (now - d.lastSeen) / 1000));

            // Heartbeat
            if (d.lastHeartbeatReceived > 0) {
                h.tvHb.setText(String.format(Locale.getDefault(), "💓 HB#%d←#%d (%ds)",
                        d.heartbeatSeq, d.prevHeartbeatSeq, (now - d.lastHeartbeatReceived) / 1000));
            } else {
                h.tvHb.setText("💓 No HB");
            }

            // RECEIVED messages + ACK
            List<DiscoveredDevice.ReceivedMessage> recv = d.getLastReceivedMessages(3);
            bindReceivedMessage(h.tvRecv1, h.tvRecv1Ack, recv.size() > 0 ? recv.get(0) : null, now);
            bindReceivedMessage(h.tvRecv2, h.tvRecv2Ack, recv.size() > 1 ? recv.get(1) : null, now);
            bindReceivedMessage(h.tvRecv3, h.tvRecv3Ack, recv.size() > 2 ? recv.get(2) : null, now);

            // SENT messages + ACK
            List<DiscoveredDevice.SentMessage> sent = d.getLastSentMessages(3);
            bindSentMessage(h.tvSent1, h.tvSent1Ack, sent.size() > 0 ? sent.get(0) : null, now);
            bindSentMessage(h.tvSent2, h.tvSent2Ack, sent.size() > 1 ? sent.get(1) : null, now);
            bindSentMessage(h.tvSent3, h.tvSent3Ack, sent.size() > 2 ? sent.get(2) : null, now);

            // Service info
            h.tvService.setText(String.format("📡 Svc: %s | Slot: %s | Visible: %d | Pending: %d",
                    d.lastServiceName != null ? d.lastServiceName : "-",
                    d.lastSlotIndex >= 0 ? String.valueOf(d.lastSlotIndex) : "-",
                    d.currentVisibleMsgIds.size(),
                    d.getPendingAckMessageIds().size()));

            h.itemView.setOnClickListener(v -> listener.onClick(d));
            h.itemView.setBackgroundColor(d.hasOurApp && d.isOnline() ? 0x0800FF00 : 0x00000000);
        }

        private void bindReceivedMessage(TextView tvMsg, TextView tvAck,
                                         DiscoveredDevice.ReceivedMessage m, long now) {
            if (m == null) {
                tvMsg.setVisibility(View.GONE);
                tvAck.setVisibility(View.GONE);
                return;
            }
            tvMsg.setVisibility(View.VISIBLE);
            tvAck.setVisibility(View.VISIBLE);

            long ago = (now - m.receivedAt) / 1000;
            tvMsg.setText(String.format("↓ [%s] \"%s\" (%ds)", m.messageId, truncate(m.text, 20), ago));

            if (m.ackConfirmed) {
                tvAck.setText(String.format("  ↪ ACK ✓ delivered (sent %dx)", m.ackSendCount));
                tvAck.setTextColor(0xFF4CAF50);
            } else if (m.ackSent) {
                long ackAgo = (now - m.ackSentAt) / 1000;
                tvAck.setText(String.format("  ↪ ACK sending... %dx (%ds)", m.ackSendCount, ackAgo));
                tvAck.setTextColor(0xFFFF9800);
            } else {
                tvAck.setText("  ↪ ACK pending");
                tvAck.setTextColor(0xFF9E9E9E);
            }
        }

        private void bindSentMessage(TextView tvMsg, TextView tvAck,
                                     DiscoveredDevice.SentMessage m, long now) {
            if (m == null) {
                tvMsg.setVisibility(View.GONE);
                tvAck.setVisibility(View.GONE);
                return;
            }
            tvMsg.setVisibility(View.VISIBLE);
            tvAck.setVisibility(View.VISIBLE);

            long ago = (now - m.sentAt) / 1000;
            String ackIcon = m.acknowledged ? "✓" : "⏳";
            tvMsg.setText(String.format("↑ %s [%s] \"%s\" (%ds) S:%d",
                    ackIcon, m.messageId, truncate(m.text, 15), ago, m.slotIndex));
            tvMsg.setTextColor(m.acknowledged ? 0xFF4CAF50 : 0xFFFF9800);

            if (m.acknowledged) {
                long ackAgo = (now - m.ackReceivedAt) / 1000;
                String batch = m.ackBatch.isEmpty() ? "" : " (+" + String.join(",", m.ackBatch) + ")";
                tvAck.setText(String.format("  ↩ ACK received (%ds)%s", ackAgo, batch));
                tvAck.setTextColor(0xFF2196F3);
            } else {
                tvAck.setText("  ↩ waiting ACK...");
                tvAck.setTextColor(0xFF9E9E9E);
            }
        }

        private String truncate(String s, int max) {
            if (s == null) return "";
            return s.length() <= max ? s : s.substring(0, max) + "…";
        }

        @Override
        public int getItemCount() {
            return list.size();
        }

        static class VH extends RecyclerView.ViewHolder {
            TextView tvName, tvStatus, tvId, tvSessionId, tvApp, tvHb;
            TextView tvRecv1, tvRecv1Ack, tvRecv2, tvRecv2Ack, tvRecv3, tvRecv3Ack;
            TextView tvSent1, tvSent1Ack, tvSent2, tvSent2Ack, tvSent3, tvSent3Ack;
            TextView tvService;

            VH(View v) {
                super(v);
                tvName = v.findViewById(R.id.tv_device_name);
                tvStatus = v.findViewById(R.id.tv_status_indicator);
                tvId = v.findViewById(R.id.tv_device_id);
                tvSessionId = v.findViewById(R.id.tv_session_id);
                tvApp = v.findViewById(R.id.tv_app_status);
                tvHb = v.findViewById(R.id.tv_heartbeat_info);

                tvRecv1 = v.findViewById(R.id.tv_received_1);
                tvRecv1Ack = v.findViewById(R.id.tv_received_1_ack);
                tvRecv2 = v.findViewById(R.id.tv_received_2);
                tvRecv2Ack = v.findViewById(R.id.tv_received_2_ack);
                tvRecv3 = v.findViewById(R.id.tv_received_3);
                tvRecv3Ack = v.findViewById(R.id.tv_received_3_ack);

                tvSent1 = v.findViewById(R.id.tv_sent_1);
                tvSent1Ack = v.findViewById(R.id.tv_sent_1_ack);
                tvSent2 = v.findViewById(R.id.tv_sent_2);
                tvSent2Ack = v.findViewById(R.id.tv_sent_2_ack);
                tvSent3 = v.findViewById(R.id.tv_sent_3);
                tvSent3Ack = v.findViewById(R.id.tv_sent_3_ack);

                tvService = v.findViewById(R.id.tv_service_info);
            }
        }
    }
}