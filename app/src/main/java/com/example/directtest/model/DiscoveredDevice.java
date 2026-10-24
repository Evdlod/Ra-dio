package com.example.directtest.model;

import android.net.wifi.p2p.WifiP2pDevice;

import com.example.directtest.P2pConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Информация об обнаруженном устройстве.
 * Хранит состояние устройства, историю сообщений и метаданные.
 */
public class DiscoveredDevice {

    // ==================== ИДЕНТИФИКАЦИЯ ====================

    /** MAC-адрес устройства */
    public String address;

    /** Уникальный ID приложения на устройстве */
    public String deviceId;

    /** Имя устройства */
    public String name;

    /** Объект WifiP2pDevice от системы */
    public WifiP2pDevice device;

    /** Флаг: на устройстве установлено наше приложение */
    public boolean hasOurApp;

    // ==================== ВРЕМЯ ====================

    /** Время первого обнаружения (мс) */
    public long firstSeen;

    /** Время последнего обнаружения (мс) */
    public long lastSeen;

    /** Количество обнаружений */
    public int seenCount;

    // ==================== HEARTBEAT ====================

    /** Текущий номер heartbeat */
    public long heartbeatSeq;

    /** Предыдущий номер heartbeat */
    public long prevHeartbeatSeq;

    /** Время получения последнего heartbeat (мс) */
    public long lastHeartbeatReceived;

    // ==================== SESSION ====================

    /** Session ID устройства (timestamp-based hex) */
    public String sessionId;

    /**
     * Поправка часов собеседника относительно наших (мс).
     * peerTime ≈ ourTime + clockSkewMs.
     * Нужна потому, что все timestamp в TXT-записях ставятся часами отправителя,
     * а на части устройств часы уходят на минуты — без поправки фильтр
     * «возраста сообщения» отбрасывает все сообщения такого устройства.
     */
    public long clockSkewMs = 0;

    /** Известна ли поправка часов (получен хотя бы один heartbeat с полем t) */
    public boolean hasClockSkew = false;

    /** Флаг для отслеживания перехода online */
    private boolean wasOnlineLastCheck;

    // ==================== ИСТОРИЯ СООБЩЕНИЙ ====================

    /** Отправленные сообщения */
    public List<SentMessage> sentMessages = new ArrayList<>();

    /** Полученные сообщения */
    public List<ReceivedMessage> receivedMessages = new ArrayList<>();

    /** Максимальное количество сообщений в истории */
    public static final int MAX_MESSAGES_HISTORY = 10;

    // ==================== СЕРВИС ====================

    /** Имя последнего обнаруженного сервиса */
    public String lastServiceName;

    /** Индекс последнего использованного слота */
    public int lastSlotIndex = -1;

    /** ID сообщений, видимых в текущий момент */
    public Set<String> currentVisibleMsgIds = new HashSet<>();

    // ==================== ВЛОЖЕННЫЕ КЛАССЫ ====================

    /**
     * Информация об отправленном сообщении
     */
    public static class SentMessage {
        public String messageId;
        public String text;
        public long sentAt;
        public int slotIndex;
        public boolean acknowledged;
        public long ackReceivedAt;
        public List<String> ackBatch = new ArrayList<>();

        /** Конструктор по умолчанию для загрузки из состояния */
        public SentMessage() {
        }

        public SentMessage(String id, String text, int slot) {
            this.messageId = id;
            this.text = text;
            this.slotIndex = slot;
            this.sentAt = System.currentTimeMillis();
        }
    }

    /**
     * Информация о полученном сообщении
     */
    public static class ReceivedMessage {
        public String messageId;
        public String text;
        public long receivedAt;
        public boolean ackSent;
        public long ackSentAt;
        public int ackSendCount;
        public boolean ackConfirmed;

        /** Конструктор по умолчанию для загрузки из состояния */
        public ReceivedMessage() {
        }

        public ReceivedMessage(String id, String text) {
            this.messageId = id;
            this.text = text;
            this.receivedAt = System.currentTimeMillis();
        }
    }

    // ==================== СТАТУС ====================

    /**
     * Проверить, онлайн ли устройство
     * @return true если устройство отвечало недавно
     */
    public boolean isOnline() {
        return System.currentTimeMillis() - lastSeen < P2pConfig.DEVICE_ONLINE_THRESHOLD;
    }

    /**
     * Проверить и обновить переход в онлайн.
     * @return true если устройство только что стало онлайн (было оффлайн)
     */
    public boolean checkAndUpdateOnlineTransition() {
        boolean currentlyOnline = isOnline();
        boolean justCameOnline = !wasOnlineLastCheck && currentlyOnline;
        wasOnlineLastCheck = currentlyOnline;
        return justCameOnline;
    }

    /**
     * Получить короткий ID устройства для отображения
     * @return первые 8 символов deviceId или часть MAC-адреса
     */
    public String getShortId() {
        if (deviceId != null && deviceId.length() >= 8) {
            return deviceId.substring(0, 8);
        }
        if (address != null) {
            String clean = address.replace(":", "");
            return clean.substring(0, Math.min(12, clean.length()));
        }
        return "unknown";
    }

    // ==================== HEARTBEAT ====================

    /**
     * Обновить информацию о heartbeat
     * @param newSeq новый номер последовательности
     */
    public void updateHeartbeat(long newSeq) {
        if (newSeq != heartbeatSeq) {
            prevHeartbeatSeq = heartbeatSeq;
            heartbeatSeq = newSeq;
            lastHeartbeatReceived = System.currentTimeMillis();
        }
    }

    /**
     * Обновить поправку часов по timestamp из свежей записи собеседника
     * (heartbeat/ACK-сервис публикуются каждые 2-5 секунд, поэтому t ≈ «сейчас»).
     *
     * @param peerTimeMs время собеседника в миллисекундах
     */
    public void updateClockSkew(long peerTimeMs) {
        if (peerTimeMs <= 0) return;

        long skew = peerTimeMs - System.currentTimeMillis();

        // Отбрасываем неправдоподобные значения (битый timestamp, чужой формат)
        if (Math.abs(skew) > P2pConfig.CLOCK_SKEW_LIMIT_MS) {
            return;
        }

        // Сглаживаем: одна «странная» запись не должна ломать поправку
        if (!hasClockSkew) {
            clockSkewMs = skew;
            hasClockSkew = true;
        } else {
            clockSkewMs = (clockSkewMs * 3 + skew) / 4;
        }
    }

    /**
     * Время собеседника, соответствующее нашему «сейчас»
     */
    public long peerNowMs() {
        return System.currentTimeMillis() + clockSkewMs;
    }

    // ==================== ОТПРАВЛЕННЫЕ СООБЩЕНИЯ ====================

    /**
     * Добавить отправленное сообщение в историю
     */
    public void addSentMessage(String msgId, String text, int slot) {
        SentMessage msg = new SentMessage(msgId, text, slot);
        sentMessages.add(0, msg);
        trimSentMessages();
        lastSlotIndex = slot;
    }

    /**
     * Добавить отправленное сообщение из сохранённого состояния
     */
    public void addSentMessageFromState(String msgId, String text, long sentAt, boolean acked, long ackTime) {
        // Проверка на дубликат
        for (SentMessage sm : sentMessages) {
            if (sm.messageId.equals(msgId)) {
                return;
            }
        }

        SentMessage sm = new SentMessage();
        sm.messageId = msgId;
        sm.text = text;
        sm.sentAt = sentAt;
        sm.acknowledged = acked;
        sm.ackReceivedAt = ackTime;
        sm.slotIndex = -1;  // Неизвестен после перезапуска

        sentMessages.add(sm);
        trimSentMessages();
    }

    /**
     * Пометить отправленное сообщение как подтверждённое
     */
    public void markSentMessageAcked(String msgId, List<String> batchAcks) {
        for (SentMessage msg : sentMessages) {
            if (msg.messageId.equals(msgId)) {
                msg.acknowledged = true;
                msg.ackReceivedAt = System.currentTimeMillis();
                if (batchAcks != null) {
                    for (String ack : batchAcks) {
                        if (!ack.equals(msgId) && !msg.ackBatch.contains(ack)) {
                            msg.ackBatch.add(ack);
                        }
                    }
                }
                break;
            }
        }
    }

    /**
     * Получить последние отправленные сообщения
     */
    public List<SentMessage> getLastSentMessages(int count) {
        List<SentMessage> result = new ArrayList<>();
        for (int i = 0; i < Math.min(count, sentMessages.size()); i++) {
            result.add(sentMessages.get(i));
        }
        return result;
    }

    /**
     * Получить количество неподтверждённых отправленных сообщений
     */
    public int getPendingSentMessagesCount() {
        int count = 0;
        for (SentMessage msg : sentMessages) {
            if (!msg.acknowledged) {
                count++;
            }
        }
        return count;
    }

    private void trimSentMessages() {
        while (sentMessages.size() > MAX_MESSAGES_HISTORY) {
            sentMessages.remove(sentMessages.size() - 1);
        }
    }

    // ==================== ПОЛУЧЕННЫЕ СООБЩЕНИЯ ====================

    /**
     * Добавить полученное сообщение в историю
     */
    public void addReceivedMessage(String msgId, String text) {
        // Проверяем дубликаты
        for (ReceivedMessage rm : receivedMessages) {
            if (rm.messageId.equals(msgId)) {
                return;
            }
        }
        ReceivedMessage msg = new ReceivedMessage(msgId, text);
        receivedMessages.add(0, msg);
        trimReceivedMessages();
    }

    /**
     * Добавить полученное сообщение из сохранённого состояния
     */
    public void addReceivedMessageFromState(String msgId, String text, long receivedAt, boolean ackConfirmed) {
        // Проверка на дубликат
        for (ReceivedMessage rm : receivedMessages) {
            if (rm.messageId.equals(msgId)) {
                return;
            }
        }

        ReceivedMessage rm = new ReceivedMessage();
        rm.messageId = msgId;
        rm.text = text;
        rm.receivedAt = receivedAt;
        rm.ackSent = true;
        rm.ackConfirmed = ackConfirmed;
        rm.ackSentAt = receivedAt;
        rm.ackSendCount = 1;

        receivedMessages.add(rm);
        trimReceivedMessages();
    }

    /**
     * Пометить что ACK для сообщения отправлен
     */
    public void markReceivedMessageAckSent(String msgId) {
        for (ReceivedMessage msg : receivedMessages) {
            if (msg.messageId.equals(msgId)) {
                msg.ackSent = true;
                msg.ackSentAt = System.currentTimeMillis();
                msg.ackSendCount++;
                break;
            }
        }
    }

    /**
     * Пометить что ACK для сообщения доставлен (другая сторона убрала слот)
     */
    public void markReceivedMessageAckConfirmed(String msgId) {
        for (ReceivedMessage msg : receivedMessages) {
            if (msg.messageId.equals(msgId)) {
                msg.ackConfirmed = true;
                break;
            }
        }
    }

    /**
     * Получить последние полученные сообщения
     */
    public List<ReceivedMessage> getLastReceivedMessages(int count) {
        List<ReceivedMessage> result = new ArrayList<>();
        for (int i = 0; i < Math.min(count, receivedMessages.size()); i++) {
            result.add(receivedMessages.get(i));
        }
        return result;
    }

    /**
     * Получить ID сообщений, ожидающих подтверждения ACK
     */
    public List<String> getPendingAckMessageIds() {
        List<String> result = new ArrayList<>();
        for (ReceivedMessage msg : receivedMessages) {
            if (!msg.ackConfirmed && currentVisibleMsgIds.contains(msg.messageId)) {
                result.add(msg.messageId);
            }
        }
        return result;
    }

    private void trimReceivedMessages() {
        while (receivedMessages.size() > MAX_MESSAGES_HISTORY) {
            receivedMessages.remove(receivedMessages.size() - 1);
        }
    }

    // ==================== ОЧИСТКА ====================

    /**
     * Очистить историю сообщений и heartbeat
     * (вызывается при смене сессии устройства)
     */
    public void clearHistory() {
        sentMessages.clear();
        receivedMessages.clear();
        currentVisibleMsgIds.clear();
        heartbeatSeq = 0;
        prevHeartbeatSeq = 0;
        lastHeartbeatReceived = 0;
    }
}