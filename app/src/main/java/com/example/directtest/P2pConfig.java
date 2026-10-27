package com.example.directtest;

/**
 * Централизованная конфигурация WiFi P2P Discovery.
 * Все константы, таймауты и лимиты в одном месте.
 */
public final class P2pConfig {

    private P2pConfig() {
        // Utility class
    }

    // ==================== SERVICE NAMES ====================

    /**
     * Тип DNS-SD сервиса
     */
    public static final String SERVICE_TYPE = "_wfd._tcp";

    /**
     * Имя основного сервиса (heartbeat)
     */
    public static final String MAIN_SERVICE_NAME = "WFD_Main";

    /**
     * Префикс для слотов сообщений (WFD_Msg0, WFD_Msg1, WFD_Msg2)
     */
    public static final String MSG_SLOT_PREFIX = "WFD_Msg";

    /**
     * Имя сервиса подтверждений
     */
    public static final String ACK_SERVICE_NAME = "WFD_Ack";

    /**
     * Имя сервиса синхронизации
     */
    public static final String SYNC_SERVICE_NAME = "WFD_Sync";

    /**
     * Маркер в имени устройства для идентификации нашего приложения
     */
    public static final String APP_MARKER = "[WFD]";

    // ==================== LIMITS ====================

    /**
     * Максимальное количество слотов для сообщений
     */
    public static final int MAX_MSG_SLOTS = 3;

    /**
     * Максимальное количество обработанных ID сообщений в памяти
     */
    public static final int MAX_PROCESSED_IDS = 100;

    /**
     * Максимальное количество обработанных ACK в памяти
     */
    public static final int MAX_PROCESSED_ACKS = 50;

    /**
     * Максимальное количество TXT записей для дедупликации
     */
    public static final int MAX_RECENT_TXT_RECORDS = 50;

    /**
     * Максимальная длина сообщения в TXT record (в СИМВОЛАХ, жёсткий предел UI).
     * ВНИМАНИЕ: реальное ограничение DNS-SD — байты, см. MAX_TEXT_BYTES.
     * Кириллица в UTF-8 занимает 2 байта на символ, эмодзи — 4.
     */
    public static final int MAX_MESSAGE_LENGTH = 100;

    /**
     * Бюджет одного TXT-рекорда в байтах.
     * Протокол DNS-SD ограничивает строку 255 байтами, а весь TXT-рекорд
     * рекомендуется держать в пределах ~200-240 байт. Превышение приводит
     * к тому, что часть стеков молча выбрасывает запись: сообщение не
     * доставляется и никакой ошибки в лог не попадает.
     */
    public static final int MAX_TXT_RECORD_BYTES = 240;

    /**
     * Максимальный размер текста сообщения в байтах UTF-8.
     * Ограничение применяется до подгонки всей записи под MAX_TXT_RECORD_BYTES.
     */
    public static final int MAX_TEXT_BYTES = 120;

    /**
     * Минимальный осмысленный размер текста (если под ACK остаётся меньше —
     * ACK выбрасывается из записи, чтобы не терять текст сообщения).
     */
    public static final int MIN_TEXT_BYTES = 24;

    /**
     * Максимальное количество ACK в одной записи
     */
    public static final int MAX_ACKS_PER_RECORD = 5;

    // ==================== TIMING: DISCOVERY ====================

    /**
     * Минимальный интервал между запросами discovery (мс).
     * discoverPeers/discoverServices — асинхронные команды supplicant:
     * повторный вызов во время активного поиска возвращает BUSY, а на части
     * прошивок прерывает текущий поиск и результаты теряются.
     *
     * Это «пол» для любых источников запроса (периодика, PEERS_CHANGED,
     * регистрация слота, forceRefresh). Быстрый режим (FAST_INTERVAL = 3 с)
     * фактически ограничен этим значением.
     */
    public static final long MIN_DISCOVER_GAP_MS = 4_000;

    /**
     * Задержка между discoverPeers и discoverServices внутри одного цикла (мс)
     */
    public static final long DISCOVER_SERVICES_DELAY_MS = 150;

    /**
     * Максимальное количество повторов discoverServices при BUSY
     */
    public static final int DISCOVER_MAX_RETRIES = 3;

    /**
     * Базовая задержка повтора при BUSY (мс), растёт экспоненциально: 2с, 4с, 8с
     */
    public static final long DISCOVER_RETRY_BASE_MS = 2_000;

    /**
     * Быстрый интервал discovery после обнаружения нового устройства (мс)
     */
    public static final long FAST_INTERVAL = 3_000;

    /**
     * Нормальный интервал discovery (мс)
     */
    public static final long NORMAL_INTERVAL = 8_000;

    /**
     * Длительность быстрого режима после обнаружения устройства (мс)
     */
    public static final long FAST_MODE_DURATION = 30_000;

    // ==================== TIMING: HEARTBEAT & SERVICES ====================

    /**
     * Интервал обновления heartbeat (мс)
     */
    public static final long HEARTBEAT_INTERVAL = 5_000;

    /**
     * Интервал обновления ACK сервиса (мс)
     */
    public static final long ACK_UPDATE_INTERVAL = 2_000;

    /**
     * Время жизни ACK сервиса (мс)
     */
    public static final long ACK_SERVICE_LIFETIME = 15_000;

    /**
     * Время жизни SYNC сервиса (мс)
     */
    public static final long SYNC_SERVICE_LIFETIME = 30_000;

    /**
     * Интервал проверки необходимости SYNC (мс)
     */
    public static final long SYNC_CHECK_INTERVAL = 10_000;

    // ==================== TIMING: MESSAGES ====================

    /**
     * Таймаут слота сообщения (мс). Устарело: используется SLOT_RENEW_INTERVAL.
     */
    public static final long SLOT_TIMEOUT = 30_000;

    /**
     * Интервал между переопубликациями слота без ACK (мс).
     * Каждая переопубликация обновляет DNS-кэш собеседника.
     */
    public static final long SLOT_RENEW_INTERVAL = 15_000;

    /**
     * Сколько раз переопубликовывать слот, прежде чем освободить его
     * (15 с × 3 = 45 с, после чего сообщение уходит в SYNC).
     */
    public static final int SLOT_RENEW_ROUNDS = 3;

    /**
     * Максимальный возраст сообщения в секундах (TTL)
     */
    public static final long MAX_MSG_AGE_SEC = 120;

    /**
     * Предел правдоподобного расхождения часов с собеседником (мс).
     * Более сильное расхождение не запоминается, чтобы не «сломать» фильтр возраста.
     */
    public static final long CLOCK_SKEW_LIMIT_MS = 12L * 60 * 60 * 1000;

    /**
     * Окно дедупликации TXT записей (мс)
     */
    public static final long TXT_DEDUP_WINDOW_MS = 2_000;

    // ==================== TIMING: DEVICE STATUS ====================

    /**
     * Порог для определения онлайн-статуса устройства (мс)
     */
    public static final long DEVICE_ONLINE_THRESHOLD = 20_000;

    /**
     * Время жизни устройства в кэше (мс)
     */
    public static final long CACHE_TTL = 120_000;

    // ==================== A1: ПРИОРИТЕТНЫЙ ПОИСК ====================

    /**
     * Минимальный интервал для приоритетного поиска (мс).
     *
     * Обычный {@link #MIN_DISCOVER_GAP_MS} = 4 с — «пол», защищающий от BUSY.
     * Но когда пришло событие «зарегистрирован слот с сообщением» или
     * «обнаружен пир», ждать 4 секунды бессмысленно: поиск нужен прямо
     * сейчас. 800 мс — минимальный интервал, который supplicant стабильно
     * переваривает на протестированных прошивках (Xiaomi, vivo, Samsung).
     */
    public static final long PRIORITY_DISCOVER_GAP_MS = 800;

    /**
     * Минимальный интервал для «лёгкого» прохода (только discoverServices).
     *
     * Полный проход (с discoverPeers) стоит дороже: он перезапускает
     * сканирование и на части прошивок обрывает уже начатый поиск. Поэтому
     * порог {@link #PRIORITY_DISCOVER_GAP_MS} применяется ТОЛЬКО к проходам
     * без discoverPeers; полные проходы всегда уважают
     * {@link #MIN_DISCOVER_GAP_MS}.
     */
    public static final long PRIORITY_SERVICES_ONLY_GAP_MS = 800;

    /**
     * Длительность одной фазы поиска (мс), A2.
     * Внутри фазы вызывается только что-то одно: либо discoverPeers, либо
     * discoverServices. Так supplicant не получает две команды подряд и не
     * отвечает BUSY.
     */
    public static final long DISCOVER_PHASE_WINDOW_MS = 1_500;

    // ==================== A3: ОКНА СИНХРОНИЗИРОВАННОГО ПОИСКА ====================

    /** Период окон поиска (мс). Обе стороны считают от эпохи — окна совпадают. */
    public static final long SYNC_WINDOW_PERIOD_MS = 5_000;

    /** Длительность окна поиска (мс). */
    public static final long SYNC_WINDOW_LENGTH_MS = 2_000;

    /**
     * Период окон, когда торопиться некуда (нет неподтверждённых сообщений).
     *
     * Частые окна нужны только чтобы быстро доставить/подтвердить сообщение.
     * В покое они лишь дёргают supplicant — на части прошивок это мешает
     * нормальному поиску. В покое работаем в прежнем темпе (8 с).
     */
    public static final long SYNC_WINDOW_IDLE_PERIOD_MS = 8_000;

    /**
     * Сколько миллисекунд список пиров считается «свежим».
     *
     * Только при свежем списке приоритетный проход может ограничиться
     * discoverServices (A2). Иначе обязателен полный проход с discoverPeers —
     * иначе на «холодном» кэше (сохранённые устройства с прошлой сессии)
     * новые телефоны не находятся вовсе.
     */
    public static final long PEERS_FRESH_MS = 10_000;

    // ==================== A4: ДЕШЁВЫЙ ОПРОС ПИРОВ ====================

    /**
     * Интервал опроса {@code requestPeers} (мс).
     *
     * В отличие от discoverPeers это чтение уже известного списка: supplicant
     * не запускает сканирование, BUSY не возвращается. Позволяет замечать
     * собеседника, которого нашёл сам собеседник, не дожидаясь своего поиска.
     */
    public static final long PEERS_POLL_INTERVAL_MS = 3_000;

    /** Ускоренная проверка SYNC, когда есть неподтверждённые сообщения (мс). */
    public static final long SYNC_FAST_CHECK_MS = 5_000;

    /** Размер Bloom-фильтра ID сообщений в SYNC-записи (бит). */
    public static final int BLOOM_BITS = 128;

    // ==================== D: ВЕНДОРСКАЯ СОВМЕСТИМОСТЬ ====================

    /**
     * Требовать включённый Location Mode для обнаружения (D1).
     * На MIUI/Funtouch без него discoverPeers возвращает пустой список.
     */
    public static final boolean REQUIRE_LOCATION_FOR_DISCOVERY = true;

    /**
     * Через сколько миллисекунд «радио недоступно» перепроверяется принудительно.
     *
     * Защита от залипания: прошивка может не прислать broadcast о включении
     * Wi-Fi/P2P, и тогда гейт остановил бы поиск навсегда. По истечении этого
     * времени делается пробный проход и состояние перечитывается.
     */
    public static final long RADIO_SELF_HEAL_MS = 15_000;

    /**
     * Период обновления основной записи WFD_Main (мс), D8.
     * Heartbeat уже публикует её каждые 5 с — это верхняя граница.
     */
    public static final long MAIN_SERVICE_REFRESH_MS = 10_000;

    // ==================== PROTOCOL VERSION ====================

    /** Версия TXT-протокола DNS-SD. */
    public static final String PROTOCOL_VERSION = "6";
}