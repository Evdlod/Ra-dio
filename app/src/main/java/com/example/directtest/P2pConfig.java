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

    // ==================== TRANSPORT: СОКЕТЫ ПОВЕРХ P2P-ГРУППЫ ====================

    /**
     * Транспорт сообщений по TCP поверх P2P-группы — часть быстрого пути (B1).
     *
     * ВКЛЮЧЁН: это основной канал доставки, DNS-SD остаётся слоем обнаружения
     * и резервным путём (TXT-слот публикуется только если ACK не пришёл за
     * {@link #TXT_FALLBACK_DELAY_MS}).
     *
     * Что нужно учитывать:
     *  - при первом соединении Android может показать системный диалог
     *    «Принять запрос» на принимающей стороне; повторные соединения идут
     *    по постоянной группе (SSID + пароль, без WPS) и диалога не требуют;
     *  - на части прошивок во время активного соединения приостанавливается
     *    поиск новых устройств — поэтому группа удерживается, а не пересоздаётся;
     *  - при выходе из зоны группа распадается, приложение переподключается
     *    само и параллельно продолжает работать по TXT.
     *
     * Выключить можно одним флагом: приложение вернётся к доставке через TXT.
     */
    public static final boolean ENABLE_P2P_SOCKET_TRANSPORT = true;

    /**
     * TCP-порт сокетного транспорта
     */
    public static final int SOCKET_PORT = 8988;

    /**
     * Таймаут подключения к владельцу группы (мс)
     */
    public static final int SOCKET_CONNECT_TIMEOUT_MS = 8_000;

    /**
     * Количество попыток подключения к владельцу группы
     */
    public static final int SOCKET_CONNECT_ATTEMPTS = 5;

    /**
     * Таймаут чтения из сокета (мс). Если данных нет дольше — соединение
     * считается мёртвым и закрывается.
     */
    public static final int SOCKET_READ_TIMEOUT_MS = 60_000;

    /**
     * Интервал heartbeat внутри сокетного соединения (мс)
     */
    public static final long SOCKET_HB_INTERVAL_MS = 15_000;

    /**
     * Минимальный интервал между попытками сформировать P2P-группу (мс)
     */
    public static final long GROUP_RETRY_INTERVAL_MS = 30_000;

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

    // ==================== A4: ДЕШЁВЫЙ ОПРОС ПИРОВ ====================

    /**
     * Интервал опроса {@code requestPeers} (мс).
     *
     * В отличие от discoverPeers это чтение уже известного списка: supplicant
     * не запускает сканирование, BUSY не возвращается. Позволяет замечать
     * собеседника, которого нашёл сам собеседник, не дожидаясь своего поиска.
     */
    public static final long PEERS_POLL_INTERVAL_MS = 2_000;

    // ==================== A5/A6: ГРУППА ====================

    /**
     * Переиспользовать постоянную группу (persistent group, API 29+).
     * Повторный контакт с уже знакомым телефоном занимает ~1 с вместо 3–9 с,
     * потому что WPS-негоциация не выполняется.
     */
    public static final boolean ENABLE_PERSISTENT_GROUP = true;

    /** Префикс имени сети группы. Имя одинаковое у обеих сторон — залог переиспользования. */
    public static final String PERSISTENT_GROUP_PREFIX = "DIRECT-RD-";

    /** Пароль группы (8..63 символа). */
    public static final String PERSISTENT_GROUP_PASSPHRASE = "ra-dio-2026-wifi";

    /**
     * Диапазон группы (D5): 0 — авто, 1 — 2,4 ГГц, 2 — 5 ГГц.
     * 2,4 ГГц по умолчанию: дальше и совместимее на бюджетных чипсетах.
     */
    public static final int GROUP_BAND_AUTO = 0;
    public static final int GROUP_BAND_24 = 1;
    public static final int GROUP_BAND_5 = 2;
    public static final int PREFERRED_GROUP_BAND = GROUP_BAND_24;

    /**
     * Сколько времени группа считается «нужной» после последнего контакта (мс), A7.
     * Пока собеседник рядом, группу не рвём: пересоздание стоит 3–9 секунд.
     */
    public static final long KEEP_GROUP_GRACE_MS = 30_000;

    // ==================== B1: БЫСТРЫЙ ПУТЬ КАК ОСНОВНОЙ ====================

    /**
     * Сокет/групповой канал — основной путь доставки, TXT — резервный.
     *
     * Как работает: сообщение сразу уходит по быстрому каналу; публикация в
     * TXT-слот откладывается на {@link #TXT_FALLBACK_DELAY_MS}. Если за это
     * время пришёл ACK — TXT вообще не используется (экономия эфира и
     * отсутствие «мигающих» сервисов). Если нет — включается прежний
     * надёжный путь.
     */
    public static final boolean SOCKET_IS_PRIMARY_PATH = true;

    /** Задержка перед публикацией TXT-слота при работающем быстром канале (мс). */
    public static final long TXT_FALLBACK_DELAY_MS = 1_500;

    // ==================== B2/B3: КАНАЛ ВНУТРИ ГРУППЫ ====================

    /** UDP-канал внутри подсети группы (самый быстрый путь). */
    public static final boolean ENABLE_GROUP_LINK_CHANNEL = true;

    /** Порт UDP-канала в подсети группы (один и тот же у всех участников). */
    public static final int GROUP_LINK_PORT = 8989;

    /** Период presence-маячка владельца группы (мс). */
    public static final int GROUP_LINK_BEACON_MS = 1_000;

    /** Максимальный размер полезной нагрузки, отправляемой одной датаграммой. */
    public static final int GROUP_LINK_MAX_DATAGRAM = 1_200;

    /**
     * mDNS ({@code NsdManager}) поверх линка группы (B2).
     *
     * <p>По умолчанию ВЫКЛЮЧЕНО. На части прошивок NsdManager не умеет
     * работать через P2P-интерфейс (регистрация уходит в default network) и
     * молча ничего не находит. Основной «быстрый» механизм — UDP-канал
     * ({@link #ENABLE_GROUP_LINK_CHANNEL}), который не зависит от выбора
     * интерфейса. Флаг оставлен для экспериментов на устройствах, где
     * NsdManager через группу работает.
     */
    public static final boolean ENABLE_MDNS_ON_GROUP = false;

    // ==================== B5/B6/B7/B8: НАДЁЖНОСТЬ И ТАЙМАУТЫ ====================

    /** Интервал keep-alive в быстром канале (мс), B7. 15 с недостаточно: power-save рвёт. */
    public static final long SOCKET_KEEPALIVE_MS = 5_000;

    /** Лестница повторов (мс), B6: 1 → 2 → 4 → 8. */
    public static final long[] RETRY_LADDER_MS = {1_000, 2_000, 4_000, 8_000};

    /** Нижняя граница адаптивного таймаута ожидания ACK (мс), B8. */
    public static final long ADAPTIVE_RTO_MIN_MS = 300;

    /** Верхняя граница адаптивного таймаута (мс), B8. */
    public static final long ADAPTIVE_RTO_MAX_MS = 8_000;

    /** Множитель SRTT для расчёта RTO (как в TCP). */
    public static final double ADAPTIVE_RTO_FACTOR = 4.0;

    /** Интервал фонового замера RTT (мс). */
    public static final long RTT_PROBE_INTERVAL_MS = 5_000;

    // ==================== C: ГАРАНТИИ ДОСТАВКИ ====================

    /** Ретрансляция через посредника — расширяет дальность без Bluetooth (C6). */
    public static final boolean ENABLE_RELAY = true;

    /** Сколько дополнительных хопов допускается (1 — только прямой сосед). */
    public static final int RELAY_TTL = 1;

    /** Ускоренная проверка SYNC, когда есть неподтверждённые сообщения (мс). */
    public static final long SYNC_FAST_CHECK_MS = 5_000;

    /** Размер Bloom-фильтра ID сообщений в SYNC-записи (бит). */
    public static final int BLOOM_BITS = 128;

    /** Приоритет сообщения: 0 — обычное, 1 — срочное (обгоняет остальные в очереди). */
    public static final int PRIORITY_NORMAL = 0;
    public static final int PRIORITY_URGENT = 1;

    // ==================== D: ВЕНДОРСКАЯ СОВМЕСТИМОСТЬ ====================

    /**
     * Требовать включённый Location Mode для обнаружения (D1).
     * На MIUI/Funtouch без него discoverPeers возвращает пустой список.
     */
    public static final boolean REQUIRE_LOCATION_FOR_DISCOVERY = true;

    /**
     * Период обновления основной записи WFD_Main (мс), D8.
     * Heartbeat уже публикует её каждые 5 с — это верхняя граница.
     */
    public static final long MAIN_SERVICE_REFRESH_MS = 10_000;

    // ==================== PROTOCOL VERSION ====================

    /**
     * Версия протокола.
     *
     * 5 — переход на бинарные кадры (B4), канал внутри группы (B2/B3),
     * ретрансляция (C6). Устройства с версией 4 продолжают работать по TXT.
     */
    public static final String PROTOCOL_VERSION = "5";
}