package com.example.directtest.fast;

import android.content.Context;
import android.os.Build;
import android.os.SystemClock;

import com.example.directtest.DiagnosticLogger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Структурированный журнал событий для однозначного сопоставления записей
 * двух телефонов.
 *
 * <h3>Зачем отдельный журнал</h3>
 * Обычный {@link DiagnosticLogger} — это читаемый человеком текст: в нём есть
 * время только «ЧЧ:ММ:СС.ммм» и нет общих идентификаторов, поэтому два лога
 * с двух телефонов сопоставить почти невозможно: непонятно, какое событие на
 * приёмнике соответствует какому событию на отправителе, и на сколько
 * расходятся часы устройств.
 *
 * <p>Здесь каждое событие — одна строка TSV с жёстко зафиксированным набором
 * колонок:
 *
 * <pre>
 * t_utc_ms  t_iso  t_mono_ms  dev  sid  event  trace  peer  seq  detail
 * </pre>
 *
 * <ul>
 *   <li>{@code t_utc_ms} — абсолютное время (эпоха, мс). Оба телефона обычно
 *       синхронизированы по NTP, поэтому колонка годится для грубого
 *       выравнивания, а {@code t_mono_ms} — для точных интервалов внутри
 *       одного устройства;</li>
 *   <li>{@code dev} — короткий ID устройства ({@code deviceId}), он уникален
 *       и печатается в логах обеих сторон;</li>
 *   <li>{@code trace} — <b>общий идентификатор цепочки</b>: для сообщения это
 *       {@code msgId}, для замера задержки — {@code probeId}. Именно по этой
 *       колонке строки двух телефонов склеиваются в одну историю
 *       (join по trace);</li>
 *   <li>{@code peer} — короткий ID собеседника, от которого/которому идёт
 *       событие;</li>
 *   <li>{@code detail} — набор {@code key=value} через пробел: задержки,
 *       каналы, попытки, смещение часов и т.п.</li>
 * </ul>
 *
 * <h3>Как сопоставлять</h3>
 * <ol>
 *   <li>Выгрузить {@code corr_*.tsv} с обоих телефонов (кнопка в
 *       {@code DiagnosticActivity}).</li>
 *   <li>Склеить по колонке {@code trace}: {@code MSG_SEND} (dev=A) →
 *       {@code MSG_RX} (dev=B) → {@code ACK_TX} (dev=B) → {@code ACK_RX}
 *       (dev=A).</li>
 *   <li>Разницу {@code t_utc_ms} читать как задержку; систематическое
 *       смещение пары оценивать по времени TXT-heartbeat и полю {@code skew_ms}.</li>
 * </ol>
 *
 * Формат специально плоский (TSV): открывается в Excel / pandas без
 * парсеров и остаётся читаемым глазами.
 */
public final class CorrelationLog {

    /** Заголовок файла. Версия формата — на случай будущих изменений. */
    public static final String HEADER =
            "#ra-dio-correlation v1\n"
                    + "#columns:\tt_utc_ms\tt_iso\tt_mono_ms\tdev\tsid\tevent\ttrace\tpeer\tseq\tdetail";

    // ==================== ИМЕНА СОБЫТИЙ ====================
    // Короткие, стабильные, одинаковые на обоих устройствах — по ним идёт
    // сопоставление. Не переименовывать без смены версии формата.

    /** Старт менеджера: версия, модель, API. */
    public static final String EV_START = "START";
    /** Остановка. */
    public static final String EV_STOP = "STOP";
    /** Проверка окружения (D1/D4): Location Mode, FEATURE_WIFI_DIRECT и пр. */
    public static final String EV_ENV = "ENV";
    /** Запрос обнаружения. {@code detail: reason=..., gap_ms=..., win=open|closed} */
    public static final String EV_DISC_REQ = "DISC_REQ";
    /** Итог обнаружения: ok/busy/err. */
    public static final String EV_DISC_RES = "DISC_RES";
    /** Дешёвый опрос requestPeers (A4). */
    public static final String EV_PEERS = "PEERS";
    /** Устройство с нашим приложением найдено (первый контакт). */
    public static final String EV_PEER_FOUND = "PEER_FOUND";
    /** Устройство потеряно. */
    public static final String EV_PEER_LOST = "PEER_LOST";
    /** Разница часов с собеседником пересчитана. */
    public static final String EV_CLOCK = "CLOCK";
    /** Окно синхронизированного поиска (A3). */
    public static final String EV_WINDOW = "WINDOW";
    /** Группа Wi-Fi Direct сформирована. */
    public static final String EV_GRP_UP = "GRP_UP";
    /** Группа распалась. */
    public static final String EV_GRP_DOWN = "GRP_DOWN";
    /** Соединение быстрого канала установлено. */
    public static final String EV_LINK_UP = "LINK_UP";
    /** Соединение быстрого канала потеряно. */
    public static final String EV_LINK_DOWN = "LINK_DOWN";
    /** Сообщение принято в приложении (submit). */
    public static final String EV_MSG_SEND = "MSG_SEND";
    /** Сообщение опубликовано в конкретном канале. */
    public static final String EV_MSG_TX = "MSG_TX";
    /** Сообщение получено. */
    public static final String EV_MSG_RX = "MSG_RX";
    /** Дубликат отброшен (та же цепочка пришла вторым каналом). */
    public static final String EV_MSG_DUP = "MSG_DUP";
    /** Подтверждение отправлено. */
    public static final String EV_ACK_TX = "ACK_TX";
    /** Подтверждение получено. */
    public static final String EV_ACK_RX = "ACK_RX";
    /** Замер RTT по каналу (проактивный probe). */
    public static final String EV_RTT = "RTT";
    /** Повторная передача. */
    public static final String EV_RETRY = "RETRY";
    /** Слот TXT опубликован (резервный путь). */
    public static final String EV_TXT_PUB = "TXT_PUB";
    /** Синхронизация состояний (anti-entropy). */
    public static final String EV_SYNC = "SYNC";
    /** Ретрансляция кадра через посредника (C6). */
    public static final String EV_RELAY = "RELAY";
    /** Ошибка. */
    public static final String EV_ERROR = "ERR";

    private static final int RING_SIZE = 4000;
    private static final long FLUSH_EVERY_MS = 1_000;

    private static CorrelationLog INSTANCE;

    private final Object lock = new Object();
    private final Deque<String> ring = new ArrayDeque<>(RING_SIZE);
    private final AtomicLong seq = new AtomicLong();
    private final SimpleDateFormat isoFormat;

    private String deviceId = "unknown";
    private String sessionId = "-";
    private String model = "-";
    private long startedUtcMs;
    private long startedMonoMs;

    private BufferedWriter writer;
    private File file;
    private boolean storageFailed;
    private long lastFlushMs;

    private CorrelationLog() {
        isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
    }

    public static synchronized CorrelationLog get() {
        if (INSTANCE == null) {
            INSTANCE = new CorrelationLog();
        }
        return INSTANCE;
    }

    /**
     * Открыть файл журнала. Безопасно вызывать повторно (например, после
     * перезапуска сервиса) — файл открывается заново, старое содержимое
     * сохраняется.
     */
    public void init(Context context, String deviceId, String sessionId) {
        synchronized (lock) {
            this.deviceId = shortId(deviceId);
            this.sessionId = sessionId != null ? sessionId : "-";
            this.model = Build.MANUFACTURER + " " + Build.MODEL;
            this.startedUtcMs = System.currentTimeMillis();
            this.startedMonoMs = SystemClock.elapsedRealtime();

            if (writer != null) {
                return; // уже открыт
            }
            if (context == null) {
                storageFailed = true;
                return;
            }

            try {
                File dir = new File(context.getExternalFilesDir(null), "logs");
                if (!dir.exists() && !dir.mkdirs()) {
                    throw new IOException("cannot create log dir: " + dir);
                }
                String name = "corr_" + this.deviceId + "_" + this.sessionId + ".tsv";
                file = new File(dir, name);
                writer = new BufferedWriter(new FileWriter(file, true));
                writer.write(HEADER);
                writer.write("\n");
                writer.write("#device\t" + model + "\tapi=" + Build.VERSION.SDK_INT
                        + "\tdevice_id=" + this.deviceId
                        + "\tsession=" + this.sessionId
                        + "\tstarted_utc=" + startedUtcMs + "\n");
                writer.flush();
            } catch (IOException e) {
                storageFailed = true;
                writer = null;
                DiagnosticLogger.getInstance().w("[Corr] cannot open correlation log: " + e.getMessage());
            }
        }
    }

    /** Путь к файлу журнала (null, если хранилище недоступно). */
    public File getFile() {
        synchronized (lock) {
            return file;
        }
    }

    public boolean isFileAvailable() {
        synchronized (lock) {
            return writer != null && file != null;
        }
    }

    /**
     * Записать событие.
     *
     * @param event код события ({@link #EV_MSG_RX} и т.п.)
     * @param trace общий идентификатор цепочки (msgId/probeId) или {@code null}
     * @param peer  короткий ID собеседника или {@code null}
     * @param kv    пары «ключ, значение, ключ, значение…» для колонки detail
     */
    public void event(String event, String trace, String peer, String... kv) {
        long utc = System.currentTimeMillis();
        long mono = SystemClock.elapsedRealtime();
        long id = seq.incrementAndGet();

        String iso;
        synchronized (isoFormat) {
            iso = isoFormat.format(new Date(utc));
        }

        StringBuilder sb = new StringBuilder(160);
        sb.append(utc).append('\t')
                .append(iso).append('\t')
                .append(mono).append('\t')
                .append(deviceId).append('\t')
                .append(sessionId).append('\t')
                .append(event).append('\t')
                .append(trace != null ? trace : "-").append('\t')
                .append(peer != null ? shortId(peer) : "-").append('\t')
                .append(id).append('\t')
                .append(formatDetail(kv));

        String line = sb.toString();

        synchronized (lock) {
            ring.addLast(line);
            while (ring.size() > RING_SIZE) {
                ring.removeFirst();
            }
            if (writer != null) {
                try {
                    writer.write(line);
                    writer.write('\n');
                    if (utc - lastFlushMs >= FLUSH_EVERY_MS) {
                        writer.flush();
                        lastFlushMs = utc;
                    }
                } catch (IOException e) {
                    storageFailed = true;
                    try {
                        writer.close();
                    } catch (IOException ignored) {
                    }
                    writer = null;
                }
            }
        }

        // Важные события дублируем в обычный лог — их видно в UI без выгрузки.
        if (isUiVisible(event)) {
            DiagnosticLogger.getInstance().i("[EVT] " + event
                    + (trace != null ? " " + trace : "")
                    + (peer != null ? " ↔ " + shortId(peer) : "")
                    + (kv.length > 0 ? " " + formatDetail(kv) : ""));
        }
    }

    /**
     * Упрощённый вызов: событие без trace и peer.
     *
     * ВАЖНО: метод назван {@code note}, а не {@code event}. Раньше здесь была
     * перегрузка {@code event(String event, String... kv)}, и компилятор не мог
     * выбрать между ней и {@code event(String, String, String, String...)} для
     * вызовов вида {@code event(EV, null, null, "k", "v")} — javac отвечал
     * «reference to event is ambiguous». Разные имена снимают проблему навсегда.
     */
    public void note(String event, String... kv) {
        event(event, null, null, kv);
    }

    /** Замер задержки: RTT/латентность доставки с привязкой к цепочке. */
    public void latency(String event, String trace, String peer, String phase, long ms) {
        CorrelationLog.this.event(event, trace, peer, "phase", phase, "ms", String.valueOf(ms));
    }

    /** Снимок журнала в текстовом виде (для шаринга/просмотра). */
    public String snapshot() {
        synchronized (lock) {
            StringBuilder sb = new StringBuilder(HEADER).append('\n');
            for (String line : ring) {
                sb.append(line).append('\n');
            }
            if (storageFailed) {
                sb.append("#storage_failed\n");
            }
            return sb.toString();
        }
    }

    /** Последние {@code n} строк — для экрана диагностики. */
    public String tail(int n) {
        synchronized (lock) {
            StringBuilder sb = new StringBuilder();
            int skip = Math.max(0, ring.size() - n);
            int i = 0;
            for (String line : ring) {
                if (i++ < skip) continue;
                sb.append(line).append('\n');
            }
            return sb.toString();
        }
    }

    public void flush() {
        synchronized (lock) {
            if (writer == null) return;
            try {
                writer.flush();
            } catch (IOException ignored) {
            }
        }
    }

    public void close() {
        synchronized (lock) {
            if (writer == null) return;
            try {
                writer.flush();
                writer.close();
            } catch (IOException ignored) {
            }
            writer = null;
        }
    }

    public long getStartedUtcMs() {
        return startedUtcMs;
    }

    public long getStartedMonoMs() {
        return startedMonoMs;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getSessionId() {
        return sessionId;
    }

    // ==================== ВНУТРЕННЕЕ ====================

    private static boolean isUiVisible(String event) {
        switch (event) {
            case EV_MSG_SEND:
            case EV_MSG_RX:
            case EV_ACK_RX:
            case EV_PEER_FOUND:
            case EV_GRP_UP:
            case EV_GRP_DOWN:
            case EV_LINK_UP:
            case EV_LINK_DOWN:
            case EV_ENV:
            case EV_ERROR:
            case EV_SYNC:
                return true;
            default:
                return false;
        }
    }

    private static String formatDetail(String... kv) {
        if (kv == null || kv.length == 0) return "-";
        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(kv[i]).append('=').append(sanitize(kv[i + 1]));
        }
        return sb.length() == 0 ? "-" : sb.toString();
    }

    /** Значения не должны ломать TSV: табы/переводы строк заменяем. */
    private static String sanitize(String value) {
        if (value == null) return "null";
        return value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    /** Короткий ID: первые 8 символов, как в остальных логах проекта. */
    public static String shortId(String id) {
        if (id == null) return "-";
        return id.length() > 8 ? id.substring(0, 8) : id;
    }
}
