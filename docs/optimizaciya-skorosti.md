# Как ускорить обнаружение и гарантированную доставку

Аналитическая записка к `WiFi Direct Text Transceiver`. Дополняет
[отчёт об ошибках](otchet-oshibki-priem-peredacha.md): там — почему не работает,
здесь — **что сделать, чтобы работало быстро**.

Дата: 2026-10-25. Кодовая база: `master` после PR #1 и #2.

---

## 0. Короткий ответ

Сейчас задержка «нажал Отправить → увидел ACK» в лучшем случае **8–12 секунд**,
в худшем — десятки секунд, причём 80–90 % этого времени уходит **не на передачу**,
а на **ожидание чужих циклов опроса**. Причина архитектурная: DNS-SD — это
**pull-модель** (каждый узнаёт новости только когда сам спросит), а нам нужна
**push-модель** (отправитель сам «кричит» в эфир).

Три главных вывода:

1. **Главный резерв — не в DNS-SD, а в смене модели обнаружения.** BLE-реклама
   (или Wi-Fi Aware) даёт первый контакт за 0,3–1 с вместо 2–10 с, потому что
   не требует, чтобы получатель в этот момент опрашивал эфир.
2. **Внутри DNS-SD тоже есть 3–4-кратный запас** — за счёт событийного discovery,
   немедленного ACK, коротких ретрансмиссий и синхронизированных окон.
3. **«Гарантированная доставка» на радио делается не блокировками и не ожиданием,
   а идемпотентностью**: client-generated ID + дедупликация + сверка состояний
   (anti-entropy). Так делают Kafka, Signal, Briar, Google Nearby.

Ниже — измерения, практики индустрии со ссылками и конкретный план с оценкой
выигрыша по каждому пункту.

---

## 1. Куда именно уходит время: бюджет задержки

### 1.1. Что происходит при отправке одного сообщения (текущий код)

| Шаг | Механизм в коде | Типичное время |
|---|---|---|
| 1. Публикация записи слота | `addLocalService(WFD_MsgN)` | 0,1–0,5 с |
| 2. Получатель замечает запись | его `discoverServices()`, но не чаще `MIN_DISCOVER_GAP_MS = 4 с`, периодика `NORMAL_INTERVAL = 8 с` | **0–8 с** (p50 ≈ 4 с) |
| 3. Получатель публикует ACK | `ackUpdateRunnable` каждые 2 с или heartbeat 5 с | 0–5 с |
| 4. Отправитель замечает ACK | его собственный `discoverServices()` | **0–8 с** (p50 ≈ 4 с) |
| 5. Если что-то не дошло | `SLOT_RENEW_INTERVAL = 15 с` × 3, затем SYNC | +15…45 с |
| 6. Крайний случай | `SYNC_CHECK_INTERVAL = 10 с`, `SYNC_INTERVAL = 60 с` | до 60 с |

**Итого p50 ≈ 8–12 с, p95 ≈ 20–30 с.** Это ровно то, что видит пользователь.

### 1.2. Где физический предел

Измерения Wi-Fi Direct из литературы: обнаружение устройства — **1,48–15,35 с**,
формирование группы — **3–9 с**; «в худшем случае обнаружение может занимать
больше десяти секунд»
([IJERT, обзор архитектуры Wi-Fi Direct](https://www.ijert.org/android-wi-fi-direct-architecture-from-protocol-implementation-to-formal-specification)).
Причина — протокол: probe request/response по «социальным» каналам и поиск
окна, когда оба устройства слушают. Apple в статье про AWDL пишет, что
установление Wi-Fi P2P-соединения занимает
[«от четырёх до более чем десяти секунд»](https://arxiv.org/pdf/1808.03156).

Вывод: **в 8-секундной периодике виноват не только наш таймер, но и сам
транспорт**. Уменьшать интервал до 500 мс бессмысленно — supplicant ответит
`BUSY`, а батарея уйдёт. Нужен другой канал обнаружения.

### 1.3. Pull против push — ключевая таблица

| Свойство | DNS-SD / `discoverServices` (сейчас) | BLE-реклама | Wi-Fi Aware (NAN) |
|---|---|---|---|
| Кто инициирует | **получатель** (опрос) | **отправитель** (вещание) | оба, в синхронных окнах |
| Задержка первого контакта | 1,5–15 с | 0,1–1 с | **< 0,5 с** |
| Зависит от интервала опроса | да, линейно | нет | нет (окна каждые ~524 мс) |
| Можно передать данные без соединения | да, TXT ≤ 255 Б (наш хак) | да, 20–24 Б в adv-пакете | **да, 255 Б в service discovery frame** |
| Нужен диалог подтверждения | нет | нет | нет |
| Расход батареи | высокий (постоянный поиск) | низкий | очень низкий |

---

## 2. Что используют ведущие компании

### 2.1. Google — Nearby Connections: «дешёвое обнаружение, быстрые данные»

Библиотека Google Nearby Connections работает ровно по этой схеме: несколько
транспортов одновременно, причём обнаружение идёт по самому дешёвому каналу,
а данные — по самому быстрому. Внутри поддерживаются
`BLUETOOTH`, `BLE`, `WIFI_LAN`, `WIFI_DIRECT` и точка доступа
([разбор архитектуры google/nearby](https://deepwiki.com/google/nearby)).

Механика:

* реклама и скан идут через BLE с кастомным service UUID — приложение видит
  только «своих», а не все Bluetooth-устройства вокруг
  ([описание «танца» BLE-обнаружения](https://gamenight.zaions.com/blog/bluetooth-multiplayer-android-explained.html));
* после рукопожатия библиотека **апгрейдит канал**: «мы переключаемся между Wi-Fi
  и Bluetooth, чтобы получить максимальную пропускную способность… сначала
  подключаемся по низкоскоростному каналу, затем маленьким протоколом обмениваемся
  списком поддерживаемых транспортов и апгрейдимся»
  ([ответ инженера Google на Stack Overflow](https://stackoverflow.com/questions/51976470/google-nearby-connections-2-0-capabilities));
* стратегии топологии: `P2P_CLUSTER` (M:N, только неразрушающие среды),
  `P2P_STAR` (1:N, точка доступа), `P2P_POINT_TO_POINT` (1:1, максимум скорости).
  Академическое сравнение: STAR даёт до 79,9 Мбит/с, POINT_TO_POINT — около
  600 Мбит/с
  ([ScienceDirect, 2023](https://www.sciencedirect.com/science/article/pii/S2542660523002184)).

**Что забираем:** два уровня — «быстрое обнаружение» и «быстрая передача» —
не обязаны быть одним и тем же транспортом. И апгрейд канала после первого контакта.

### 2.2. Apple — AWDL и AirDrop: синхронные окна вместо опроса

AWDL — собственный протокол Apple для AirDrop/AirPlay. Устройства договариваются
о расписании: каждое объявляет последовательность **Availability Windows**
(окна доступности по 16 TU ≈ 16 мс), выбирается мастер, который синхронизирует
время; канальная последовательность покрывает 64 окна ≈ 1 с
([статья «One Billion Apples' Secret Sauce»](https://arxiv.org/html/1808.03156v1)).
Радио **переключается по времени** между каналом точки доступа и «социальными»
каналами (6/44/149), поэтому AirDrop находит устройства, **не отключая Wi-Fi**
([разбор AWDL](https://theapplewiki.com/wiki/Apple_Wireless_Direct_Link)).

**Что забираем:** если все устройства дышат в одном ритме, окно встречи
наступает через предсказуемые ~0,5–1 с, а не «когда-нибудь в течение 15 секунд».
Это можно повторить на уровне приложения, не трогая радио (см. п. 4.4).

### 2.3. Qualcomm / Wi-Fi Alliance — Wi-Fi Aware (NAN)

NAN — это, по сути, «DNS-SD, сделанный правильно»: publish/subscribe внутри
синхронных Discovery Window каждые 512 TU (≈ 524 мс), длительность окна ~16 мс,
service discovery frames до 400 µs
([разбор NAN](https://infishark.com/blogs/learn/wifi-aware-nan-explained)).
Android поддерживает с API 26: обнаружение «обычно < 500 мс», данные можно
передавать **до установления соединения** — до 255 байт прямо в service discovery
frame, а для потока поднимается NDP (data path)
([Android Developers](https://developer.android.com/develop/connectivity/wifi/wifi-aware),
[Stack Overflow: разница NAN и P2P](https://stackoverflow.com/questions/52760132/difference-between-wifi-aware-and-wifi-p2p-on-android)).

Существенно для нас: **наш TXT-хак с 240-байтными записями — это ровно то,
для чего NAN придуман**, только там это штатная операция с задержкой < 500 мс
и без кэша supplicant.

### 2.4. Briar — абстракция транспорта и delay-tolerant sync

Bramble (стек Briar) специально сделан для сетей с задержками: Bluetooth,
Wi-Fi, Tor и даже USB-флешка как транспорт, единый протокол поверх всех
([how it works](https://briarproject.org/how-it-works/)). Важные детали реализации:

* у **каждого транспорта есть параметр «максимальная задержка»**
  (например, 30 000 мс для Bluetooth) — таймеры не размазаны по коду, а заданы
  в конфигурации транспорта
  ([разбор Bramble](https://ethz.ch/content/dam/ethz/special-interest/infk/inst-infsec/appliedcrypto/education/theses/report_YuanmingSong.pdf));
* вместо «пуш-доставки» — **синхронизация состояния** при встрече: устройства
  обмениваются тем, что накопилось, и расходятся
  ([survey, TUM](https://www.net.in.tum.de/fileadmin/TUM/NET/NET-2021-05-1/NET-2021-05-1_01.pdf));
* доставка возможна через посредника («store-and-forward»), то есть отсутствие
  прямой связи не блокирует сообщение.

**Что забираем:** (а) явный бюджет задержки на транспорт в конфиге;
(б) «синхронизация вместо пересылки» — у нас это уже SYNC, но с плохим
интервалом и неэкономным форматом; (в) idempotent-модель вместо гарантий
реального времени.

### 2.5. Bridgefy, BitChat, Meshtastic — дедупликация и TTL

Все mesh-мессенджеры живут на трёх правилах: **TTL** (сколько прыжков),
**дубликат-кэш** (по ID сообщения) и **ограничение вещания**. У BitChat —
до 7 прыжков, у OSHI — 5, у Meshtastic — 3 по умолчанию
([сравнение](https://oshi-messenger.com/best-offline-messenger)).

**Что забираем:** наш `processedMessageIds` — уже дубликат-кэш; при добавлении
любого вещания (broadcast) нужен ещё и TTL, иначе «шторм» ретрансмиссий.

### 2.6. Signal / Kafka / WebRTC — гарантии и процентили

| Практика | Где | Что именно |
|---|---|---|
| Client-generated ID + идемпотентный приёмник | Signal, Matrix, XMPP | отправитель генерирует `msgId`, получатель дедуплицирует; ретрай не создаёт дубль |
| At-least-once + дедуп вместо exactly-once | Kafka (`enable.idempotence`, idempotent producer) | на ненадёжном канале «ровно один раз» недостижимо, но «хотя бы раз + дедуп» эквивалентно по наблюдаемому результату |
| RTO и экспоненциальный backoff | TCP/QUIC | не «повторять через фиксированные 500 мс», а увеличивать паузу |
| Piggyback-подтверждения | TCP ACK, QUIC | не отдельный пакет, а поле в каждом исходящем кадре |
| FEC / дублирование для мелких пакетов | WebRTC (RED/FEC) | при потере 10–20 % дешевле отправить 2 копии, чем ждать RTO |
| Set reconciliation (Bloom/IBLT) | Bitcoin (compact blocks), Cassandra repair | передавать не список ID, а компактный «отпечаток», чтобы найти расхождения |
| Ethernet-метрики по процентилям | вся индустрия | SLO задаётся по p95/p99, а не по среднему |

**Что забираем:** всё перечисленное применимо почти без изменений — у нас уже есть
`msgId`, ACK и SYNC, надо довести их до «взрослого» вида.

### 2.7. Android-специфика, которую используют все

* `WifiManager.WIFI_MODE_FULL_LOW_LATENCY` — уже применяется после PR #2;
* `setTcpNoDelay(true)` + `SO_PRIORITY` для сокетов — уже есть `NoDelay`;
* `LocalOnlyHotspot` (API 26+) — точка доступа без интернета **без диалога
  подтверждения**, альтернатива P2P-группе: снимает 3–9 с на формирование группы;
* `ConnectivityManager.bindProcessToNetwork` — чтобы трафик шёл в локальную сеть,
  а не в мобильную;
* foreground service типа `connectedDevice` + исключение из battery optimization —
  сделано в PR #2;
* BLE: `ADVERTISE_MODE_LOW_LATENCY` = интервал 100 мс, `SCAN_MODE_LOW_LATENCY` =
  окно 5 с / интервал 5 с, connection interval 7,5–30 мс
  ([параметры Android BLE](https://par.nsf.gov/servlets/purl/10127090)) — этого
  достаточно, чтобы текст влезал в один GATT-пакет с MTU 517.

---

## 3. Что именно оптимизировать в нашей программе

### 3.1. Четыре независимых источника задержки

1. **Опрос вместо вещания** — ждём до `NORMAL_INTERVAL` (8 с).
2. **Ретрансмиссия слишком редкая** — `SLOT_RENEW_INTERVAL` 15 с.
3. **Сверка состояний слишком редкая** — SYNC раз в 60 с.
4. **Формат неэкономный** — base64 в сокете (+33 %), списки ID в TXT (быстро
   упирается в 240 байт), полный текст сообщения в каждой перепубликации слота.

### 3.2. Матрица приоритетов

| # | Оптимизация | Усилие | Ожидаемый эффект | Риск |
|---|---|---|---|---|
| 1 | Событийный discovery (burst по событию, а не только по таймеру) | S | p50 4 с → 1–2 с | батарея, `BUSY` |
| 2 | Немедленный ACK (публиковать сразу, не ждать 2-секундный цикл) | S | −2…5 с на подтверждение | чаще `addLocalService` |
| 3 | Ретрай слота 1→2→4→8 с вместо 15 с | S | p95 45 с → 10 с | больше трафика |
| 4 | SYNC 10 с для неподтверждённых + Bloom-фильтр ID | M | сходимость 60 с → 10 с, объём ×5 меньше | ложные срабатывания фильтра |
| 5 | Бинарный формат кадра (varint + length-prefix вместо base64/текста) | M | −25…35 % байт, −несколько мс | совместимость версий |
| 6 | **BLE-обнаружение** (adv 100 мс + скан LOW_LATENCY) | M | первый контакт 2–10 с → 0,3–1 с | новые разрешения, +1–3 % батареи |
| 7 | **Wi-Fi Aware (NAN)**, где поддерживается | L | контакт < 0,5 с, данные до 255 Б без соединения | поддержка железом, API 26+ |
| 8 | Синхронизированные окна на уровне приложения | M | p50 → < 1 с при недорогом радио | нужна общая шкала времени |
| 9 | LocalOnlyHotspot вместо P2P-группы (для сокетов) | M | −3…9 с на установление канала данных | не на всех прошивках |
| 10 | Метрики p50/p95 в лог + тест-сценарий | S | возможность измерять, а не гадать | — |

---

## 4. Конкретные решения с кодом

### 4.1. Событийный discovery (самое дешёвое улучшение)

Сейчас `requestDiscovery()` троттлится 4 секундами. Добавляем приоритетный проход
для событий «пользователь отправил сообщение» и «пришло сообщение без ACK»:

```java
private static final long PRIORITY_DISCOVER_GAP_MS = 800;

/** Проход вне общей очереди: 1 раз в 800 мс, не чаще. */
private void requestUrgentDiscovery(String reason) {
    long now = System.currentTimeMillis();
    if (now - lastUrgentDiscover < PRIORITY_DISCOVER_GAP_MS) return;
    lastUrgentDiscover = now;
    lastDiscoverCall = 0;              // обходим общий троттлинг
    requestDiscovery(reason + "-urgent");
}
```

Вызовы: в `sendMessage()` после `registerMessageSlot()` и в `handleMessageSlotRecord()`
перед публикацией ACK. Ожидаемо: p50 с 4 с до 1–2 с.

### 4.2. Немедленный ACK

`ackUpdateRunnable` ходит раз в 2 с. Нужен прямой вызов `updateAckService()`
сразу после приёма сообщения (с троттлингом 500 мс, чтобы пачка сообщений не
породила пачку `addLocalService`):

```java
handler.removeCallbacks(ackUpdateRunnable);
handler.postDelayed(ackUpdateRunnable, 300);
```

### 4.3. Ретрай с экспоненциальной паузой

`SLOT_RENEW_INTERVAL = 15_000` заменяем на массив задержек `{1000, 2000, 4000, 8000}`:
серия коротких повторов «догоняет» собеседника, если он как раз проснулся.
Суммарно 15 с вместо 45 — и в 3 раза меньше «висящих» сообщений.

### 4.4. Синхронизированные окна (app-level Discovery Window)

Идея Apple/NAN, реализуемая без новых API:

* у нас уже есть поправка часов собеседника (`DiscoveredDevice.clockSkewMs`, PR #2);
* договариваемся, что «окно эфира» — это каждая секунда с номером,
  кратным 2: `epoch = (now + skew) / 2000`;
* `discoverServices()`/публикация слота планируются на ближайшую границу окна.

Тогда два телефона встречаются в эфире предсказуемо, а не случайно, и p50
падения задержки — до сотен миллисекунд. Побочный плюс: можно реже опрашивать
вне окон, экономя батарею.

### 4.5. BLE-обнаружение как «первый контакт»

Схема (как в Nearby Connections): BLE — только для обнаружения и коротких
сигналов, данные — по TXT/сокету.

```java
// Реклама: свой ID + флаг «есть непрочитанное», 24 байта полезной нагрузки
AdvertiseData data = new AdvertiseData.Builder()
        .addServiceUuid(new ParcelUuid(WFD_SERVICE_UUID))
        .addServiceData(new ParcelUuid(WFD_SERVICE_UUID), payload) // id(4) + sid(4) + flags(1)
        .setIncludeDeviceName(false)
        .build();

AdvertiseSettings settings = new AdvertiseSettings.Builder()
        .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_LOW_LATENCY) // 100 мс
        .setTxPowerLevel(AdvertiseSettings.ADVERTISE_TX_POWER_HIGH)
        .setConnectable(false)
        .build();

advertiser.startAdvertising(settings, data, advertiseCallback);

// Скан
ScanSettings scan = new ScanSettings.Builder()
        .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
        .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
        .build();
```

Что это даёт: **устройство узнаёт о соседе за 0,3–1 с** (100-мс реклама
обнаруживается за 1–3 цикла скана), после чего сразу идёт обычный TXT/сокетный
обмен. Опрос эфира больше не определяет задержку — её определяет только передача.

Разрешения (Android 12+): `BLUETOOTH_ADVERTISE`, `BLUETOOTH_SCAN`,
`BLUETOOTH_CONNECT`; рекламу/скан глушим при погашенном экране, если батарея
в приоритете.

### 4.6. Wi-Fi Aware (NAN) — «DNS-SD, сделанный правильно»

Там, где железо поддерживает (`WifiAwareManager.isAvailable()`), наш слот
заменяется на publish/subscribe:

```java
PublishConfig cfg = new PublishConfig.Builder()
        .setServiceName("_wfd._tcp")          // service id = hash имени
        .setServiceSpecificInfo(slotBytes)    // до 255 Б: msgId + текст
        .build();
awareSession.publish(cfg, new DiscoverySessionCallback() {
    @Override public void onMessageReceived(PeerHandle peer, byte[] message) { /* ACK */ }
});
```

Плюсы, прямо релевантные нашим болячкам:

* данные в service discovery frame — **до** установления соединения;
* окна каждые ~524 мс → предсказуемые < 500 мс;
* нет диалога подтверждения и нет кэша supplicant, который «съедал» TXT-обновления;
* NDP (data path) поднимается за ~400 мс, если нужен поток.

Ограничение: 255 байт на service specific info и зависимость от чипсета
(на части устройств `isAvailable()` = false). Поэтому NAN — «быстрый путь»,
DNS-SD — «всегда доступный путь».

### 4.7. Экономный SYNC: Bloom-фильтр вместо списка ID

Сейчас `WFD_Sync` везёт `sent=id1,id2,id3` и `recv=...`, что упирается в 240 байт
(3–4 ID на направление). Bloom-фильтр на 128 бит (16 байт) вмещает ~20 ID при
ложноположительных ~2 %:

```java
// отправитель SYNC
BitSet bloom = new BitSet(128);
for (String id : myRecvIds) {
    int h = bloomHash(id, 0);
    bloom.set(h & 127);
    bloom.set(bloomHash(id, 1) & 127);
}
record.put("bloom", Base64.getEncoder().encodeToString(bloom.toByteArray())); // 22 байта
```

Получатель проверяет свои `sent` по фильтру: «в фильтре нет» = гарантированно
не дошло (→ переотправить), «в фильтре есть» = скорее всего дошло (→ сверить
точным списком только при сомнении). Ложноположительные не ломают протокол:
они означают лишь отсутствие лишней переотправки, а точную сверку всегда можно
сделать полным списком в сокетном канале.

Дополнительно: `SYNC_CHECK_INTERVAL` для неподтверждённых сообщений — 10 с
вместо 60 с.

### 4.8. Бинарный кадр вместо base64

Base64 раздувает полезную нагрузку на 33 %. Формат кадра:

```
[1 байт тип][1 байт флаги][2 байта длина][N байт payload][4 байта CRC32]
payload MSG: [msgId 8 байт][utf8 текст]
payload ACK: [msgId 8 байт]
```

Выигрыш по байтам ~25–35 %, по времени — единицы миллисекунд, но главное:
исчезает риск, что перевод строки или `|` внутри текста сломает кадр, и появляется
CRC для явного обнаружения порчи.

### 4.9. Дублирование мелких сообщений (FEC «для бедных»)

Текст ≤ 100 символов — это один пакет. На потерях 10–20 % дешевле отправить его
дважды с интервалом 200–300 мс, чем ждать RTO 1 с. Реализуется флагом
`dup=1` в кадре; получатель дедуплицирует по `msgId` (механизм уже есть).

---

## 5. Ожидаемый результат

При выполнении пунктов 1–6 и 8 (без новых разрешений, только текущий транспорт):

| Метрика | Сейчас | После P0 | После P1 (BLE/NAN) |
|---|---|---|---|
| Первый контакт (discovery) | 1,5–15 с | 1–2 с (событийный burst + окна) | **0,3–1 с** (BLE), < 0,5 с (NAN) |
| Отправка → ACK, p50 | 8–12 с | 2–4 с | **< 1,5 с** |
| Отправка → ACK, p95 | 20–30 с | 6–10 с | 3–5 с |
| Сходимость после потери ACK | до 60 с | 10–15 с | 5–10 с |
| Доля доставленных при 20 % потерь | низкая | средняя | **высокая** (дубли + SYNC) |

Честная оговорка: числа — оценки на основе бюджета задержки из п. 1.1 и
литературных данных о транспортах; их нужно подтвердить измерением на двух
телефонах (см. п. 7).

---

## 6. Чего делать не стоит

* **Уменьшать период `discoverServices` до 500 мс.** supplicant ответит `BUSY`,
  а часть прошивок прервёт текущий поиск — станет хуже, не лучше.
* **Держать `WIFI_MODE_FULL_HIGH_PERF` и постоянный скан Wi-Fi.** Это уже
  приводило к разряду и троттлингу; после PR #2 используется `LOW_LATENCY`,
  а сканировать надо точечно.
* **Строить mesh с вещанием без TTL и TTL-дедупа.** Один «шторм» кладёт эфир.
* **Пытаться сделать exactly-once на радио.** Правильная модель —
  at-least-once + дедупликация по `msgId` (у нас уже есть).
* **Гнаться за средним.** Оптимизировать надо p95: пользователь помнит именно
  «иногда сообщение висит 40 секунд».

---

## 7. Как измерить, что стало быстрее

Нужен воспроизводимый сценарий, иначе любые правки — вера.

1. Два телефона, приложение запущено на обоих, экран включён, расстояние 2–3 м.
2. Отправить 30 сообщений по 20–40 символов с интервалом 5 с.
3. Для каждого сообщения из `DiagnosticLogger` вытащить метки:
   `SEND MESSAGE`, `Message slot N registered`, `MESSAGE RECEIVED`, `ACK received`,
   `ACK published #N`.
4. Считать: `t_ack − t_send` → p50/p95; долю доставленных с первой попытки;
   число ретрансмиссий.
5. Повторить трижды: (а) оба экрана включены, (б) экран получателя погашен,
   (в) расстояние 20–30 м.

Добавить в приложение сводку (кнопка «Статистика»), чтобы не парсить логи руками:

```java
class LatencyStats {
    void recordSend(String msgId) { sentAt.put(msgId, System.currentTimeMillis()); }
    void recordAck(String msgId)  { ackAt.put(msgId, System.currentTimeMillis()); }
    String summary() { /* p50, p95, max, delivery ratio */ }
}
```

Целевые SLO для проекта: **p50 ≤ 2 с, p95 ≤ 6 с, доставка ≥ 99 % за 30 с**
(на текущем транспорте, без BLE/NAN).

---

## 8. Порядок внедрения

1. **Шаг 1 (полдня):** п. 4.1–4.3 + п. 4.9 — событийный discovery, немедленный
   ACK, короткие ретраи, дублирование. Не требует новых разрешений.
2. **Шаг 2 (1–2 дня):** п. 4.7–4.8 — Bloom-SYNC и бинарный кадр; плюс п. 7 —
   замеры до/после, иначе шаг 3 не с чем сравнивать.
3. **Шаг 3 (2–3 дня):** п. 4.4–4.5 — синхронизированные окна и BLE-обнаружение.
   Здесь же разрешения, экран-настройка «быстрое обнаружение: вкл/выкл».
4. **Шаг 4 (по результатам шага 3):** п. 4.6 — Wi-Fi Aware как опциональный
   «быстрый путь» с фолбэком на DNS-SD; п. 4.9 (LocalOnlyHotspot) для сокетов.

---

## 9. Источники

* Google Nearby Connections, стратегии и транспорты —
  [strategies](https://developers.google.com/nearby/connections/strategies),
  [google/nearby: список сред](https://deepwiki.com/google/nearby),
  [апгрейд транспорта (ответ инженера Google)](https://stackoverflow.com/questions/51976470/google-nearby-connections-2-0-capabilities)
* Сравнение STAR/POINT_TO_POINT по пропускной способности —
  [ScienceDirect, 2023](https://www.sciencedirect.com/science/article/pii/S2542660523002184)
* Apple AWDL: окна доступности, канальная последовательность, синхронизация —
  [arXiv 1808.03156](https://arxiv.org/html/1808.03156v1),
  [The Apple Wiki: AWDL](https://theapplewiki.com/wiki/Apple_Wireless_Direct_Link)
* Wi-Fi Aware (NAN): окна 512 TU, 255 байт в SDF, NDP —
  [Android Developers](https://developer.android.com/develop/connectivity/wifi/wifi-aware),
  [разбор NAN](https://infishark.com/blogs/learn/wifi-aware-nan-explained),
  [NAN vs P2P](https://stackoverflow.com/questions/52760132/difference-between-wifi-aware-and-wifi-p2p-on-android)
* Задержки Wi-Fi Direct (1,48–15,35 с обнаружение, 3–9 с группа) —
  [IJERT](https://www.ijert.org/android-wi-fi-direct-architecture-from-protocol-implementation-to-formal-specification),
  [arXiv 1808.03156](https://arxiv.org/pdf/1808.03156)
* BLE: интервалы рекламы/скана, connection interval, Android-режимы —
  [параметры Android BLE](https://par.nsf.gov/servlets/purl/10127090),
  [BLE connection intervals](https://novelbits.io/ble-connection-intervals/),
  [BLE throughput primer](https://interrupt.memfault.com/blog/ble-throughput-primer)
* Briar/Bramble: абстракция транспорта, per-transport latency, delay-tolerant sync —
  [how it works](https://briarproject.org/how-it-works/),
  [разбор BTP](https://ethz.ch/content/dam/ethz/special-interest/infk/inst-infsec/appliedcrypto/education/theses/report_YuanmingSong.pdf),
  [survey TUM](https://www.net.in.tum.de/fileadmin/TUM/NET/NET-2021-05-1/NET-2021-05-1_01.pdf)
* Mesh-мессенджеры: TTL и дедупликация —
  [сравнение BitChat/OSHI/Meshtastic](https://oshi-messenger.com/best-offline-messenger)
