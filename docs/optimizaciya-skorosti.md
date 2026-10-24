# Как ускорить обнаружение и доставку — только Wi-Fi Direct

Аналитическая записка к `WiFi Direct Text Transceiver`. Дополняет
[отчёт об ошибках](otchet-oshibki-priem-peredacha.md).

**Редакция 2 (2026-10-25).** Из первых версий исключены **Wi-Fi Aware (NAN)** и
**BLE/Bluetooth-обнаружение**: NAN есть не на всех чипсетах, а Bluetooth проигрывает
Wi-Fi по дальности. Ниже — только технологии, которые работают на Xiaomi, Redmi,
vivo, realme, Oppo, Samsung и т. п.: сам Wi-Fi Direct (`WifiP2pManager`),
TCP/UDP-сокеты внутри P2P-группы, mDNS (`NsdManager`) на этом же линке и
Local Only Hotspot как альтернатива P2P-группе.

---

## 0. Короткий ответ

Задержка «нажал Отправить → увидел ACK» сейчас **p50 ≈ 8–12 с, p95 ≈ 20–30 с**,
и почти всё это — не передача, а **ожидание чужих циклов опроса**: DNS-SD это
pull-модель, где получатель узнаёт новость только когда сам спросит эфир.

Мы не можем ускорить сам Wi-Fi Direct: обнаружение 1,48–15,35 с, формирование
группы 3–9 с — это свойства протокола, а не нашего кода. Зато можем:

1. **перестать платить эти секунды на каждое сообщение** — удерживать группу и
   гонять текст по сокету (после контакта задержка падает до единиц миллисекунд);
2. **сократить ожидание внутри DNS-SD в 3–4 раза** — событийный опрос, фазировка,
   синхронизированные окна;
3. **сделать ретрансмиссию и сверку состояний быстрыми** — 1→2→4→8 с вместо 15 с
   и Bloom-фильтр вместо списков ID;
4. **убрать платформенные «тихие» потери** на MIUI/Funtouch — включённый Location
   Mode, автозапуск, энергосбережение.

Ожидаемый результат без единой новой технологии: **p50 ≈ 1–3 с, p95 ≈ 5–8 с**,
а при удержании группы — **сотни миллисекунд**.

---

## 1. Что именно доступно на целевых устройствах

| Возможность | API | Xiaomi/Redmi (MIUI, HyperOS) | vivo/iQOO (Funtouch/OriginOS) | Комментарий |
|---|---|---|---|---|
| Wi-Fi Direct (P2P) | `WifiP2pManager`, API 14+ | да | да | основа проекта |
| DNS-SD поверх P2P | `WifiP2pDnsSdServiceInfo` | да | да | текущий канал данных |
| TCP/UDP в группе | `java.net.Socket` | да | да | уже добавлено (PR #2) |
| mDNS на линке группы | `NsdManager`, API 16+ | да | да | **новое предложение** |
| Persistent group | `WifiP2pConfig.Builder.enablePersistentMode`, API 29+ | да (Android 10+) | да | **новое предложение** |
| Группа по SSID+паролю без WPS | `WifiP2pConfig.Builder.setNetworkName/setPassphrase` | да | да | ускоряет вход в группу |
| Local Only Hotspot | `WifiManager.startLocalOnlyHotspot`, API 26+ | да, но с оговорками | да | альтернатива P2P-группе |
| Выбор диапазона группы | `setGroupOperatingBand` | да | да | 2,4 ГГц — дальше, 5 ГГц — быстрее |
| ~~Wi-Fi Aware (NAN)~~ | `WifiAwareManager` | **не на всех** | **не на всех** | исключено |
| ~~BLE-обнаружение~~ | `BluetoothLeAdvertiser` | да | да | исключено: дальность |

---

## 2. Бюджет задержки: куда уходит время

### 2.1. Одно сообщение, текущая реализация

| Шаг | Механизм в коде | Время |
|---|---|---|
| 1. Публикация слота | `addLocalService(WFD_MsgN)` | 0,1–0,5 с |
| 2. Получатель замечает запись | его `discoverServices()`, не чаще `MIN_DISCOVER_GAP_MS = 4 с`, периодика 8 с | **0–8 с** (p50 ≈ 4 с) |
| 3. Получатель публикует ACK | `ackUpdateRunnable` раз в 2 с, либо heartbeat 5 с | 0–5 с |
| 4. Отправитель замечает ACK | его собственный `discoverServices()` | **0–8 с** (p50 ≈ 4 с) |
| 5. Потеря → переопубликация слота | `SLOT_RENEW_INTERVAL = 15 с` × 3 | +15…45 с |
| 6. Крайний случай | `SYNC_CHECK_INTERVAL = 10 с` / `SYNC_INTERVAL = 60 с` | до 60 с |

### 2.2. Скрытые статьи расхода, которых нет в таблице

* **WPS-негоциация при каждом `connect()`** — обмен ключами через supplicant,
  секунды поверх формирования группы. Обходится, если в `WifiP2pConfig` заданы
  `networkName` + `passphrase`: по комментарию в AOSP `WifiP2pServiceImpl`,
  «supplicant can construct a group on the fly … or join a group without
  negotiation and WPS»
  ([AOSP](https://cs.android.com/android/platform/superproject/+/master:packages/modules/Wifi/service/java/com/android/server/wifi/p2p/WifiP2pServiceImpl.java)).
* **Churn локальных сервисов.** Сейчас `WFD_Main` переопубликовывается каждые 5 с
  (remove + add), плюс до 6 сервисов одновременно (main, ack, sync, 3 слота).
  Каждая перерегистрация — работа supplicant, которая задерживает выдачу остальных
  TXT-записей.
* **Power save.** Между пакетами Wi-Fi уходит в экономию энергии, и первый пакет
  после паузы ждёт пробуждения радио. При редком трафике это добавляет десятки
  миллисекунд к каждому обмену — незаметно на фоне 8 секунд, но критично, когда
  мы дойдём до «сотен миллисекунд».
* **Ожидание ACK-цикла 2 с.** ACK публикуется не в момент приёма, а по таймеру.

### 2.3. Пределы транспорта (не наши)

Измерения Wi-Fi Direct: обнаружение **1,48–15,35 с**, формирование группы
**3–9 с**, «в худшем случае обнаружение может занимать больше десяти секунд»
([IJERT](https://www.ijert.org/android-wi-fi-direct-architecture-from-protocol-implementation-to-formal-specification)).
Apple в разборе AWDL пишет, что установление Wi-Fi P2P-соединения занимает
[«от четырёх до более чем десяти секунд»](https://arxiv.org/pdf/1808.03156).

Вывод: **сокращать период опроса до сотен миллисекунд бессмысленно** — supplicant
ответит `BUSY`, часть прошивок прервёт текущий поиск. Ускорять надо иначе:
меньше зависеть от опроса и не платить formation time на каждое сообщение.

---

## 3. Принцип, который мы можем воспроизвести без NAN и BLE

В AWDL и NAN выигрыш даёт не радио, а **синхронизация**: устройства дышат в одном
ритме и встречаются в предсказуемом окне. Там окна делает прошивка; **у нас уже
есть всё необходимое, чтобы сделать такие окна на уровне приложения**:

* в PR #2 добавлена поправка часов собеседника (`DiscoveredDevice.clockSkewMs`),
  то есть обе стороны знают общую шкалу времени;
* значит, обе стороны могут считать один и тот же номер окна и планировать
  `discoverServices()` и публикации на его границу.

Это не заменяет Wi-Fi Direct, а снимает главную причину задержки — **случайность
момента встречи**.

---

## 4. Новые предложения

### Блок A. Первый контакт: ускорить обнаружение (только Wi-Fi Direct)

**A1. Событийный (приоритетный) проход discovery.**
Сейчас всё троттлится 4 секундами. Добавляем отдельный «срочный» канал: после
отправки/приёма сообщения — один проход с троттлингом 800 мс.

```java
private static final long PRIORITY_DISCOVER_GAP_MS = 800;
private long lastUrgentDiscover = 0;

private void requestUrgentDiscovery(String reason) {
    long now = System.currentTimeMillis();
    if (now - lastUrgentDiscover < PRIORITY_DISCOVER_GAP_MS) return;
    lastUrgentDiscover = now;
    lastDiscoverCall = 0;                    // обходим общий троттлинг
    requestDiscovery(reason + "-urgent");
}
```

Вызовы: в `sendMessage()` после `registerMessageSlot()` и в
`handleMessageSlotRecord()` перед публикацией ACK. Эффект: p50 с 4 с до 1–2 с.

**A2. Фазировка вместо «оба сразу».**
`discoverPeers()` и `discoverServices()` дёргают один supplicant. Вместо пары
«peers → 150 мс → services» делаем окна: 1,5 с `discoverPeers`, затем 1,5 с
`discoverServices`, затем пауза. Меньше `BUSY`, выше доля успешных проходов
(сейчас часть проходов теряется впустую).

**A3. Синхронизированные окна (app-level).**
Обе стороны договариваются: «окно эфира» — каждая секунда с чётным номером.

```java
private static final long WINDOW_MS = 2_000;

private long nextWindowStart() {
    long now = System.currentTimeMillis();
    return now + (WINDOW_MS - (now % WINDOW_MS));
}

// публикации и discoverServices планируем на границу окна
handler.postDelayed(this::requestDiscovery, nextWindowStart() - System.currentTimeMillis());
```

Между окнами опрос можно вообще не делать — экономия батареи и меньше `BUSY`.
Внутри окна вероятность «поймать» собеседника резко выше, чем при случайных
моментах.

**A4. Опрос `requestPeers` вместо нового поиска.**
`requestPeers()`/`requestGroupInfo()` отдают уже известное состояние и **не**
запускают поиск, то есть не вызывают `BUSY`. Дешёвый поллинг раз в 2 с позволяет
узнавать о новых соседях, не трогая supplicant, а `discoverServices()` вызывать
только когда список peers действительно изменился.

**A5. Группа по известным SSID + паролю (без WPS).**
Если оба устройства уже знают `networkName` и `passphrase`, `connect()` идёт
«join without negotiation and WPS» — заметно быстрее обычного пути с WPS-PBC.
Пароль можно передать один раз в TXT-записи (8–63 ASCII) или вывести
детерминированно из пары `deviceId` (обфускация, не безопасность — о чём честно
сказать в README).

```java
WifiP2pConfig cfg = new WifiP2pConfig.Builder()
        .setDeviceAddress(MacAddress.fromString(peer.address))
        .setNetworkName("DIRECT-WFD-" + shortDeviceId)   // SSID группы
        .setPassphrase(passphrase)                        // 8..63 ASCII
        .enablePersistentMode(true)                       // см. A6
        .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_2GHZ)
        .build();
manager.connect(channel, cfg, listener);
```

**A6. Persistent group — главный резерв по времени контакта.**
Один раз сформированная группа запоминается системой, и повторное соединение
идёт без пользовательского подтверждения и без полной негоциации
(`enablePersistentMode(true)`, API 29+;
[wpa_supplicant README-P2P](https://android.googlesource.com/platform/external/wpa_supplicant_8/+/master/wpa_supplicant/README-P2P):
`persist` — «only if previous persistent group existed between peers and shall be
re-used»). Это превращает повторный контакт из 3–9 с в ~1 с.

**A7. Не рвать группу.**
Сейчас в `stop()` вызывается `removeGroup()`, а при выключенном сокетном
транспорте — ещё и в `CONNECTION_CHANGED`. Для сценария «рация в кармане»
группу надо **удерживать**: пока устройство онлайн и рядом, `removeGroup` не
вызывать вовсе. Освобождать — только по явной команде пользователя или при
выходе из приложения на N минут.

### Блок B. Доставка: уйти от TXT, не уходя с Wi-Fi

**B1. Сокеты как основной путь (уже есть, но выключено).**
`P2pSocketTransport` (PR #2) при включении даёт отправку за единицы миллисекунд.
Новая логика: если сокет до получателя жив — **не публиковать слот вообще**,
TXT использовать только для presence. Это убирает из горячего пути и опрос, и
churn сервисов, и лимит 240 байт.

**B2. mDNS поверх группы вместо supplicant-DNS-SD.**
После формирования группы у нас появляется обычная IP-сеть. `NsdManager`
(mDNS, multicast 224.0.0.251) работает на этом линке и **не зависит от
`discoverServices()`**: нет `BUSY`, нет кэша supplicant, задержка — единицы
миллисекунд. Формат записей можно сохранить прежний (те же `id`, `mid`, `msg`),
так что это совместимая замена транспорта.

```java
NsdServiceInfo svc = new NsdServiceInfo();
svc.setServiceName("wfd-" + shortDeviceId);
svc.setServiceType("_wfd._tcp");
svc.setPort(P2pConfig.SOCKET_PORT);
svc.setAttribute("id", shortDeviceId.getBytes(StandardCharsets.UTF_8));
nsdManager.registerService(svc, NsdManager.PROTOCOL_DNS_SD, registrationListener);
```

Практический смысл: presence и «есть новое сообщение» перестают зависеть от
медленного канала, а DNS-SD остаётся только для **первого** контакта.

**B3. UDP-broadcast от владельца группы к клиентам.**
В star-топологии GO может отправить один пакет всем: групповое сообщение
уходит одному адресату на уровне приложения, а не N копий по TCP.

**B4. Бинарный кадр вместо base64.**
Base64 раздувает нагрузку на 33 %. Формат:
`[тип 1 Б][флаги 1 Б][длина 2 Б][payload][CRC32 4 Б]`, текст — UTF-8 без
кодирования. Меньше байт — меньше эфирного времени, плюс CRC для явного
обнаружения порчи.

**B5. Немедленный ACK и piggyback.**
Публиковать ACK сразу после приёма (с троттлингом 300 мс), а в каждом исходящем
кадре всегда нести до 5 последних подтверждений — как TCP ACK. Отдельный
«пакет подтверждения» почти никогда не нужен.

**B6. Ретраи 1→2→4→8 с вместо фиксированных 15 с.**
Серия коротких повторов «догоняет» собеседника, который как раз проснулся.
Суммарно 15 с вместо 45 и в 3 раза меньше «висящих» сообщений.

**B7. Keep-alive против power save.**
Пока есть подключённый пир — раз в 5 с служебный пакет (он же heartbeat
присутствия). Тогда радио не уходит в глубокую экономию и первый значащий пакет
не ждёт пробуждения.

**B8. Адаптивные таймауты (EWMA RTT).**
Держим по пиру сглаженную оценку RTT и выводим из неё таймаут ACK
(как RTO в TCP: `RTO = RTT_avg + 4·RTT_dev`). На плохом канале приложение
перестаёт ждать «наугад».

### Блок C. Гарантированная доставка

**C1. At-least-once + дедупликация (уже есть).** `msgId` генерирует отправитель,
приёмник дедуплицирует по `processedMessageIds`. Ретрай не создаёт дубль —
именно это и есть «гарантированная доставка» на радио.

**C2. Bloom-фильтр в SYNC вместо списков ID.**
128 бит (16 байт, base64 → 22 байта) вмещают ~20 ID при ложноположительных ~2 %.
Сейчас в `WFD_Sync` уезжает 3–4 ID и запись упирается в лимит 240 байт.

```java
BitSet bloom = new BitSet(128);
for (String id : myRecvIds) {
    bloom.set(hash1(id) & 127);
    bloom.set(hash2(id) & 127);
}
record.put("bloom", Base64.getEncoder().encodeToString(bloom.toByteArray()));
```

«В фильтре нет» = гарантированно не дошло → переотправка. «Есть» = скорее всего
дошло (точную сверку при сомнении делаем по сокету полным списком).

**C3. Быстрый SYNC для неподтверждённых.**
`SYNC_CHECK_INTERVAL` 10 с → 5 с, если есть неподтверждённые сообщения;
60 с оставить только для фоновой сверки «всё ли сошлось».

**C4. Anti-entropy при каждом контакте.**
Как только группа/сокет поднялись — сразу одним обменом выяснить, чего не хватает
обеим сторонам, не дожидаясь таймеров. Именно так работает Briar: синхронизация
состояния при встрече, а не «доставка по расписанию».

**C5. Приоритеты и TTL.**
Очередь отправки: короткие сообщения и ACK — вперёд, длинные — после. Плюс TTL
для переотправок, чтобы «мёртвое» сообщение не занимало слот бесконечно
(уже частично сделано через `MAX_ACK_ATTEMPTS`).

**C6. (Опционально) ретрансляция через промежуточные устройства.**
Если нужна дальность больше одного хопа — это делается **на том же Wi-Fi Direct**,
без Bluetooth: устройство-посредник принимает сообщение и переопубликовывает.
Даёт дальность, но добавляет задержку и требует TTL + дубликат-кэш (у нас есть
`processedMessageIds`). Предлагаю как отдельный режим, не по умолчанию.

### Блок D. Совместимость и живучесть: Xiaomi, Redmi, vivo, realme, Oppo

**D1. Location Mode должен быть включён — это не про разрешения.**
Документация Android прямо говорит: `discoverPeers`, `discoverServices` и
`requestPeers` требуют **включённого режима определения местоположения**
([Use Wi-Fi Direct (P2P) for service discovery](https://developer.android.com/develop/connectivity/wifi/nsd-wifi-direct)).
На MIUI и Funtouch это самая частая причина «список устройств пустой»: разрешение
выдано, а переключатель «Геолокация» выключен. Нужна явная проверка и подсказка
со ссылкой на настройки:

```java
LocationManager lm = (LocationManager) getSystemService(LOCATION_SERVICE);
boolean locationOn = lm != null && lm.isLocationEnabled();

if (!locationOn) {
    // показать объяснение и кнопку → Settings.ACTION_LOCATION_SOURCE_SETTINGS
}
```

Ставить это в статус-бар приложения, а не только в логи.

**D2. Агрессивное энергосбережение вендоров.**
На MIUI/HyperOS (Xiaomi/Redmi/Poco), Funtouch/OriginOS (vivo/iQOO), ColorOS (Oppo,
realme) сервис убивают, если приложению не разрешён автозапуск. Уже добавлен
запрос исключения из оптимизации батареи (PR #2); дополнительно нужен
экран-инструкция с переходами в настройки вендора
(`ACTION_APPLICATION_DETAILS_SETTINGS`, OEM-специфичные Intent'ы — с проверкой
на `ActivityNotFoundException`) и напоминание «закрепите приложение в списке
последних задач».

**D3. Не использовать скрытые API.**
`setDeviceName`, `deletePersistentGroup` и подобное — рефлексия, которая на
Android 9+ блокируется (уже убрано в PR #2). Идентификация — только по `deviceId`
из TXT/mDNS.

**D4. Feature detection вместо предположений.**
`PackageManager.hasSystemFeature(PackageManager.FEATURE_WIFI_DIRECT)`,
проверка `WifiP2pManager != null`, `WifiP2pManager.WIFI_P2P_STATE_ENABLED`,
`WifiManager.isWifiEnabled()`. Если P2P выключен политикой (MDM, родительский
контроль), приложение должно сказать это словами, а не «искать вечно».

**D5. Диапазон группы по сценарию.**
`GROUP_OWNER_BAND_2GHZ` — по умолчанию (дальше, лучше пробивает препятствия);
`GROUP_OWNER_BAND_5GHZ` — переключатель «быстрее, но ближе» для тесной группы.
Не полагаться на 5 ГГц как на единственный вариант: на бюджетных Redmi/vivo
он не всегда доступен в P2P.

**D6. Local Only Hotspot — альтернатива P2P-группе, где она капризничает.**
`WifiManager.startLocalOnlyHotspot()` (API 26+) поднимает точку доступа без
интернета; клиент подключается через `WifiNetworkSpecifier`. Плюс: нет
GO-негоциации и «принять приглашение» на стороне владельца. Минус: на части
прошивок точка доступа рвёт домашний Wi-Fi и/или требует подтверждения, поэтому
это **резервный путь** с проверкой доступности и явным откатом на обычный P2P.

**D7. Меньше сервисов — быстрее выдача.**
Держать одновременно не 6 записей, а 2–3: `WFD_Main` (presence + ACK) и
`WFD_Msg` (сообщения). `WFD_Sync` публиковать только когда реально нужна сверка.
Меньше churn supplicant — стабильнее и быстрее приходят остальные записи.

**D8. Обновлять `WFD_Main` реже.**
Сейчас remove+add каждые 5 с. Предлагаю: обновление раз в 10 с, а срочное — только
при изменении набора ACK. Присутствие всё равно видно по слотам и heartbeat
в `WFD_Msg`.

---

## 5. Матрица приоритетов

| # | Оптимизация | Усилие | Эффект | Риск |
|---|---|---|---|---|
| A1 | Событийный discovery | S | p50 4 с → 1–2 с | батарея |
| B5 | Немедленный ACK + piggyback | S | −2…5 с | чаще публикации |
| B6 | Ретраи 1→2→4→8 с | S | p95 45 с → ~10 с | больше трафика |
| D1 | Проверка Location Mode | S | убирает «пустой список» на MIUI/vivo | нет |
| A3 | Синхронизированные окна | M | встреча предсказуема, p50 < 1 с внутри окна | нужна общая шкала (есть) |
| A4 | Поллинг `requestPeers` | S | меньше `BUSY`, дешевле | нет |
| A6+A7 | Persistent group + не рвать группу | M | повторный контакт 3–9 с → ~1 с | память групп в системе |
| B2 | mDNS на линке группы | M | presence за мс, без `BUSY` | новая зависимость |
| B1 | Сокеты как основной путь | S | сообщение за мс | включение режима (PR #2) |
| C2 | Bloom-SYNC | M | сходимость 60 с → 10 с, −80 % байт | ложные срабатывания |
| B4 | Бинарный кадр | M | −25…35 % байт | совместимость версий |
| B8 | EWMA RTT / адаптивные таймауты | M | меньше «напрасных» ожиданий | — |
| D6 | Local Only Hotspot как резерв | M | обход проблемных P2P-стеков | нестабильно на части прошивок |
| C6 | Ретрансляция через посредника | L | дальность > 1 хопа | задержка, TTL-логика |

---

## 6. Ожидаемые метрики

| Метрика | Сейчас | После S-пунктов (A1, B5, B6, D1) | После A6/A7 + B1/B2 (группа удерживается) |
|---|---|---|---|
| Первый контакт | 1,5–15 с | 1–3 с | ~1 с (persistent group) |
| Отправка → ACK, p50 | 8–12 с | 2–4 с | **0,3–1 с** |
| Отправка → ACK, p95 | 20–30 с | 5–8 с | 2–4 с |
| Сходимость после потери ACK | до 60 с | 10–15 с | 5–10 с |
| Доля доставленных при 20 % потерь | низкая | средняя | высокая |

Оценки — из бюджета задержки в п. 2.1 и литературных данных о Wi-Fi Direct;
их нужно подтвердить измерением (п. 8), а не принимать на веру.

---

## 7. Чего делать не стоит

* **Уменьшать период `discoverServices` до сотен миллисекунд** — `BUSY`, и часть
  прошивок прервёт текущий поиск.
* **Тянуть BLE/NAN «для скорости»** — NAN нет на всех чипсетах, Bluetooth проигрывает
  по дальности; выигрыш не окупает потерю совместимости.
* **Полагаться на 5 ГГц** как на обязательный диапазон.
* **Делать mesh-вещание без TTL и дедупликации** — один шторм забивает эфир.
* **Гоняться за средним.** Оптимизировать надо p95: пользователь помнит
  «иногда висит 40 секунд».
* **Строить гарантии на блокировках и ожидании.** Правильная модель —
  at-least-once + дедупликация по `msgId` (уже есть).

---

## 8. Как измерить результат

1. Два телефона (обязательно включить два разных: например, Xiaomi и vivo),
   приложение запущено на обоих, Location Mode включён.
2. Отправить 30 сообщений по 20–40 символов с интервалом 5 с.
3. Из `DiagnosticLogger` взять метки `SEND MESSAGE`, `Message slot N registered`,
   `MESSAGE RECEIVED`, `ACK received`, `ACK published #N`.
4. Считать `t_ack − t_send`: p50, p95, долю доставки с первой попытки, число
   ретрансмиссий.
5. Повторить трижды: оба экрана включены; экран получателя погашен; расстояние
   20–30 м.

Целевые SLO: **p50 ≤ 2 с, p95 ≤ 6 с, доставка ≥ 99 % за 30 с** на текущем
транспорте; после удержания группы — p50 ≤ 1 с.

---

## 9. Порядок внедрения

1. **Шаг 1 (полдня, без новых зависимостей):** A1, A4, B5, B6, D1, D7, D8.
   Только правки внутри существующего менеджера + проверка Location Mode.
2. **Шаг 2 (1–2 дня):** B4, C2, B8, п. 8 (замеры до/после) — иначе шаг 3 не с чем
   сравнивать.
3. **Шаг 3 (2–3 дня):** A6, A7, A5, B1 — удержание группы и сокеты как основной
   путь. Здесь же переключатель «быстрый режим» в настройках.
4. **Шаг 4 (по результатам):** B2 (mDNS на линке группы), A3 (окна), D6
   (Local Only Hotspot как резерв), при необходимости C6 (ретрансляция).

---

## 10. Источники

* Wi-Fi Direct: обнаружение 1,48–15,35 с, группа 3–9 с —
  [IJERT](https://www.ijert.org/android-wi-fi-direct-architecture-from-protocol-implementation-to-formal-specification)
* Требование включённого Location Mode для `discoverPeers`/`discoverServices`/
  `requestPeers` —
  [Android Developers: Use Wi-Fi Direct (P2P) for service discovery](https://developer.android.com/develop/connectivity/wifi/nsd-wifi-direct),
  [Wi-Fi scanning overview](https://developer.android.com/develop/connectivity/wifi/wifi-scan)
* Группа по SSID+паролю «without negotiation and WPS» —
  [AOSP `WifiP2pServiceImpl`](https://cs.android.com/android/platform/superproject/+/master:packages/modules/Wifi/service/java/com/android/server/wifi/p2p/WifiP2pServiceImpl.java)
* Persistent group (`enablePersistentMode`, `setGroupOperatingBand`) —
  [WifiP2pConfig.Builder](https://developer.android.com/reference/android/net/wifi/p2p/WifiP2pConfig.Builder),
  [Android 10 features](https://developer.android.com/about/versions/10/features),
  [wpa_supplicant README-P2P](https://android.googlesource.com/platform/external/wpa_supplicant_8/+/master/wpa_supplicant/README-P2P)
* Практика MIUI: «location services must be enabled for wifi direct to work»,
  ограничения MIUI —
  [r/Xiaomi](https://www.reddit.com/r/Xiaomi/comments/etqlg2/mi_9t_unable_to_use_wifi_direct/),
  [Stack Overflow: discovery failed on Android 10](https://stackoverflow.com/questions/63188989/why-wifi-direct-discovery-getting-failed-on-android-10)
* Принцип синхронных окон (только как обоснование, не как внедряемая технология) —
  [Apple AWDL, arXiv 1808.03156](https://arxiv.org/html/1808.03156v1)
* Briar/Bramble: синхронизация состояния при контакте, per-transport latency —
  [how it works](https://briarproject.org/how-it-works/),
  [survey TUM](https://www.net.in.tum.de/fileadmin/TUM/NET/NET-2021-05-1/NET-2021-05-1_01.pdf)
* NEARBY_WIFI_DEVICES vs ACCESS_FINE_LOCATION (что нужно на Android 13+) —
  [Android Developers](https://developer.android.com/develop/connectivity/wifi/wifi-permissions)
