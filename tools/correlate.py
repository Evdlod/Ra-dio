#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Склейка журналов корреляции Ra-dio с двух телефонов и расчёт метрик доставки.

Использование:
    python3 tools/correlate.py corr_A.tsv corr_B.tsv [--verbose]

Ожидается формат из приложения (CorrelationLog):

    #ra-dio-correlation v1
    #columns: t_utc_ms t_iso t_mono_ms dev sid event trace peer seq detail
    1761400000123 2026-10-25T12:13:20.123 51234 a1b2c3d4 7f3c MSG_SEND ... 1 slot=0 fast=1

Скрипт ничего не отправляет в сеть, зависимостей не требует (только stdlib),
работает с любым числом файлов: строки просто складываются в общий пул.
"""

import argparse
import statistics
import sys
from collections import Counter, defaultdict

COLUMNS = ["t_utc_ms", "t_iso", "t_mono_ms", "dev", "sid",
           "event", "trace", "peer", "seq", "detail"]


def parse_file(path):
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < len(COLUMNS):
                # Терпим к усечённым строкам (обрезанный при выгрузке хвост)
                parts += [""] * (len(COLUMNS) - len(parts))
            row = dict(zip(COLUMNS, parts[:len(COLUMNS)]))
            try:
                row["t_utc_ms"] = int(row["t_utc_ms"])
                row["t_mono_ms"] = int(row["t_mono_ms"])
            except ValueError:
                continue
            row["detail_map"] = parse_detail(row.get("detail", ""))
            rows.append(row)
    return rows


def parse_detail(detail):
    result = {}
    for chunk in (detail or "").split(" "):
        if "=" in chunk:
            key, _, value = chunk.partition("=")
            result[key] = value
    return result


def pct(values, q):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def fmt_ms(value):
    return "—" if value is None else "%.0f мс" % value


def analyze(rows, verbose=False):
    by_trace = defaultdict(list)
    for row in rows:
        if row["trace"] and row["trace"] != "-":
            by_trace[row["trace"]].append(row)

    send_recv, send_ack, recv_ack = [], [], []
    channels = Counter()
    dup = 0
    txt_published = 0
    txt_skipped = 0
    incomplete = []

    for trace, events in by_trace.items():
        by_event = defaultdict(list)
        for e in events:
            by_event[e["event"]].append(e)

        send = by_event.get("MSG_SEND")
        recv = by_event.get("MSG_RX")
        ack_rx = by_event.get("ACK_RX")

        if send and recv:
            delta = recv[0]["t_utc_ms"] - send[0]["t_utc_ms"]
            if -5_000 < delta < 300_000:
                send_recv.append(delta)
                channels[recv[0]["detail_map"].get("channel", "?")] += 1
        elif send and not recv:
            incomplete.append((trace, "нет MSG_RX"))

        if send and ack_rx:
            delta = ack_rx[0]["t_utc_ms"] - send[0]["t_utc_ms"]
            if -5_000 < delta < 300_000:
                send_ack.append(delta)

        if recv and ack_rx:
            delta = ack_rx[0]["t_utc_ms"] - recv[0]["t_utc_ms"]
            if -5_000 < delta < 300_000:
                recv_ack.append(delta)

        dup += len(by_event.get("MSG_DUP", []))
        for pub in by_event.get("TXT_PUB", []):
            if pub["detail_map"].get("result") == "published":
                txt_published += 1
            else:
                txt_skipped += 1

    devices = sorted({r["dev"] for r in rows})
    models = {}
    for r in rows:
        if r["event"] == "START":
            env = r["detail_map"].get("model")
            if env:
                models[r["dev"]] = env

    print("=" * 68)
    print("Ra-dio: анализ корреляции")
    print("=" * 68)
    print("Телефоны: %s" % ", ".join("%s%s" % (d, " (%s)" % models[d] if d in models else "")
                                     for d in devices))
    print("Строк в журналах: %d, цепочек (trace): %d" % (len(rows), len(by_trace)))
    print()

    def report(name, values):
        if not values:
            print("  %-22s нет парных событий" % name)
            return
        print("  %-22s n=%d  p50=%s  p90=%s  p95=%s  max=%s" % (
            name, len(values), fmt_ms(pct(values, 0.50)),
            fmt_ms(pct(values, 0.90)), fmt_ms(pct(values, 0.95)),
            fmt_ms(max(values))))

    print("Задержки:")
    report("send → recv", send_recv)
    report("recv → ack", recv_ack)
    report("send → ack", send_ack)
    print()

    print("Каналы доставки (по MSG_RX):")
    total = sum(channels.values()) or 1
    for channel, count in channels.most_common():
        print("  %-14s %4d  (%.0f %%)" % (channel, count, 100.0 * count / total))
    print()

    print("Резервный TXT-путь:")
    print("  опубликовано слотов: %d" % txt_published)
    print("  пропущено (быстрый ACK): %d" % txt_skipped)
    if txt_published + txt_skipped:
        share = 100.0 * txt_skipped / (txt_published + txt_skipped)
        print("  доля быстрого пути: %.0f %% %s" % (
            share, "(норма)" if share >= 80 else "(быстрый канал работает не всегда)"))
    print()

    if dup:
        print("Дубликаты: %d (второй канал пришёл позже — это ожидаемо)" % dup)
    if incomplete:
        print("Незавершённые цепочки: %d" % len(incomplete))
        for trace, why in incomplete[:10]:
            print("  %s: %s" % (trace, why))
        if verbose:
            for trace, why in incomplete[10:]:
                print("  %s: %s" % (trace, why))

    # Расхождение часов: сравниваем метки из событий MSG_TX/MSG_RX одной цепочки
    offsets = []
    for trace, events in by_trace.items():
        sent = [e for e in events if e["event"] == "MSG_TX"]
        recv = [e for e in events if e["event"] == "MSG_RX"]
        if sent and recv:
            offsets.append(recv[0]["t_utc_ms"] - sent[0]["t_utc_ms"])
    if offsets:
        print()
        print("Смещение часов между телефонами (оценка): медиана %.0f мс, разброс %.0f мс"
              % (statistics.median(offsets), max(offsets) - min(offsets)))

    print()
    print("Как читать: p50 send→ack и есть то время, которое пользователь")
    print("воспринимает как «сообщение дошло». Цель — ≤ 2000 мс при p95 ≤ 6000 мс.")


def main():
    parser = argparse.ArgumentParser(
        description="Склейка журналов корреляции Ra-dio с двух телефонов")
    parser.add_argument("files", nargs="+", help="файлы corr_*.tsv")
    parser.add_argument("--verbose", action="store_true", help="печатать все цепочки")
    args = parser.parse_args()

    rows = []
    for path in args.files:
        try:
            rows.extend(parse_file(path))
        except OSError as exc:
            print("Не удалось прочитать %s: %s" % (path, exc), file=sys.stderr)
            return 1

    if not rows:
        print("В файлах нет данных формата corr_*.tsv", file=sys.stderr)
        return 1

    analyze(rows, args.verbose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
