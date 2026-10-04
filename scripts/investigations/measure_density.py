"""증분 덤프 1일분을 스트리밍으로 훑어 이벤트 밀도와 스키마 다양성을 실측한다.

압축을 디스크에 풀지 않는다. 2.6GB를 그대로 두고 읽으면서 집계한다.

    FLUX_STORAGE_ROOT=... python scripts/investigations/measure_density.py [--limit N]
"""

import argparse
import json
import sys
import tarfile
from collections import Counter
from compression import zstd
from datetime import datetime, timezone
from pathlib import Path

from flux.common import config


def find_dump(root: Path) -> Path:
    base = root / "dumps" / "listenbrainz" / "incremental"
    candidates = sorted(base.glob("*/listenbrainz-listens-dump-*.tar.zst"))
    if not candidates:
        raise SystemExit(f"덤프를 찾지 못했다: {base}")
    return candidates[-1]


class Stats:
    def __init__(self):
        self.records = 0
        self.raw_bytes = 0
        self.hourly = Counter()
        self.info_combos = Counter()
        self.info_keys = Counter()
        self.top_level_keys = Counter()
        self.with_mbid_mapping = 0
        self.with_additional_info = 0
        self.min_ts = None
        self.max_ts = None

    def add(self, line: bytes) -> None:
        self.raw_bytes += len(line)
        record = json.loads(line)
        self.records += 1
        self.top_level_keys[tuple(sorted(record))] += 1

        ts = record.get("timestamp") or record.get("listened_at")
        if ts is not None:
            self.min_ts = ts if self.min_ts is None else min(self.min_ts, ts)
            self.max_ts = ts if self.max_ts is None else max(self.max_ts, ts)
            hour = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H")
            self.hourly[hour] += 1

        meta = record.get("track_metadata") or {}
        if "mbid_mapping" in meta:
            self.with_mbid_mapping += 1

        info = meta.get("additional_info")
        if isinstance(info, dict):
            self.with_additional_info += 1
            self.info_combos[tuple(sorted(info))] += 1
            self.info_keys.update(info.keys())

    def report(self, window_seconds: float | None = None) -> dict:
        """window_seconds: 덤프가 덮는 제출 구간. 밀도는 이 값으로 나눈다.

        레코드의 timestamp는 '청취 시각'이라 과거 기록이 섞인다.
        제출 밀도(초당 몇 건이 들어오는가)와 청취 시각 분포는 다른 이야기다.
        """
        peak_hour, peak_count = max(self.hourly.items(), key=lambda kv: kv[1]) if self.hourly else ("-", 0)
        listen_span = (self.max_ts - self.min_ts) if self.min_ts and self.max_ts else 0
        return {
            "records": self.records,
            "uncompressed_bytes": self.raw_bytes,
            "avg_bytes_per_record": round(self.raw_bytes / self.records, 1) if self.records else 0,
            "submission_window_seconds": round(window_seconds) if window_seconds else None,
            "submission_per_second": round(self.records / window_seconds, 1) if window_seconds else None,
            "listen_span_seconds": listen_span,
            "listen_span_days": round(listen_span / 86400, 1),
            "oldest_listen": datetime.fromtimestamp(self.min_ts, timezone.utc).isoformat() if self.min_ts else None,
            "newest_listen": datetime.fromtimestamp(self.max_ts, timezone.utc).isoformat() if self.max_ts else None,
            "peak_listen_hour": peak_hour,
            "peak_listen_hour_records": peak_count,
            "listen_hours_covered": len(self.hourly),
            "distinct_additional_info_combos": len(self.info_combos),
            "distinct_additional_info_keys": len(self.info_keys),
            "additional_info_ratio": round(self.with_additional_info / self.records, 4) if self.records else 0,
            "mbid_mapping_ratio": round(self.with_mbid_mapping / self.records, 4) if self.records else 0,
            "distinct_top_level_shapes": len(self.top_level_keys),
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="이 개수만 읽고 멈춘다 (시험용)")
    args = parser.parse_args()

    dump = find_dump(config.local_root())
    print(f"덤프: {dump.name}")
    print(f"압축 크기: {dump.stat().st_size:,} 바이트\n")

    stats = Stats()
    meta_files = {}

    with zstd.ZstdFile(dump) as z, tarfile.open(fileobj=z, mode="r|") as tar:
        for member in tar:
            base = member.name.rsplit("/", 1)[-1]
            if base in ("SCHEMA_SEQUENCE", "START_TIMESTAMP", "END_TIMESTAMP"):
                meta_files[base] = tar.extractfile(member).read().decode().strip()
                continue
            if not base.endswith(".listens"):
                continue

            stream = tar.extractfile(member)
            for line in stream:
                if not line.strip():
                    continue
                stats.add(line)
                if stats.records % 500_000 == 0:
                    print(f"  {stats.records:,}건 ({stats.raw_bytes / 1024**3:.2f}GB)", flush=True)
                if args.limit and stats.records >= args.limit:
                    break
            break

    window = None
    if {"START_TIMESTAMP", "END_TIMESTAMP"} <= meta_files.keys():
        start = datetime.fromisoformat(meta_files["START_TIMESTAMP"])
        end = datetime.fromisoformat(meta_files["END_TIMESTAMP"])
        window = (end - start).total_seconds()

    result = stats.report(window)
    result["dump"] = dump.name
    result["compressed_bytes"] = dump.stat().st_size
    if args.limit is None:
        result["compression_ratio"] = round(result["uncompressed_bytes"] / result["compressed_bytes"], 2)
    else:
        result["compression_ratio"] = None  # 일부만 읽었으므로 계산하지 않는다
    result.update({f"meta_{k.lower()}": v for k, v in meta_files.items()})

    print("\n" + "=" * 58)
    for key, value in result.items():
        shown = f"{value:,}" if isinstance(value, int) else value
        print(f"{key:34} {shown}")
    print("=" * 58)

    path = config.write_measurement(f"density-{dump.name.split('-')[3]}", result)
    print(f"\n기록: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
