#!/usr/bin/env python3
"""
smart_collector.py — Ceph 다중 클러스터 HDD SMART 수집기 (단일 파일)

[수집]  SSH → smartctl 원격 실행 → raw JSON 그대로 JSONL 저장
[변환]  JSONL → Backblaze 호환 CSV 변환 (수집 직후 또는 일괄 변환 가능)

사용법:
    # 1. 수집 (raw JSONL 저장)
    python3 smart_collector.py collect -c config.yaml
    python3 smart_collector.py collect -c config.yaml --cluster cluster-prod-01
    python3 smart_collector.py collect -c config.yaml --dry-run

    # 2. 변환 (JSONL → Backblaze 호환 CSV)
    python3 smart_collector.py convert -i datasets/hourly/202604/smart_20260406_1400.jsonl
    python3 smart_collector.py convert -i "datasets/hourly/202604/*.jsonl" -o merged.csv

    # 3. 초기 세팅 (원격 노드에 smartmontools 설치 + sudoers 설정)
    python3 smart_collector.py setup -c config.yaml

의존성:
    pip install asyncssh pyyaml
"""

import asyncio
import asyncssh
import json
import csv
import os
import sys
import yaml
import logging
import argparse
import hashlib
import glob
import smtplib
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage
from typing import Optional
from dataclasses import dataclass, field


# =============================================================================
# 설정
# =============================================================================

class Config:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self._raw = yaml.safe_load(f)

        g = self._raw.get("global", {})
        self.output_base = Path(g.get("output_base_dir", "./datasets"))
        self.log_dir = Path(g.get("log_dir", "./logs"))
        self.log_level = g.get("log_level", "INFO")

        s = self._raw.get("ssh", {})
        self.ssh_user = s.get("username", "root")
        self.ssh_key = os.path.expanduser(s.get("key_path", "~/.ssh/id_rsa"))
        self.ssh_connect_timeout = s.get("connect_timeout", 10)
        self.ssh_command_timeout = s.get("command_timeout", 120)

        p = self._raw.get("parallelism", {})
        self.max_nodes_per_cluster = p.get("max_concurrent_nodes_per_cluster", 50)
        self.max_total_sessions = p.get("max_total_concurrent_sessions", 200)
        self.max_disks_per_node = p.get("max_concurrent_disks_per_node", 10)
        self.stagger_delay = p.get("connection_stagger_delay", 0.05)

        self.schedule_cfg = self._raw.get("schedule", {})
        self.notify_cfg = self._raw.get("notification", {})
        self.smtp_password = os.environ.get("SMTP_APP_PASSWORD", "")
        self.clusters = self._raw.get("clusters", [])

        self.output_base.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def get_ssh_for_cluster(self, cluster: dict) -> dict:
        ov = cluster.get("ssh_override", {}) or {}
        return {
            "username": ov.get("username") or self.ssh_user,
            "key_path": os.path.expanduser(ov.get("key_path") or self.ssh_key),
        }


# =============================================================================
# 로깅
# =============================================================================

def setup_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger("smart_collector")
    logger.setLevel(getattr(logging, config.log_level))

    fmt = logging.Formatter("[%(asctime)s] %(levelname)-7s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    log_file = config.log_dir / f"collector_{datetime.now():%Y%m%d}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# =============================================================================
# 데이터 모델
# =============================================================================

@dataclass
class NodeResult:
    cluster: str
    hostname: str
    success: bool = False
    drives: list = field(default_factory=list)     # raw smartctl JSON 리스트
    errors: list = field(default_factory=list)
    duration: float = 0.0


# =============================================================================
# 수집 (collect)
# =============================================================================

async def collect_node(
    cluster_name: str,
    hostname: str,
    ssh_cfg: dict,
    config: Config,
    global_sem: asyncio.Semaphore,
    cluster_sem: asyncio.Semaphore,
    logger: logging.Logger,
) -> NodeResult:
    """단일 노드: SSH 접속 → smartctl --scan → 각 디스크 smartctl -a -j"""

    result = NodeResult(cluster=cluster_name, hostname=hostname)

    async with global_sem, cluster_sem:
        t0 = asyncio.get_event_loop().time()
        try:
            conn_opts = {
                "host": hostname,
                "username": ssh_cfg["username"],
                "client_keys": [ssh_cfg["key_path"]],
                "known_hosts": None,          # 내부망
                "connect_timeout": config.ssh_connect_timeout,
            }

            async with asyncssh.connect(**conn_opts) as conn:
                # ── 1) 드라이브 목록 스캔 ──
                scan_cmd = "sudo smartctl --scan -j"
                scan_result = await asyncio.wait_for(
                    conn.run(scan_cmd, check=False),
                    timeout=30
                )
                scan_json = json.loads(scan_result.stdout)
                devices = scan_json.get("devices", [])

                if not devices:
                    result.errors.append("no devices found")
                    return result

                # ── 2) 각 디스크 smartctl -a -j 병렬 실행 ──
                disk_sem = asyncio.Semaphore(config.max_disks_per_node)

                async def get_smart(dev: dict) -> Optional[dict]:
                    dev_name = dev.get("name", "")
                    dev_type = dev.get("type", "")
                    # nice 19 + ionice idle → 서버 부하 방지
                    cmd = f"nice -n 19 ionice -c 3 sudo smartctl -a -j"
                    if dev_type:
                        cmd += f" -d {dev_type}"
                    cmd += f" {dev_name}"

                    async with disk_sem:
                        try:
                            r = await asyncio.wait_for(
                                conn.run(cmd, check=False),
                                timeout=config.ssh_command_timeout
                            )
                            raw = json.loads(r.stdout)
                            # 디바이스 경로를 명시적으로 삽입
                            raw["_collected_device"] = dev_name
                            raw["_collected_type"] = dev_type
                            return raw
                        except Exception as e:
                            result.errors.append(f"{dev_name}: {e}")
                            return None

                tasks = [get_smart(d) for d in devices]
                disk_results = await asyncio.gather(*tasks)

                result.drives = [r for r in disk_results if r is not None]
                result.success = True

                logger.info(
                    f"[{cluster_name}/{hostname}] "
                    f"{len(result.drives)}/{len(devices)} disks 수집 완료"
                )

        except asyncssh.Error as e:
            result.errors.append(f"ssh: {e}")
            logger.error(f"[{cluster_name}/{hostname}] SSH 오류: {e}")
        except asyncio.TimeoutError:
            result.errors.append("timeout")
            logger.error(f"[{cluster_name}/{hostname}] 타임아웃")
        except json.JSONDecodeError as e:
            result.errors.append(f"json: {e}")
            logger.error(f"[{cluster_name}/{hostname}] JSON 파싱 실패")
        except Exception as e:
            result.errors.append(str(e))
            logger.error(f"[{cluster_name}/{hostname}] 예외: {e}")
        finally:
            result.duration = round(asyncio.get_event_loop().time() - t0, 2)

    return result


async def collect_cluster(
    cluster: dict,
    config: Config,
    global_sem: asyncio.Semaphore,
    logger: logging.Logger,
) -> list:
    """클러스터 내 모든 노드 병렬 수집"""

    name = cluster["name"]
    nodes = cluster.get("nodes", [])
    ssh_cfg = config.get_ssh_for_cluster(cluster)
    cluster_sem = asyncio.Semaphore(config.max_nodes_per_cluster)

    logger.info(f"[{name}] {len(nodes)}개 노드 수집 시작")

    tasks = []
    for i, hostname in enumerate(nodes):
        if i > 0 and config.stagger_delay > 0:
            await asyncio.sleep(config.stagger_delay)
        tasks.append(collect_node(
            name, hostname, ssh_cfg, config,
            global_sem, cluster_sem, logger
        ))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    out = []
    for r in results:
        if isinstance(r, Exception):
            out.append(NodeResult(cluster=name, hostname="?", errors=[str(r)]))
        else:
            out.append(r)
    return out


async def run_collect(config: Config, logger: logging.Logger,
                      target_cluster: str = None, dry_run: bool = False):
    """메인 수집 루프"""

    now = datetime.now()
    clusters = config.clusters
    if target_cluster:
        clusters = [c for c in clusters if c["name"] == target_cluster]

    total_nodes = sum(len(c.get("nodes", [])) for c in clusters)
    logger.info(f"수집 시작: {len(clusters)}개 클러스터, {total_nodes}개 노드")

    if dry_run:
        for c in clusters:
            for n in c.get("nodes", []):
                logger.info(f"  [DRY RUN] {c['name']}/{n}")
        return

    global_sem = asyncio.Semaphore(config.max_total_sessions)

    # 모든 클러스터 동시 수집
    cluster_tasks = [
        collect_cluster(c, config, global_sem, logger) for c in clusters
    ]
    all_results_nested = await asyncio.gather(*cluster_tasks, return_exceptions=True)

    # 결과 합치기
    all_results = []
    for cr in all_results_nested:
        if isinstance(cr, Exception):
            logger.error(f"클러스터 수집 실패: {cr}")
        else:
            all_results.extend(cr)

    # ── JSONL 저장 (raw) ──
    out_dir = config.output_base / "hourly" / f"{now:%Y%m}"
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / f"smart_{now:%Y%m%d_%H%M}.jsonl"

    collection_id = hashlib.sha256(
        f"{now.isoformat()}-{os.getpid()}".encode()
    ).hexdigest()[:12]

    drive_count = 0
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for nr in all_results:
            if not nr.success:
                continue
            for raw_json in nr.drives:
                # 수집 메타데이터만 최소한으로 추가
                record = {
                    "_meta": {
                        "timestamp": now.isoformat(),
                        "collection_id": collection_id,
                        "cluster": nr.cluster,
                        "hostname": nr.hostname,
                    },
                    "smartctl": raw_json,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                drive_count += 1

    logger.info(f"JSONL 저장: {jsonl_path} ({drive_count}개 드라이브)")

    # Daily 복사
    daily_hour = config.schedule_cfg.get("daily_snapshot_hour", 18)
    if now.hour == daily_hour:
        daily_dir = config.output_base / "daily" / f"{now:%Y}"
        daily_dir.mkdir(parents=True, exist_ok=True)
        daily_path = daily_dir / f"smart_daily_{now:%Y%m%d}.jsonl"
        import shutil
        shutil.copy2(jsonl_path, daily_path)
        logger.info(f"Daily 복사: {daily_path}")

    # 통계
    ok_nodes = sum(1 for r in all_results if r.success)
    fail_nodes = sum(1 for r in all_results if not r.success)
    logger.info(
        f"수집 완료: 노드 {ok_nodes}/{total_nodes} 성공, "
        f"드라이브 {drive_count}개 저장"
    )

    # 알림
    if config.notify_cfg.get("enabled"):
        send_notification(config, logger, now, ok_nodes, fail_nodes,
                          total_nodes, drive_count, jsonl_path, all_results)

    # 보존 정책
    cleanup_old_files(config, logger)


# =============================================================================
# 변환 (convert) — JSONL → CSV (Backblaze 호환 포맷)
# =============================================================================
#
# CSV 컬럼 구조:
#   [메타 컬럼]                       — Backblaze 기본 + Ceph 환경 확장
#   [ATA SMART: smart_{id}_normalized, smart_{id}_raw]  — Backblaze 동일
#   [SAS SCSI: scsi_{direction}_{metric}]               — SAS HDD 전용 확장
#   [SAS 기타: scsi_grown_defect_list, ...]             — SAS HDD 전용 확장
#
# Backblaze는 ATA HDD만 사용하므로 smart_{id} 컬럼만 있음.
# 우리는 SAS HDD도 있으므로, scsi_ 접두어 컬럼을 뒤에 추가.
# ATA 드라이브는 scsi_ 컬럼이 빈칸, SAS 드라이브는 smart_{id} 컬럼이 빈칸.

# Backblaze ATA SMART ID 목록 (현재 스키마 기준)
_BACKBLAZE_SMART_IDS = [
    1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18,
    22, 23, 24, 27, 71, 82, 90,
    160, 161, 163, 164, 165, 166, 167, 168, 169,
    170, 171, 172, 173, 174, 175, 176, 177, 178, 179,
    180, 181, 182, 183, 184, 187, 188, 189,
    190, 191, 192, 193, 194, 195, 196, 197, 198, 199,
    200, 201, 202, 206, 210, 211, 212, 218,
    220, 222, 223, 224, 225, 226,
    230, 231, 232, 233, 234, 235,
    240, 241, 242, 244, 245, 246, 247, 248,
    250, 251, 252, 254, 255,
]

# SAS SCSI error counter 방향 × 지표
_SCSI_DIRECTIONS = ["read", "write", "verify"]
_SCSI_ERROR_METRICS = [
    "errors_corrected_by_eccfast",
    "errors_corrected_by_eccdelayed",
    "errors_corrected_by_rereads_rewrites",
    "total_errors_corrected",
    "correction_algorithm_invocations",
    "total_uncorrected_errors",
    "gigabytes_processed",
]


def _extract_row(record: dict) -> dict:
    """JSONL 1줄(record) → Backblaze 호환 CSV 1행 dict 변환

    record 구조:
        {"_meta": {...}, "smartctl": {smartctl -a -j 원본}}
    """
    meta = record.get("_meta", {})
    s = record.get("smartctl", {})

    row = {}

    # ── 메타 컬럼 (Backblaze + Ceph 합집합) ──
    # Backblaze 기본
    row["date"] = meta.get("timestamp", "")
    row["serial_number"] = s.get("serial_number", "")
    row["model"] = s.get("model_name", "")
    cap = s.get("user_capacity", {})
    row["capacity_bytes"] = cap.get("bytes", "") if isinstance(cap, dict) else cap
    row["failure"] = 0
    row["datacenter"] = ""
    row["cluster_id"] = meta.get("cluster", "")
    row["vault_id"] = ""
    row["pod_id"] = meta.get("hostname", "")
    row["pod_slot_num"] = s.get("_collected_device", "")
    row["is_legacy_format"] = 0

    # Ceph 환경 확장 (Backblaze에 없는 필드)
    row["model_family"] = s.get("model_family", "")
    row["vendor"] = s.get("vendor", "")
    row["product"] = s.get("product", "")
    row["firmware_version"] = s.get("firmware_version", "")
    row["revision"] = s.get("revision", "")
    row["logical_unit_id"] = s.get("logical_unit_id", "")
    row["interface_type"] = s.get("_collected_type", "")
    row["health_status_passed"] = ""
    ss = s.get("smart_status", {})
    if isinstance(ss, dict) and "passed" in ss:
        row["health_status_passed"] = int(ss["passed"])
    row["temperature"] = ""
    temp = s.get("temperature", {})
    if isinstance(temp, dict):
        row["temperature"] = temp.get("current", "")
    row["power_on_hours"] = ""
    poh = s.get("power_on_time", {})
    if isinstance(poh, dict):
        row["power_on_hours"] = poh.get("hours", "")

    # ── ATA SMART: smart_{id}_normalized / smart_{id}_raw ──
    # ATA 데이터가 있는 드라이브만 ATA 컬럼 생성 (SAS 드라이브는 건너뜀)
    ata_table = s.get("ata_smart_attributes", {}).get("table", [])
    if ata_table:
        ata_by_id = {}
        for attr in ata_table:
            aid = attr.get("id")
            if aid is not None:
                ata_by_id[aid] = attr

        # Backblaze 기본 ID + 이 드라이브 고유 ID 합집합
        all_ata_ids = set(_BACKBLAZE_SMART_IDS) | set(ata_by_id.keys())

        for aid in sorted(all_ata_ids):
            attr = ata_by_id.get(aid)
            if attr:
                row[f"smart_{aid}_normalized"] = attr.get("value", "")
                raw = attr.get("raw", {})
                row[f"smart_{aid}_raw"] = raw.get("value", "") if isinstance(raw, dict) else raw
            else:
                row[f"smart_{aid}_normalized"] = ""
                row[f"smart_{aid}_raw"] = ""

    # ── SAS SCSI Error Counter Log ──
    # SAS 데이터가 있는 드라이브만 SCSI 컬럼 생성 (ATA 드라이브는 건너뜀)
    ecl = s.get("scsi_error_counter_log", {})
    has_scsi = bool(ecl) or "scsi_grown_defect_list" in s

    if has_scsi:
        for direction in _SCSI_DIRECTIONS:
            counters = ecl.get(direction, {})
            for metric in _SCSI_ERROR_METRICS:
                col = f"scsi_{direction}_{metric}"
                row[col] = counters.get(metric, "")

        # SAS 기타 지표
        row["scsi_grown_defect_list"] = s.get("scsi_grown_defect_list", "")
        row["scsi_non_medium_error_count"] = ecl.get("non_medium_error_count", "")

        ssc = s.get("scsi_start_stop_cycle_counter", {})
        row["scsi_start_stop_count"] = ssc.get("accumulated_start_stop_cycles", "")
        row["scsi_load_unload_count"] = ssc.get("accumulated_load_unload_cycles", "")

        pue = s.get("scsi_percentage_used_endurance_indicator", {})
        row["scsi_percentage_used_endurance"] = pue.get(
            "percentage_used_endurance_indicator", "")

    return row


def _build_column_order(rows: list) -> list:
    """전체 행에서 등장한 컬럼들을 순서대로 정렬.

    순서: [메타] → [SAS scsi_] → [ATA smart_{id}]
    자동 감지: 실제 데이터에 값이 있는 그룹만 포함
              (전부 SAS면 ATA 컬럼 생략, 전부 ATA면 SAS 컬럼 생략)
    """

    # 1) 메타 컬럼 고정 순서
    meta_order = [
        # Backblaze 기본
        "date", "serial_number", "model", "capacity_bytes", "failure",
        "datacenter", "cluster_id", "vault_id", "pod_id", "pod_slot_num",
        "is_legacy_format",
        # Ceph 확장
        "model_family", "vendor", "product", "firmware_version",
        "revision", "logical_unit_id", "interface_type",
        "health_status_passed", "temperature", "power_on_hours",
    ]

    # 2) 모든 행에서 등장한 키 수집
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())

    # 3) 실제 값이 존재하는지 확인 (빈 문자열만 있는 그룹은 제외)
    has_ata_data = False
    has_scsi_data = False
    for row in rows:
        if has_ata_data and has_scsi_data:
            break
        for k, v in row.items():
            if v == "" or v is None:
                continue
            if k.startswith("smart_") and not has_ata_data:
                has_ata_data = True
            elif k.startswith("scsi_") and not has_scsi_data:
                has_scsi_data = True

    ordered = []
    used = set()

    # 메타 컬럼 먼저
    for col in meta_order:
        if col in all_keys:
            ordered.append(col)
            used.add(col)

    # 4) SAS scsi_ 컬럼 먼저 (데이터가 있을 때만)
    if has_scsi_data:
        scsi_fixed_order = []
        for direction in _SCSI_DIRECTIONS:
            for metric in _SCSI_ERROR_METRICS:
                scsi_fixed_order.append(f"scsi_{direction}_{metric}")
        scsi_fixed_order.extend([
            "scsi_grown_defect_list", "scsi_non_medium_error_count",
            "scsi_start_stop_count", "scsi_load_unload_count",
            "scsi_percentage_used_endurance",
        ])

        for col in scsi_fixed_order:
            if col in all_keys and col not in used:
                ordered.append(col)
                used.add(col)

    # 5) ATA smart_{id}_normalized / smart_{id}_raw (데이터가 있을 때만)
    if has_ata_data:
        ata_ids = set()
        for k in all_keys:
            if k.startswith("smart_") and "_normalized" in k:
                try:
                    ata_ids.add(int(k.replace("smart_", "").replace("_normalized", "")))
                except ValueError:
                    pass
            elif k.startswith("smart_") and "_raw" in k:
                try:
                    ata_ids.add(int(k.replace("smart_", "").replace("_raw", "")))
                except ValueError:
                    pass

        for aid in sorted(ata_ids):
            for suffix in ["_normalized", "_raw"]:
                col = f"smart_{aid}{suffix}"
                if col in all_keys and col not in used:
                    ordered.append(col)
                    used.add(col)

    # 6) 혹시 빠진 컬럼이 있으면 알파벳순 추가
    remaining = sorted(all_keys - used)
    ordered.extend(remaining)

    return ordered


def run_convert(input_patterns: list, output_path: str, logger: logging.Logger):
    """JSONL → Backblaze 호환 CSV 변환"""

    input_files = []
    for pattern in input_patterns:
        input_files.extend(sorted(glob.glob(pattern, recursive=True)))

    if not input_files:
        logger.error(f"입력 파일 없음: {input_patterns}")
        return

    logger.info(f"변환 대상: {len(input_files)}개 JSONL 파일")

    # 1단계: 모든 레코드를 Backblaze 호환 행으로 변환
    rows = []
    for fpath in input_files:
        with open(fpath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(f"{fpath}:{line_num} JSON 파싱 실패: {e}")
                    continue
                rows.append(_extract_row(record))

    if not rows:
        logger.warning("변환할 데이터 없음")
        return

    # 2단계: 컬럼 순서 결정
    ordered = _build_column_order(rows)

    # 3단계: CSV 쓰기
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with open(out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=ordered,
                                restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    size_mb = out.stat().st_size / (1024 * 1024)

    # 통계
    ata_cols = [c for c in ordered if c.startswith("smart_")]
    scsi_cols = [c for c in ordered if c.startswith("scsi_")]
    meta_cols = [c for c in ordered if not c.startswith("smart_") and not c.startswith("scsi_")]

    logger.info(
        f"CSV 저장: {out} ({len(rows)}행, {len(ordered)}열, {size_mb:.1f}MB) "
        f"[메타:{len(meta_cols)} ATA:{len(ata_cols)} SAS:{len(scsi_cols)}]"
    )

    # 컬럼 메타 저장
    meta_path = out.with_suffix(".columns.json")
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump({
            "total_columns": len(ordered),
            "meta_columns": meta_cols,
            "ata_smart_columns": ata_cols,
            "scsi_columns": scsi_cols,
            "drive_count": len(rows),
            "source_files": input_files,
        }, f, indent=2, ensure_ascii=False)

    logger.info(f"컬럼 목록: {meta_path}")


# =============================================================================
# 초기 세팅 (setup) — 원격 노드 준비
# =============================================================================

async def run_setup(config: Config, logger: logging.Logger):
    """모든 노드에 smartmontools 설치 + sudoers 설정"""

    global_sem = asyncio.Semaphore(config.max_total_sessions)

    async def setup_node(hostname: str, ssh_cfg: dict):
        async with global_sem:
            try:
                conn_opts = {
                    "host": hostname,
                    "username": ssh_cfg["username"],
                    "client_keys": [ssh_cfg["key_path"]],
                    "known_hosts": None,
                    "connect_timeout": config.ssh_connect_timeout,
                }
                async with asyncssh.connect(**conn_opts) as conn:
                    # smartmontools 설치
                    r = await conn.run(
                        "which smartctl || sudo apt-get install -y smartmontools "
                        "|| sudo yum install -y smartmontools",
                        check=False
                    )
                    # sudoers 설정
                    user = ssh_cfg["username"]
                    sudoers_line = f"{user} ALL=(ALL) NOPASSWD: /usr/sbin/smartctl"
                    await conn.run(
                        f'echo "{sudoers_line}" | sudo tee /etc/sudoers.d/smart-collector '
                        f"&& sudo chmod 440 /etc/sudoers.d/smart-collector",
                        check=False
                    )
                    # 검증
                    ver = await conn.run("sudo smartctl --version", check=False)
                    first_line = (ver.stdout or "").split("\n")[0]
                    logger.info(f"[{hostname}] OK — {first_line}")

            except Exception as e:
                logger.error(f"[{hostname}] 세팅 실패: {e}")

    tasks = []
    for cluster in config.clusters:
        ssh_cfg = config.get_ssh_for_cluster(cluster)
        for hostname in cluster.get("nodes", []):
            tasks.append(setup_node(hostname, ssh_cfg))

    logger.info(f"원격 세팅: {len(tasks)}개 노드")
    await asyncio.gather(*tasks)
    logger.info("세팅 완료")


# =============================================================================
# 알림
# =============================================================================

def send_notification(config, logger, now, ok_nodes, fail_nodes,
                      total_nodes, drive_count, jsonl_path, all_results):
    email_cfg = config.notify_cfg.get("email", {})
    recipients = email_cfg.get("recipients", [])
    if not recipients:
        return

    failure_rate = fail_nodes / max(total_nodes, 1)
    threshold = config.notify_cfg.get("alert_conditions", {}).get(
        "failure_rate_threshold", 0.1)
    prefix = "[ALERT]" if failure_rate > threshold else "[OK]"

    msg = EmailMessage()
    msg['Subject'] = f"{prefix} [SMART] {now:%Y-%m-%d %H:%M} 수집 리포트"
    msg['From'] = email_cfg.get("sender", "")
    msg['To'] = ", ".join(recipients)

    body = (
        f"수집 시간: {now:%Y-%m-%d %H:%M:%S}\n"
        f"노드: {ok_nodes}/{total_nodes} 성공\n"
        f"드라이브: {drive_count}개 수집\n"
        f"저장: {jsonl_path}\n"
    )
    if fail_nodes > 0:
        body += "\n--- 실패 노드 ---\n"
        for r in all_results:
            if not r.success:
                body += f"  {r.cluster}/{r.hostname}: {r.errors}\n"

    msg.set_content(body)

    try:
        server = email_cfg.get("smtp_server", "")
        port = email_cfg.get("smtp_port", 587)
        use_ssl = email_cfg.get("use_ssl", False)

        if not server:
            logger.error("SMTP 서버 미설정")
            return

        if use_ssl:
            with smtplib.SMTP_SSL(server, port) as smtp:
                smtp.login(email_cfg.get("sender"), config.smtp_password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(server, port) as smtp:
                smtp.starttls()
                if config.smtp_password:
                    smtp.login(email_cfg.get("sender"), config.smtp_password)
                smtp.send_message(msg)
        logger.info("알림 발송 완료")
    except Exception as e:
        logger.error(f"알림 발송 실패: {e}")


# =============================================================================
# 보존 정책
# =============================================================================

def cleanup_old_files(config: Config, logger: logging.Logger):
    import time
    retention = config.schedule_cfg.get("retention", {})

    for subdir, max_days in [("hourly", retention.get("hourly_days", 7)),
                              ("daily", retention.get("daily_days", 365))]:
        base = config.output_base / subdir
        if not base.exists():
            continue
        cutoff = time.time() - (max_days * 86400)
        removed = 0
        for f in base.rglob("*"):
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        if removed:
            logger.info(f"보존 정책: {subdir}/ 에서 {removed}개 삭제 (>{max_days}일)")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Ceph Multi-Cluster SMART Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python3 smart_collector.py collect -c config.yaml
  python3 smart_collector.py collect -c config.yaml --cluster prod-01 --dry-run
  python3 smart_collector.py convert -i "datasets/hourly/202604/*.jsonl" -o analysis.csv
  python3 smart_collector.py setup -c config.yaml
        """)

    sub = parser.add_subparsers(dest="command", required=True)

    # ── collect ──
    p_collect = sub.add_parser("collect", help="SSH로 SMART raw 데이터 수집")
    p_collect.add_argument("-c", "--config", required=True, help="설정 YAML")
    p_collect.add_argument("--cluster", default=None, help="특정 클러스터만")
    p_collect.add_argument("--dry-run", action="store_true", help="대상 목록만 출력")

    # ── convert ──
    p_convert = sub.add_parser("convert", help="JSONL → CSV 변환 (후처리)")
    p_convert.add_argument("-i", "--input", nargs="+", required=True,
                           help="입력 JSONL 파일 (glob 패턴 가능)")
    p_convert.add_argument("-o", "--output", default=None,
                           help="출력 CSV 경로 (미지정시 자동 생성)")

    # ── setup ──
    p_setup = sub.add_parser("setup", help="원격 노드 초기 세팅")
    p_setup.add_argument("-c", "--config", required=True, help="설정 YAML")

    args = parser.parse_args()

    # 간단 로거 (config 없는 convert용)
    if args.command == "convert":
        logging.basicConfig(level=logging.INFO,
                            format="[%(asctime)s] %(levelname)-7s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
        logger = logging.getLogger("smart_collector")

        output = args.output
        if output is None:
            output = f"smart_converted_{datetime.now():%Y%m%d_%H%M%S}.csv"

        run_convert(args.input, output, logger)

    elif args.command == "collect":
        config = Config(args.config)
        logger = setup_logging(config)
        asyncio.run(run_collect(config, logger, args.cluster, args.dry_run))

    elif args.command == "setup":
        config = Config(args.config)
        logger = setup_logging(config)
        asyncio.run(run_setup(config, logger))


if __name__ == "__main__":
    main()
