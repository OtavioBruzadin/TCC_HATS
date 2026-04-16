from __future__ import annotations

import argparse
import csv
import json
import struct
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

RBD_RECORD_SIZE = 38
AUX_RECORD_SIZE = 80
UTC = timezone.utc
SUPPORTED_EXTENSIONS = {".rbd", ".aux", ".ws"}


@dataclass
class RBDRecord:
    sample: int
    sec: int
    ms: int
    husec: int
    golay: int
    chopper: int
    temp_env: int
    temp_hics: int
    temp_golay: int


@dataclass
class AUXRecord:
    husec: int
    jd: float
    sid: float
    elevation: float
    azimuth: float
    right_ascension: float
    declination: float
    ra_rate: float
    dec_rate: float
    object_id: int
    opmode: int


def dt_from_unix_ms(sec: int, ms: int) -> str:
    dt = datetime.fromtimestamp(sec, tz=UTC) + timedelta(milliseconds=ms)
    return dt.isoformat()


def dt_from_husec(date_str: str, husec: int) -> str:
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    dt = base + timedelta(seconds=husec / 10000.0)
    return dt.isoformat()


def decode_ad7770_u32(value: int) -> int:
    adcu = value & 0x00FFFFFF
    negative = (value & 0x0800000) > 0
    if negative:
        adcu = adcu - 0x1000000
    return adcu


def detect_date_from_name(path: Path) -> str | None:
    name = path.name
    if name.startswith("hats-") and len(name) >= 15:
        return name[5:15]
    return None


def parse_rbd_record(chunk: bytes) -> RBDRecord:
    sample, sec, ms, husec, golay, chopper, temp_env, temp_hics, temp_golay = struct.unpack("<IIHQIIIII", chunk)
    return RBDRecord(sample, sec, ms, husec, golay, chopper, temp_env, temp_hics, temp_golay)


def parse_aux_record(chunk: bytes) -> AUXRecord:
    values = struct.unpack("<Qddddddddii", chunk)
    return AUXRecord(*values)


def summarize_numeric(values: list[float | int]) -> dict:
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": sum(values) / len(values),
    }


def sanitize_name(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in name)


def sample_records(path: Path, record_size: int, parser, sample_count: int) -> tuple[list, int]:
    file_size = path.stat().st_size
    if file_size % record_size != 0:
        raise ValueError(f"File {path.name} has size {file_size}, which is not divisible by record size {record_size}")
    total_records = file_size // record_size
    if total_records == 0:
        return [], 0

    wanted = sorted(set([
        *range(min(sample_count, total_records)),
        *range(max(0, total_records - sample_count), total_records),
    ]))

    records = []
    with path.open("rb") as f:
        for idx in wanted:
            f.seek(idx * record_size)
            chunk = f.read(record_size)
            records.append((idx, parser(chunk)))
    return records, total_records


def export_rbd_csv(path: Path, out_csv: Path, limit: int | None = None, decode_adc: bool = False) -> None:
    with path.open("rb") as f, out_csv.open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["sample", "sec", "ms", "datetime_utc", "husec", "golay", "chopper", "temp_env", "temp_hics", "temp_golay"])
        index = 0
        while True:
            if limit is not None and index >= limit:
                break
            chunk = f.read(RBD_RECORD_SIZE)
            if not chunk:
                break
            if len(chunk) != RBD_RECORD_SIZE:
                raise ValueError(f"Partial RBD record at index {index}")
            record = parse_rbd_record(chunk)
            golay = decode_ad7770_u32(record.golay) if decode_adc else record.golay
            chopper = decode_ad7770_u32(record.chopper) if decode_adc else record.chopper
            temp_env = decode_ad7770_u32(record.temp_env) if decode_adc else record.temp_env
            temp_hics = decode_ad7770_u32(record.temp_hics) if decode_adc else record.temp_hics
            temp_golay = decode_ad7770_u32(record.temp_golay) if decode_adc else record.temp_golay
            writer.writerow([record.sample, record.sec, record.ms, dt_from_unix_ms(record.sec, record.ms), record.husec, golay, chopper, temp_env, temp_hics, temp_golay])
            index += 1


def export_aux_csv(path: Path, out_csv: Path, limit: int | None = None) -> None:
    date_str = detect_date_from_name(path)
    with path.open("rb") as f, out_csv.open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["husec", "datetime_utc_from_husec", "jd", "sid", "elevation", "azimuth", "right_ascension", "declination", "ra_rate", "dec_rate", "object_id", "opmode"])
        index = 0
        while True:
            if limit is not None and index >= limit:
                break
            chunk = f.read(AUX_RECORD_SIZE)
            if not chunk:
                break
            if len(chunk) != AUX_RECORD_SIZE:
                raise ValueError(f"Partial AUX record at index {index}")
            record = parse_aux_record(chunk)
            dt_husec = dt_from_husec(date_str, record.husec) if date_str else ""
            writer.writerow([record.husec, dt_husec, record.jd, record.sid, record.elevation, record.azimuth, record.right_ascension, record.declination, record.ra_rate, record.dec_rate, record.object_id, record.opmode])
            index += 1


def export_ws_csv(path: Path, out_csv: Path) -> None:
    with path.open("r", encoding="utf-8", errors="ignore") as f, out_csv.open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["time", "station_code", "temperature_c", "humidity", "pressure_h"])
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) != 5:
                continue
            try:
                time_str = parts[0]
                station_code = parts[1]
                temperature_c = float(parts[2].split("=")[1][:-1])
                humidity = float(parts[3].split("=")[1][:-1])
                pressure_h = float(parts[4].split("=")[1][:-1])
            except Exception:
                continue
            writer.writerow([time_str, station_code, temperature_c, humidity, pressure_h])


def report_rbd(path: Path, sample_count: int = 5) -> dict:
    sampled, total_records = sample_records(path, RBD_RECORD_SIZE, parse_rbd_record, sample_count)
    file_size = path.stat().st_size
    sample_values, sec_values, ms_values, husec_values, samples = [], [], [], [], []

    for idx, rec in sampled:
        sample_values.append(rec.sample)
        sec_values.append(rec.sec)
        ms_values.append(rec.ms)
        husec_values.append(rec.husec)
        samples.append({
            "index": idx,
            "sample": rec.sample,
            "sec": rec.sec,
            "ms": rec.ms,
            "datetime_utc": dt_from_unix_ms(rec.sec, rec.ms),
            "husec": rec.husec,
            "golay": rec.golay,
            "chopper": rec.chopper,
            "temp_env": rec.temp_env,
            "temp_hics": rec.temp_hics,
            "temp_golay": rec.temp_golay,
        })

    first = sampled[0][1] if sampled else None
    last = sampled[-1][1] if sampled else None

    return {
        "file": path.name,
        "type": "rbd",
        "parent_folder": str(path.parent),
        "file_size_bytes": file_size,
        "record_size_bytes": RBD_RECORD_SIZE,
        "total_records": total_records,
        "layout": [
            ["sample", "uint32", 4],
            ["sec", "uint32", 4],
            ["ms", "uint16", 2],
            ["husec", "uint64", 8],
            ["golay", "uint32", 4],
            ["chopper", "uint32", 4],
            ["temp_env", "uint32", 4],
            ["temp_hics", "uint32", 4],
            ["temp_golay", "uint32", 4],
        ],
        "first_record_time_utc": dt_from_unix_ms(first.sec, first.ms) if first else None,
        "last_record_time_utc": dt_from_unix_ms(last.sec, last.ms) if last else None,
        "sampled_records": samples,
        "sample_field_stats_from_sampled_records": {
            "sample": summarize_numeric(sample_values),
            "sec": summarize_numeric(sec_values),
            "ms": summarize_numeric(ms_values),
            "husec": summarize_numeric(husec_values),
        },
    }


def report_aux(path: Path, sample_count: int = 5) -> dict:
    sampled, total_records = sample_records(path, AUX_RECORD_SIZE, parse_aux_record, sample_count)
    file_size = path.stat().st_size
    date_str = detect_date_from_name(path)
    object_counter, opmode_counter, samples = Counter(), Counter(), []

    for idx, rec in sampled:
        object_counter[rec.object_id] += 1
        opmode_counter[rec.opmode] += 1
        samples.append({
            "index": idx,
            "husec": rec.husec,
            "datetime_utc_from_husec": dt_from_husec(date_str, rec.husec) if date_str else None,
            "jd": rec.jd,
            "sid": rec.sid,
            "elevation": rec.elevation,
            "azimuth": rec.azimuth,
            "right_ascension": rec.right_ascension,
            "declination": rec.declination,
            "ra_rate": rec.ra_rate,
            "dec_rate": rec.dec_rate,
            "object_id": rec.object_id,
            "opmode": rec.opmode,
        })

    return {
        "file": path.name,
        "type": "aux",
        "parent_folder": str(path.parent),
        "file_size_bytes": file_size,
        "record_size_bytes": AUX_RECORD_SIZE,
        "total_records": total_records,
        "layout": [
            ["husec", "uint64", 8],
            ["jd", "float64", 8],
            ["sid", "float64", 8],
            ["elevation", "float64", 8],
            ["azimuth", "float64", 8],
            ["right_ascension", "float64", 8],
            ["declination", "float64", 8],
            ["ra_rate", "float64", 8],
            ["dec_rate", "float64", 8],
            ["object_id", "int32", 4],
            ["opmode", "int32", 4],
        ],
        "date_inferred_from_filename": date_str,
        "sampled_records": samples,
        "sampled_object_id_counts": dict(object_counter),
        "sampled_opmode_counts": dict(opmode_counter),
    }


def report_ws(path: Path, sample_count: int = 5) -> dict:
    rows, station_codes, temperatures, humidities, pressures = [], Counter(), [], [], []

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) != 5:
                continue
            try:
                row = {
                    "time": parts[0],
                    "station_code": parts[1],
                    "temperature_c": float(parts[2].split("=")[1][:-1]),
                    "humidity": float(parts[3].split("=")[1][:-1]),
                    "pressure_h": float(parts[4].split("=")[1][:-1]),
                }
            except Exception:
                continue
            rows.append(row)
            station_codes[row["station_code"]] += 1
            temperatures.append(row["temperature_c"])
            humidities.append(row["humidity"])
            pressures.append(row["pressure_h"])

    sampled = rows[:sample_count] + rows[-sample_count:] if len(rows) > sample_count else rows

    return {
        "file": path.name,
        "type": "ws",
        "parent_folder": str(path.parent),
        "format": "ascii_csv_like",
        "logical_columns": [
            ["time", "iso_datetime"],
            ["station_code", "string"],
            ["temperature_c", "float"],
            ["humidity", "float"],
            ["pressure_h", "float"],
        ],
        "total_valid_rows": len(rows),
        "first_time": rows[0]["time"] if rows else None,
        "last_time": rows[-1]["time"] if rows else None,
        "sampled_rows": sampled,
        "station_code_counts": dict(station_codes),
        "stats": {
            "temperature_c": summarize_numeric(temperatures),
            "humidity": summarize_numeric(humidities),
            "pressure_h": summarize_numeric(pressures),
        },
    }


def write_json_report(report: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def ensure_project_structure(project_root: Path, data_dir_name: str = "Data", output_dir_name: str = "Reports") -> dict:
    data_dir = project_root / data_dir_name
    reports_dir = project_root / output_dir_name
    csv_dir = reports_dir / "csv"
    json_dir = reports_dir / "json"

    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    readme = data_dir / "README.txt"
    if not readme.exists():
        readme.write_text(
            "Put HATS files here.\n\n"
            "Suggested structure:\n"
            "Data/\n"
            "  2026-03-17/\n"
            "    hats-2026-03-17T1300.rbd\n"
            "    hats-2026-03-17T1100.aux\n"
            "    hats-2026-03-17.ws\n",
            encoding="utf-8",
        )

    return {
        "project_root": project_root,
        "data_dir": data_dir,
        "reports_dir": reports_dir,
        "csv_dir": csv_dir,
        "json_dir": json_dir,
    }


def find_input_files(data_dir: Path, recursive: bool = True) -> list[Path]:
    iterator = data_dir.rglob("*") if recursive else data_dir.glob("*")
    files = [path for path in iterator if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS]
    return sorted(files)


def group_files_by_date(files: list[Path]) -> dict[str, list[Path]]:
    grouped = defaultdict(list)
    for path in files:
        date_key = detect_date_from_name(path) or "unknown_date"
        grouped[date_key].append(path)
    return dict(sorted(grouped.items(), key=lambda item: item[0]))


def build_group_summary(grouped_files: dict[str, list[Path]]) -> dict:
    summary = {}
    for date_key, paths in grouped_files.items():
        summary[date_key] = {
            "total_files": len(paths),
            "files": [p.name for p in paths],
            "types": dict(Counter(p.suffix.lower().lstrip(".") for p in paths)),
        }
    return summary


def process_file(path: Path, json_dir: Path, csv_dir: Path, sample_count: int, export_csv: bool, csv_limit: int | None, decode_adc: bool) -> dict:
    suffix = path.suffix.lower()
    relative_key = sanitize_name(str(path.with_suffix("")).replace("/", "__").replace("\\", "__"))

    if suffix == ".rbd":
        report = report_rbd(path, sample_count)
        write_json_report(report, json_dir / f"{relative_key}_report.json")
        if export_csv:
            export_rbd_csv(path, csv_dir / f"{relative_key}.csv", csv_limit, decode_adc=decode_adc)
        return report

    if suffix == ".aux":
        report = report_aux(path, sample_count)
        write_json_report(report, json_dir / f"{relative_key}_report.json")
        if export_csv:
            export_aux_csv(path, csv_dir / f"{relative_key}.csv", csv_limit)
        return report

    if suffix == ".ws":
        report = report_ws(path, sample_count)
        write_json_report(report, json_dir / f"{relative_key}_report.json")
        if export_csv:
            export_ws_csv(path, csv_dir / f"{relative_key}.csv")
        return report

    report = {"file": path.name, "error": f"Unsupported extension: {suffix}"}
    write_json_report(report, json_dir / f"{relative_key}_report.json")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="*", help="Input HATS files")
    parser.add_argument("--project-root", default=".", help="Project root")
    parser.add_argument("--data-dir", default="Data", help="Folder containing files to analyze")
    parser.add_argument("--reports-dir", default="Reports", help="Folder for generated reports")
    parser.add_argument("--sample-count", type=int, default=5, help="How many records to sample from the beginning and end")
    parser.add_argument("--export-csv", action="store_true", help="Export parsed tables to CSV")
    parser.add_argument("--csv-limit", type=int, default=None, help="Maximum number of rows to export to CSV")
    parser.add_argument("--decode-adc", action="store_true", help="Decode 24-bit AD7770-style signed values stored in uint32 fields")
    parser.add_argument("--from-data-dir", action="store_true", help="Read all supported files from Data folder recursively")
    parser.add_argument("--init-project", action="store_true", help="Create Data and Reports folders")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    paths = ensure_project_structure(project_root, args.data_dir, args.reports_dir)
    data_dir = paths["data_dir"]
    reports_dir = paths["reports_dir"]
    csv_dir = paths["csv_dir"]
    json_dir = paths["json_dir"]

    if args.init_project:
        print(f"Project initialized at {project_root}")
        print(f"Data folder: {data_dir}")
        print(f"Reports folder: {reports_dir}")
        return

    input_files = []
    if args.inputs:
        input_files.extend([Path(item).resolve() for item in args.inputs])

    if args.from_data_dir:
        input_files.extend(find_input_files(data_dir, recursive=True))

    unique_files = sorted({path.resolve() for path in input_files if path.exists()})

    if not unique_files:
        raise SystemExit("No input files found. Use positional file paths or run with --from-data-dir after placing files inside Data.")

    reports = []
    for path in unique_files:
        reports.append(process_file(path, json_dir, csv_dir, args.sample_count, args.export_csv, args.csv_limit, args.decode_adc))

    grouped_files = group_files_by_date(unique_files)
    summary = {
        "project_root": str(project_root),
        "data_dir": str(data_dir),
        "reports_dir": str(reports_dir),
        "total_files_processed": len(unique_files),
        "grouped_by_date": build_group_summary(grouped_files),
        "reports": reports,
    }

    write_json_report(summary, reports_dir / "summary.json")
    print(f"Processed {len(unique_files)} file(s).")
    print(f"Summary written to: {reports_dir / 'summary.json'}")

if __name__ == "__main__":
    main()

