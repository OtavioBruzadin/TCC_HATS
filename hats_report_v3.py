import argparse
import csv
import json
import struct
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

UTC = timezone.utc

RBD_SCHEMA = {
    "record_size": 38,
    "struct_format": "<IIHQIIIII",
    "fields": [
        {"name": "sample", "type": "uint32", "size": 4},
        {"name": "sec", "type": "uint32", "size": 4},
        {"name": "ms", "type": "uint16", "size": 2},
        {"name": "husec", "type": "uint64", "size": 8},
        {"name": "golay", "type": "uint32", "size": 4, "decoder": "ad7770"},
        {"name": "chopper", "type": "uint32", "size": 4, "decoder": "ad7770"},
        {"name": "temp_env", "type": "uint32", "size": 4, "decoder": "ad7770"},
        {"name": "temp_hics", "type": "uint32", "size": 4, "decoder": "ad7770"},
        {"name": "temp_golay", "type": "uint32", "size": 4, "decoder": "ad7770"},
    ],
}

AUX_SCHEMA = {
    "record_size": 80,
    "struct_format": "<Qddddddddii",
    "fields": [
        {"name": "husec", "type": "uint64", "size": 8},
        {"name": "jd", "type": "float64", "size": 8},
        {"name": "sid", "type": "float64", "size": 8},
        {"name": "elevation", "type": "float64", "size": 8},
        {"name": "azimuth", "type": "float64", "size": 8},
        {"name": "right_ascension", "type": "float64", "size": 8},
        {"name": "declination", "type": "float64", "size": 8},
        {"name": "ra_rate", "type": "float64", "size": 8},
        {"name": "dec_rate", "type": "float64", "size": 8},
        {"name": "object", "type": "int32", "size": 4},
        {"name": "opmode", "type": "int32", "size": 4},
    ],
}


class GenericRecord(object):
    def __init__(self, values):
        for key, value in values.items():
            setattr(self, key, value)


def ensure_project_structure(project_root, data_dir_name="Data", reports_dir_name="Reports"):
    data_dir = project_root / data_dir_name
    reports_dir = project_root / reports_dir_name
    json_dir = reports_dir / "json"
    csv_dir = reports_dir / "csv"

    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    readme_path = data_dir / "README.txt"
    if not readme_path.exists():
        readme_path.write_text(
            "Suggested layout:\n\n"
            "Data/\n"
            "  2026-03-17/\n"
            "    hats-2026-03-17T1300.rbd\n"
            "    aux/\n"
            "      hats-2026-03-17T1300.aux\n"
            "      hats-2026-03-17.ws\n",
            encoding="utf-8",
        )

    return {
        "project_root": project_root,
        "data_dir": data_dir,
        "reports_dir": reports_dir,
        "json_dir": json_dir,
        "csv_dir": csv_dir,
    }


def detect_date_from_name(path):
    name = path.name
    if name.startswith("hats-") and len(name) >= 15:
        return name[5:15]
    return None


def detect_hour_from_name(path):
    name = path.name
    if "T" in name:
        return name.split("T", 1)[1][:4]
    return None


def dt_from_husec(date_str, husec):
    if not date_str:
        return None
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    return (base + timedelta(seconds=float(husec) / 10000.0)).isoformat()


def dt_from_unix_time(sec, ms):
    return (datetime.fromtimestamp(sec, tz=UTC) + timedelta(milliseconds=ms)).isoformat()


def decode_ad7770(value):
    adcu = int(value) & 0x00FFFFFF
    negative = (int(value) & 0x0800000) > 0
    if negative:
        adcu = adcu - 0x1000000
    return adcu


def summarize_numeric(values):
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": float(sum(values)) / float(len(values)),
    }


def unpack_record(chunk, schema):
    unpacked = struct.unpack(schema["struct_format"], chunk)
    values = {}
    for index, field in enumerate(schema["fields"]):
        values[field["name"]] = unpacked[index]
    return GenericRecord(values)


def sample_records(path, schema, sample_count):
    file_size = path.stat().st_size
    record_size = schema["record_size"]

    if file_size % record_size != 0:
        raise ValueError(
            "File {} has size {}, not divisible by record size {}".format(
                path.name, file_size, record_size
            )
        )

    total_records = file_size // record_size
    if total_records == 0:
        return [], 0

    wanted = list(range(min(sample_count, total_records)))
    wanted.extend(range(max(0, total_records - sample_count), total_records))
    wanted = sorted(set(wanted))

    records = []
    with path.open("rb") as f:
        for idx in wanted:
            f.seek(idx * record_size)
            chunk = f.read(record_size)
            if len(chunk) != record_size:
                raise ValueError("Partial record in {} at index {}".format(path.name, idx))
            records.append((idx, unpack_record(chunk, schema)))

    return records, total_records


def build_day_index(data_dir):
    index = {}

    for day_dir in sorted(data_dir.iterdir()):
        if not day_dir.is_dir():
            continue

        aux_dir = day_dir / "aux"
        files = {
            "rbd": sorted(day_dir.glob("*.rbd")),
            "aux": sorted(aux_dir.glob("*.aux")) if aux_dir.exists() else [],
            "ws": sorted(aux_dir.glob("*.ws")) if aux_dir.exists() else [],
        }

        hours = defaultdict(dict)

        for path in files["rbd"]:
            hours[detect_hour_from_name(path) or "unknown"]["rbd"] = path

        for path in files["aux"]:
            hours[detect_hour_from_name(path) or "unknown"]["aux"] = path

        for path in files["ws"]:
            hours["daily"]["ws"] = path

        index[day_dir.name] = {
            "day_dir": day_dir,
            "aux_dir": aux_dir,
            "hours": dict(sorted(hours.items(), key=lambda item: item[0])),
        }

    return index


def interpreted_rbd_value(field, raw_value):
    if field.get("decoder") == "ad7770":
        return decode_ad7770(raw_value)
    return raw_value


def aux_pointing_state(record):
    azimuth = getattr(record, "azimuth", None)
    elevation = getattr(record, "elevation", None)
    ra = getattr(record, "right_ascension", None)
    dec = getattr(record, "declination", None)

    pointing_valid = False

    if azimuth is not None and elevation is not None:
        if float(azimuth) != 0.0 or float(elevation) != 0.0:
            pointing_valid = True

    if ra is not None and dec is not None:
        if float(ra) != 0.0 or float(dec) != 0.0:
            pointing_valid = True

    return {
        "pointing_valid": pointing_valid,
        "pointing_zeroed": not pointing_valid,
    }


def report_rbd(path, sample_count):
    sampled, total_records = sample_records(path, RBD_SCHEMA, sample_count)
    date_str = detect_date_from_name(path)

    samples = []
    times = []

    for idx, rec in sampled:
        row = {"index": idx}

        for field in RBD_SCHEMA["fields"]:
            raw_value = getattr(rec, field["name"])
            row[field["name"]] = raw_value

            if field.get("decoder") == "ad7770":
                row[field["name"] + "_interpreted"] = interpreted_rbd_value(field, raw_value)

        if hasattr(rec, "sec") and hasattr(rec, "ms"):
            row["datetime_utc"] = dt_from_unix_time(rec.sec, rec.ms)
            times.append(row["datetime_utc"])

        if hasattr(rec, "husec"):
            row["datetime_utc_from_husec"] = dt_from_husec(date_str, rec.husec)

        samples.append(row)

    return {
        "file": path.name,
        "type": "rbd",
        "date": date_str,
        "hour": detect_hour_from_name(path),
        "parent_folder": str(path.parent),
        "file_size_bytes": path.stat().st_size,
        "record_size_bytes": RBD_SCHEMA["record_size"],
        "total_records": total_records,
        "schema": RBD_SCHEMA["fields"],
        "notes": [
            "Schema fixed in code because XML files were not available.",
            "AD7770-style fields are exported with an additional interpreted value.",
        ],
        "first_sample_time_utc": times[0] if times else None,
        "last_sample_time_utc": times[-1] if times else None,
        "sampled_records": samples,
    }


def report_aux(path, sample_count):
    sampled, total_records = sample_records(path, AUX_SCHEMA, sample_count)
    date_str = detect_date_from_name(path)

    samples = []
    object_counter = Counter()
    opmode_counter = Counter()
    pointing_valid_count = 0

    for idx, rec in sampled:
        row = {"index": idx}

        for field in AUX_SCHEMA["fields"]:
            row[field["name"]] = getattr(rec, field["name"])

        if hasattr(rec, "husec"):
            row["datetime_utc"] = dt_from_husec(date_str, rec.husec)

        pointing = aux_pointing_state(rec)
        row.update(pointing)

        if row["pointing_valid"]:
            pointing_valid_count += 1

        if hasattr(rec, "object"):
            object_counter[rec.object] += 1

        if hasattr(rec, "opmode"):
            opmode_counter[rec.opmode] += 1

        samples.append(row)

    return {
        "file": path.name,
        "type": "aux",
        "date": date_str,
        "hour": detect_hour_from_name(path),
        "parent_folder": str(path.parent),
        "file_size_bytes": path.stat().st_size,
        "record_size_bytes": AUX_SCHEMA["record_size"],
        "total_records": total_records,
        "schema": AUX_SCHEMA["fields"],
        "notes": [
            "Schema fixed in code because XML files were not available.",
            "Pointing is considered valid when azimuth/elevation or right_ascension/declination are not all zero.",
            "Zeroed pointing may indicate initialization state, missing pointing, or acquisition problems.",
        ],
        "sampled_records": samples,
        "sampled_object_counts": dict(object_counter),
        "sampled_opmode_counts": dict(opmode_counter),
        "sampled_pointing_valid_count": pointing_valid_count,
        "sampled_pointing_zeroed_count": len(samples) - pointing_valid_count,
    }


def parse_ws_line(line):
    parts = line.strip().split(",")
    if len(parts) != 5:
        return None

    try:
        return {
            "time": parts[0],
            "station_code": parts[1],
            "temperature_c": float(parts[2].split("=")[1][:-1]),
            "humidity": float(parts[3].split("=")[1][:-1]),
            "pressure_hpa": float(parts[4].split("=")[1][:-1]),
        }
    except Exception:
        return None


def report_ws(path, sample_count):
    rows = []
    station_codes = Counter()
    temperatures = []
    humidities = []
    pressures = []

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            row = parse_ws_line(raw_line)
            if row is None:
                continue
            rows.append(row)
            station_codes[row["station_code"]] += 1
            temperatures.append(row["temperature_c"])
            humidities.append(row["humidity"])
            pressures.append(row["pressure_hpa"])

    sampled = rows[:sample_count] + rows[-sample_count:] if len(rows) > sample_count else rows

    return {
        "file": path.name,
        "type": "ws",
        "date": detect_date_from_name(path),
        "parent_folder": str(path.parent),
        "total_valid_rows": len(rows),
        "first_time": rows[0]["time"] if rows else None,
        "last_time": rows[-1]["time"] if rows else None,
        "sampled_rows": sampled,
        "station_code_counts": dict(station_codes),
        "stats": {
            "temperature_c": summarize_numeric(temperatures),
            "humidity": summarize_numeric(humidities),
            "pressure_hpa": summarize_numeric(pressures),
        },
    }


def export_rbd_csv(path, out_csv, limit):
    with path.open("rb") as f, out_csv.open("w", newline="") as out:
        writer = csv.writer(out)
        headers = []

        for field in RBD_SCHEMA["fields"]:
            headers.append(field["name"])
            if field.get("decoder") == "ad7770":
                headers.append(field["name"] + "_interpreted")

        headers.extend(["datetime_utc", "datetime_utc_from_husec"])
        writer.writerow(headers)

        date_str = detect_date_from_name(path)
        index = 0

        while True:
            if limit is not None and index >= limit:
                break

            chunk = f.read(RBD_SCHEMA["record_size"])
            if not chunk:
                break

            if len(chunk) != RBD_SCHEMA["record_size"]:
                raise ValueError("Partial RBD record at index {}".format(index))

            rec = unpack_record(chunk, RBD_SCHEMA)
            row = []

            for field in RBD_SCHEMA["fields"]:
                raw_value = getattr(rec, field["name"])
                row.append(raw_value)
                if field.get("decoder") == "ad7770":
                    row.append(interpreted_rbd_value(field, raw_value))

            row.append(dt_from_unix_time(rec.sec, rec.ms) if hasattr(rec, "sec") and hasattr(rec, "ms") else None)
            row.append(dt_from_husec(date_str, rec.husec) if hasattr(rec, "husec") else None)
            writer.writerow(row)

            index += 1


def export_aux_csv(path, out_csv, limit):
    with path.open("rb") as f, out_csv.open("w", newline="") as out:
        writer = csv.writer(out)
        headers = [field["name"] for field in AUX_SCHEMA["fields"]]
        headers.extend(["datetime_utc", "pointing_valid", "pointing_zeroed"])
        writer.writerow(headers)

        date_str = detect_date_from_name(path)
        index = 0

        while True:
            if limit is not None and index >= limit:
                break

            chunk = f.read(AUX_SCHEMA["record_size"])
            if not chunk:
                break

            if len(chunk) != AUX_SCHEMA["record_size"]:
                raise ValueError("Partial AUX record at index {}".format(index))

            rec = unpack_record(chunk, AUX_SCHEMA)
            row = [getattr(rec, field["name"]) for field in AUX_SCHEMA["fields"]]
            row.append(dt_from_husec(date_str, rec.husec) if hasattr(rec, "husec") else None)

            pointing = aux_pointing_state(rec)
            row.extend([pointing["pointing_valid"], pointing["pointing_zeroed"]])

            writer.writerow(row)
            index += 1


def export_ws_csv(path, out_csv):
    with path.open("r", encoding="utf-8", errors="ignore") as f, out_csv.open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["time", "station_code", "temperature_c", "humidity", "pressure_hpa"])

        for raw_line in f:
            row = parse_ws_line(raw_line)
            if row is None:
                continue
            writer.writerow([row["time"], row["station_code"], row["temperature_c"], row["humidity"], row["pressure_hpa"]])


def write_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def process_day(day_key, day_info, json_dir, csv_dir, sample_count, export_csv, csv_limit):
    day_report = {
        "date": day_key,
        "day_dir": str(day_info["day_dir"]),
        "aux_dir": str(day_info["aux_dir"]),
        "hours": {},
        "schema_mode": "fixed_without_xml",
    }

    if "daily" in day_info["hours"] and "ws" in day_info["hours"]["daily"]:
        ws_path = day_info["hours"]["daily"]["ws"]
        ws_report = report_ws(ws_path, sample_count)
        day_report["weather_station"] = ws_report
        write_json(ws_report, json_dir / "{}__ws_report.json".format(day_key))
        if export_csv:
            export_ws_csv(ws_path, csv_dir / "{}__ws.csv".format(day_key))

    for hour_key, files in day_info["hours"].items():
        if hour_key == "daily":
            continue

        hour_report = {"hour": hour_key}

        if "rbd" in files:
            rbd_report = report_rbd(files["rbd"], sample_count)
            hour_report["rbd"] = rbd_report
            write_json(rbd_report, json_dir / "{}__{}__rbd_report.json".format(day_key, hour_key))
            if export_csv:
                export_rbd_csv(files["rbd"], csv_dir / "{}__{}__rbd.csv".format(day_key, hour_key), csv_limit)

        if "aux" in files:
            aux_report = report_aux(files["aux"], sample_count)
            hour_report["aux"] = aux_report
            write_json(aux_report, json_dir / "{}__{}__aux_report.json".format(day_key, hour_key))
            if export_csv:
                export_aux_csv(files["aux"], csv_dir / "{}__{}__aux.csv".format(day_key, hour_key), csv_limit)

        if "rbd" in hour_report and "aux" in hour_report:
            hour_report["pair_status"] = "paired"
        elif "rbd" in hour_report:
            hour_report["pair_status"] = "rbd_only"
        elif "aux" in hour_report:
            hour_report["pair_status"] = "aux_only"
        else:
            hour_report["pair_status"] = "empty"

        day_report["hours"][hour_key] = hour_report

    write_json(day_report, json_dir / "{}__day_report.json".format(day_key))
    return day_report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--data-dir", default="Data")
    parser.add_argument("--reports-dir", default="Reports")
    parser.add_argument("--init-project", action="store_true")
    parser.add_argument("--sample-count", type=int, default=5)
    parser.add_argument("--export-csv", action="store_true")
    parser.add_argument("--csv-limit", type=int, default=None)
    parser.add_argument("--from-data-dir", action="store_true")
    parser.add_argument("--day", default=None)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    paths = ensure_project_structure(project_root, args.data_dir, args.reports_dir)

    if args.init_project:
        print("Project initialized at {}".format(project_root))
        print("Data folder: {}".format(paths["data_dir"]))
        print("Reports folder: {}".format(paths["reports_dir"]))
        return

    if not args.from_data_dir:
        raise SystemExit("Use --from-data-dir to process the Data folder.")

    day_index = build_day_index(paths["data_dir"])

    if args.day:
        day_index = {k: v for k, v in day_index.items() if k == args.day}

    if not day_index:
        raise SystemExit("No day folders found inside {}.".format(paths["data_dir"]))

    reports = {}
    for day_key, day_info in day_index.items():
        reports[day_key] = process_day(
            day_key,
            day_info,
            paths["json_dir"],
            paths["csv_dir"],
            args.sample_count,
            args.export_csv,
            args.csv_limit,
        )

    summary = {
        "project_root": str(project_root),
        "data_dir": str(paths["data_dir"]),
        "reports_dir": str(paths["reports_dir"]),
        "schema_mode": "fixed_without_xml",
        "days_processed": list(reports.keys()),
        "reports": reports,
    }

    write_json(summary, paths["reports_dir"] / "summary.json")
    print("Processed {} day(s).".format(len(reports)))
    print("Summary written to: {}".format(paths["reports_dir"] / "summary.json"))


if __name__ == "__main__":
    main()
