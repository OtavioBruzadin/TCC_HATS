"""
Montagem dos relatórios.

Um arquivo JSON por objeto analisado, e os agregadores apontam para eles em vez
de copiá-los. Antes o mesmo conteúdo era gravado três vezes — no relatório da
hora, embutido inteiro no do dia, e embutido inteiro no resumo geral —, o que
com um mês de dados dava dezenas de MB repetidos.
"""

from hats import calibration, exporters, pointing, records, schema as schema_module, timebase, weather


def rbd_report(path, schema, sample_count, options, analyser):
    """Relatório do sinal do detector. Devolve (relatório, deconv)."""
    sampled, count = records.sample_records(path, schema, sample_count)
    date_str = timebase.date_from_filename(path)
    index_of = schema_module.field_index(schema)

    samples = []
    for index, record in sampled:
        row = {"index": index}
        for field in schema["fields"]:
            raw = getattr(record, field["name"])
            row[field["name"]] = raw
            if field.get("origin") == "ad7770" or field.get("convert") == "yes":
                row[field["name"] + "_interpreted"] = calibration.apply(field, raw)
        if "sec" in index_of and "ms" in index_of:
            row["datetime_utc"] = timebase.datetime_from_unix(record.sec, record.ms)
        if "husec" in index_of:
            row["datetime_utc_from_husec"] = timebase.datetime_from_husec(date_str, record.husec)
        samples.append(row)

    analysis = analyser(path, schema, options)
    deconv = analysis.pop("_deconv", None)

    report = {
        "file": path.name,
        "type": "rbd",
        "date": date_str,
        "hour": timebase.hour_from_filename(path),
        "parent_folder": str(path.parent),
        "file_size_bytes": path.stat().st_size,
        "record_size_bytes": schema["record_size"],
        "total_records": count,
        "schema_mode": schema.get("mode"),
        "schema_source": schema.get("source"),
        "schema": schema["fields"],
        "sampled_records": samples,
    }
    report.update(analysis)
    return report, deconv


def aux_report(path, schema, sample_count, options):
    """Relatório do apontamento."""
    sampled, count = records.sample_records(path, schema, sample_count)
    date_str = timebase.date_from_filename(path)

    samples = []
    for index, record in sampled:
        row = {"index": index}
        for field in schema["fields"]:
            row[field["name"]] = getattr(record, field["name"])
        row.update(pointing.corrected_values(record))
        if hasattr(record, "husec"):
            row["datetime_utc"] = timebase.datetime_from_husec(date_str, record.husec)
        row.update(pointing.state(record))
        samples.append(row)

    report = {
        "file": path.name,
        "type": "aux",
        "date": date_str,
        "hour": timebase.hour_from_filename(path),
        "parent_folder": str(path.parent),
        "file_size_bytes": path.stat().st_size,
        "record_size_bytes": schema["record_size"],
        "total_records": count,
        "schema_mode": schema.get("mode"),
        "schema_source": schema.get("source"),
        "schema": schema["fields"],
        "unit_corrections": {
            name: {"declared_in_xml": declared, "actual": actual,
                   "corrected_field": corrected_name, "factor": factor}
            for name, (declared, actual, factor, corrected_name)
            in schema_module.AUX_UNIT_FIXES.items()
        },
        "sampled_records": samples,
    }
    report.update(pointing.analyse(path, schema, options))
    return report


def ws_report(path, sample_count):
    report = {
        "file": path.name,
        "type": "ws",
        "date": timebase.date_from_filename(path),
        "parent_folder": str(path.parent),
    }
    report.update(weather.analyse(path, sample_count))
    return report


def digest_rbd(report, report_file):
    """Resumo do relatório do detector, com ponteiro para o arquivo completo."""
    digest = {
        "report_file": report_file,
        "file": report["file"],
        "total_records": report["total_records"],
        "first_time_utc": report.get("first_time_utc"),
        "last_time_utc": report.get("last_time_utc"),
        "integrity": report.get("integrity"),
    }
    demodulation_report = report.get("demodulation")
    if demodulation_report:
        digest["demodulation"] = {
            "windows": demodulation_report["windows"],
            "output_rate_hz": demodulation_report["output_rate_hz"],
            "bin_frequency_hz": demodulation_report["bin_frequency_hz"],
            "amplitude_mean": demodulation_report["amplitude"]["mean"],
            "unit": demodulation_report["unit"],
        }
    if "rbd_csv" in report:
        digest["rbd_csv"] = report["rbd_csv"]
    return digest


def digest_aux(report, report_file):
    return {
        "report_file": report_file,
        "file": report["file"],
        "total_records": report["total_records"],
        "valid_records": report["valid_records"],
        "stale_records": report["stale_records"],
        "stale_fraction": report["stale_fraction"],
        "stale_lag": report.get("stale_lag"),
        "opmode_counts_valid": report.get("opmode_counts_valid"),
        "opmode_counts_stale": report.get("opmode_counts_stale"),
    }


def digest_ws(report, report_file):
    return {
        "report_file": report_file,
        "file": report["file"],
        "total_valid_rows": report["total_valid_rows"],
        "rejected_lines": report["rejected_lines"],
        "repaired_timestamps": report["repaired_timestamps"],
        "first_time": report.get("first_time"),
        "last_time": report.get("last_time"),
        "stats": report.get("stats"),
    }


def process_day(day_key, day_info, paths, schemas, settings):
    """Processa um dia inteiro, gravando os relatórios e os CSV pedidos."""
    json_dir = paths["json_dir"]
    csv_dir = paths["csv_dir"]
    options = settings["options"]

    day_report = {
        "date": day_key,
        "day_dir": str(day_info["day_dir"]),
        "aux_dir": str(day_info["aux_dir"]),
        "hours": {},
        "rbd_schema_mode": schemas["rbd"].get("mode"),
        "aux_schema_mode": schemas["aux"].get("mode"),
        "rbd_schema_source": schemas["rbd"].get("source"),
        "aux_schema_source": schemas["aux"].get("source"),
    }

    daily = day_info["hours"].get("daily", {})
    if "ws" in daily:
        report = ws_report(daily["ws"], settings["sample_count"])
        name = "{}__ws_report.json".format(day_key)
        exporters.write_json(report, json_dir / name)
        day_report["weather_station"] = digest_ws(report, name)
        if settings["export_csv"]:
            exporters.export_ws_csv(daily["ws"], csv_dir / "{}__ws.csv".format(day_key))

    for hour_key, files in day_info["hours"].items():
        if hour_key == "daily":
            continue
        hour_report = {"hour": hour_key}

        if "rbd" in files:
            print("  {} {} rbd ...".format(day_key, hour_key), flush=True)
            report, deconv = rbd_report(files["rbd"], schemas["rbd"], settings["sample_count"],
                                        options, settings["analyser"])
            if settings["export_csv"] and settings["export_rbd_csv"]:
                csv_path = csv_dir / "{}__{}__rbd.csv".format(day_key, hour_key)
                print("    exportando CSV do sinal bruto ...", flush=True)
                settings["rbd_exporter"](files["rbd"], csv_path, schemas["rbd"], settings["csv_limit"])
                report["rbd_csv"] = csv_path.name

            name = "{}__{}__rbd_report.json".format(day_key, hour_key)
            exporters.write_json(report, json_dir / name)
            hour_report["rbd"] = digest_rbd(report, name)

            if deconv and settings["export_csv"]:
                exporters.export_deconv_csv(
                    deconv, csv_dir / "{}__{}__deconv.csv".format(day_key, hour_key),
                    report.get("demodulation", {}).get("unit"))

        if "aux" in files:
            print("  {} {} aux ...".format(day_key, hour_key), flush=True)
            report = aux_report(files["aux"], schemas["aux"], settings["sample_count"], options)
            name = "{}__{}__aux_report.json".format(day_key, hour_key)
            exporters.write_json(report, json_dir / name)
            hour_report["aux"] = digest_aux(report, name)
            if settings["export_csv"]:
                exporters.export_aux_csv(files["aux"],
                                         csv_dir / "{}__{}__aux.csv".format(day_key, hour_key),
                                         schemas["aux"], settings["csv_limit"])

        if "rbd" in hour_report and "aux" in hour_report:
            hour_report["pair_status"] = "paired"
        elif "rbd" in hour_report:
            hour_report["pair_status"] = "rbd_only"
        elif "aux" in hour_report:
            hour_report["pair_status"] = "aux_only"
        else:
            hour_report["pair_status"] = "empty"
        day_report["hours"][hour_key] = hour_report

    exporters.write_json(day_report, json_dir / "{}__day_report.json".format(day_key))
    return day_report
