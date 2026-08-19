"""
HATS report generator.

Reads the raw instrument files produced by HATS (OAFA / CRAAM) and writes JSON
reports plus optional CSV exports:

    .rbd   1 kHz detector stream (binary, layout described by XMLTables)
    .aux   telescope pointing from getPos/TheSkyX (binary, 80-byte records)
    .ws    weather station (ASCII, one line per reading)

Beyond decoding, this version demodulates the Golay signal at the chopper
frequency, which is the actual science product of the instrument.

Changes from v3
---------------
* AUX unit corrections. HATSAuxFormat.xml declares right_ascension as degrees
  and the tracking rates as degrees/s. The data is in hours and arcsec/s.
  Verified against solar ephemerides on 2026-03-17/18/19: aux_ra * 15 matches
  the Sun's RA to 0.01 deg, and the rates match its apparent motion to 1%.
  Original values are preserved; corrected copies are added alongside.
* AUX stale-record detection. 12-23% of records carry jd == 0 and a pointing
  solution that is a coherent snapshot ~1053.5 s old, while husec is current.
  They are now flagged, excluded from statistics, and counted.
* Weather station timestamps are validated. The raw files contain lines with a
  leading 0x7f byte; v3 passed them straight through into the output.
* Full-file statistics and integrity checks for RBD (v3 sampled 10 records out
  of 3.6 million and computed nothing).
* Goertzel demodulation at the chopper frequency, matching HATS_fft.c.

Stdlib only, no third-party dependencies.
"""

import argparse
import csv
import json
import math
import operator
import struct
import xml.etree.ElementTree as ET
from array import array
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

UTC = timezone.utc

TYPE_MAP = {
    "xs:int": ("i", 4),
    "xs:unsignedInt": ("I", 4),
    "xs:unsignedShort": ("H", 2),
    "xs:unsignedLong": ("Q", 8),
    "xs:double": ("d", 8),
}

# ---------------------------------------------------------------------------
# Signal processing defaults, from HATS_fft.h / windowed_dft.c
# ---------------------------------------------------------------------------

SAMPLING_FREQUENCY = 1000.0     # Hz, RBD sampling rate
TARGET_FREQUENCY = 20.0         # Hz, chopper frequency
WINDOW_SIZE = 128               # samples per demodulation window
STEPS = 32                      # window shift, in samples

# Amplitude correction of the ISO 18431-1 flat-top window. Chosen so that
# dividing the Goertzel output by the window length yields the amplitude of a
# pure sinusoid directly. Value taken from windowed_dft.c.
FLATTOP_CORRECTION = 2.000122419602196

# Nominal per-hour timing of the RBD stream.
HUSEC_PER_HOUR = 36000000
HUSEC_PER_SAMPLE = 10           # 1 kHz

# ---------------------------------------------------------------------------
# AUX unit corrections
# ---------------------------------------------------------------------------
#
# Maps a field to (declared_unit, actual_unit, factor_to_declared, new_name).
# The corrected value is actual_value * factor, stored under new_name. The
# original field is left untouched so nothing is lost.

AUX_UNIT_FIXES = {
    "right_ascension": ("degrees", "hours", 15.0, "right_ascension_deg"),
    "ra_rate": ("degrees/s", "arcsec/s", 1.0 / 3600.0, "ra_rate_deg_s"),
    "dec_rate": ("degrees/s", "arcsec/s", 1.0 / 3600.0, "dec_rate_deg_s"),
}

FALLBACK_RBD_SCHEMA = {
    "name": "fallback_rbd",
    "record_size": 38,
    "struct_format": "<IIHQiiiii",
    "fields": [
        {"name": "sample", "type": "xs:unsignedInt", "unit": "none", "size": 4, "fmt": "I", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "none"},
        {"name": "sec", "type": "xs:unsignedInt", "unit": "unix_time", "size": 4, "fmt": "I", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "unix_time"},
        {"name": "ms", "type": "xs:unsignedShort", "unit": "ms_of_second", "size": 2, "fmt": "H", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "ms_of_second"},
        {"name": "husec", "type": "xs:unsignedLong", "unit": "husec_0UTC", "size": 8, "fmt": "Q", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "husec_0UTC"},
        {"name": "golay", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.0, "slope": 0.001154, "converted_unit": "mV"},
        {"name": "chopper", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": -2.106, "slope": 0.00233, "converted_unit": "mV"},
        {"name": "temp_hics", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.013, "slope": 0.0000299, "converted_unit": "C"},
        {"name": "temp_env", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.013, "slope": 0.0000299, "converted_unit": "C"},
        {"name": "temp_golay", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.013, "slope": 0.0000299, "converted_unit": "C"},
    ],
}

FALLBACK_AUX_SCHEMA = {
    "name": "fallback_aux",
    "record_size": 80,
    "struct_format": "<Qddddddddii",
    "fields": [
        {"name": "husec", "type": "xs:unsignedLong", "unit": "husec_0UTC", "size": 8, "fmt": "Q", "dim": 1},
        {"name": "jd", "type": "xs:double", "unit": "day", "size": 8, "fmt": "d", "dim": 1},
        {"name": "sid", "type": "xs:double", "unit": "hour", "size": 8, "fmt": "d", "dim": 1},
        {"name": "elevation", "type": "xs:double", "unit": "degrees", "size": 8, "fmt": "d", "dim": 1},
        {"name": "azimuth", "type": "xs:double", "unit": "degrees", "size": 8, "fmt": "d", "dim": 1},
        {"name": "right_ascension", "type": "xs:double", "unit": "hours", "size": 8, "fmt": "d", "dim": 1},
        {"name": "declination", "type": "xs:double", "unit": "degrees", "size": 8, "fmt": "d", "dim": 1},
        {"name": "ra_rate", "type": "xs:double", "unit": "arcsec/s", "size": 8, "fmt": "d", "dim": 1},
        {"name": "dec_rate", "type": "xs:double", "unit": "arcsec/s", "size": 8, "fmt": "d", "dim": 1},
        {"name": "object", "type": "xs:int", "unit": "id", "size": 4, "fmt": "i", "dim": 1},
        {"name": "opmode", "type": "xs:int", "unit": "id", "size": 4, "fmt": "i", "dim": 1},
    ],
}

# Operation mode codes, from HATS.py extract_scans()/SkyDip() upstream.
#
# The mapping of 7 and 8 was swapped in HATS.py up to 2025-10-17; the 2026-04-17
# revision fixes it, with the changelog entry "Corrected extract_scans: opmode
# were wrong". The values below follow the corrected version.
#
# This cannot be confirmed from the data itself: in the 2026-03 files virtually
# every opmode 7/8 record is one of the stale ones (61 of 62), so the RA and Dec
# excursions during a scan are not observable.
OPMODE_NAMES = {0: "tracking", 7: "right_ascension_scan", 8: "declination_scan", 10: "skydip"}


class GenericRecord(object):
    def __init__(self, values):
        for key, value in values.items():
            setattr(self, key, value)


# ---------------------------------------------------------------------------
# Project layout
# ---------------------------------------------------------------------------

def ensure_project_structure(project_root, data_dir_name="Data", reports_dir_name="Reports", xml_dir_name="XMLTables"):
    data_dir = project_root / data_dir_name
    reports_dir = project_root / reports_dir_name
    json_dir = reports_dir / "json"
    csv_dir = reports_dir / "csv"
    xml_dir = project_root / xml_dir_name

    for directory in (data_dir, reports_dir, json_dir, csv_dir, xml_dir):
        directory.mkdir(parents=True, exist_ok=True)

    readme_path = data_dir / "README.txt"
    if not readme_path.exists():
        readme_path.write_text(
            "Expected layout:\n\n"
            "Data/\n"
            "  2026-03-17/\n"
            "    hats-2026-03-17T1800.rbd\n"
            "    aux/\n"
            "      hats-2026-03-17T1800.aux\n"
            "      hats-2026-03-17.ws\n",
            encoding="utf-8",
        )

    xml_readme_path = xml_dir / "README.txt"
    if not xml_readme_path.exists():
        xml_readme_path.write_text(
            "Optional official HATS schema files:\n\n"
            "XMLTables/\n"
            "  HATSDataFormat.xml      (or a symlink to the version in use)\n"
            "  HATSDataFormat-120.xml  (>= 2021-10-25, 38-byte records)\n"
            "  HATSDataFormat-110.xml  (<  2021-10-25, 26-byte records)\n"
            "  HATSAuxFormat.xml\n\n"
            "If these files are not present, the script uses the fixed fallback schema.\n\n"
            "Note: HATSAuxFormat.xml declares wrong units for right_ascension\n"
            "(hours, not degrees) and for ra_rate/dec_rate (arcsec/s, not degrees/s).\n"
            "The reader corrects this; see AUX_UNIT_FIXES.\n",
            encoding="utf-8",
        )

    return {"project_root": project_root, "data_dir": data_dir, "reports_dir": reports_dir, "json_dir": json_dir, "csv_dir": csv_dir, "xml_dir": xml_dir}


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
    """The AD7770 delivers 24 significant bits inside a 4-byte word, sign in bit 23."""
    adcu = int(value) & 0x00FFFFFF
    if int(value) & 0x800000:
        adcu -= 0x1000000
    return adcu


def summarize_numeric(values):
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {"count": len(values), "min": min(values), "max": max(values), "mean": float(sum(values)) / float(len(values))}


# Records read per chunk. Smaller is both faster and much lighter here: at 16384
# the working set stays in cache and peak memory is 34 MB against 151 MB at
# 131072, while the run time drops from 6.2 s to 4.3 s per hour of data.
RBD_CHUNK_RECORDS = 16384


def summarize_accumulator(accumulator):
    """Turn [min, max, sum, sum_of_squares, count] into the report dictionary."""
    lowest, highest, total, total_squares, count = accumulator
    if not count:
        return {"count": 0, "min": None, "max": None, "mean": None, "sd": None}
    mean = total / count
    variance = max(0.0, total_squares / count - mean * mean)
    return {"count": count, "min": lowest, "max": highest, "mean": mean, "sd": math.sqrt(variance)}


def scale_summary(summary, slope, offset):
    """Apply an affine calibration to an already computed summary."""
    if not summary["count"]:
        return dict(summary)
    lowest = summary["min"] * slope + offset
    highest = summary["max"] * slope + offset
    if slope < 0:
        lowest, highest = highest, lowest
    return {"count": summary["count"], "min": lowest, "max": highest,
            "mean": summary["mean"] * slope + offset, "sd": summary["sd"] * abs(slope)}


def summarize_values(values):
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None, "sd": None}
    count = len(values)
    total = sum(values)
    mean = total / count
    variance = max(0.0, sum(map(operator.mul, values, values)) / count - mean * mean)
    return {"count": count, "min": min(values), "max": max(values), "mean": mean, "sd": math.sqrt(variance)}


class RunningStats(object):
    """Streaming min/max/mean/sd, so a 3.6M-record file never has to be held in memory."""

    def __init__(self):
        self.n = 0
        self.total = 0.0
        self.total_sq = 0.0
        self.minimum = None
        self.maximum = None

    def add(self, value):
        value = float(value)
        self.n += 1
        self.total += value
        self.total_sq += value * value
        if self.minimum is None or value < self.minimum:
            self.minimum = value
        if self.maximum is None or value > self.maximum:
            self.maximum = value

    def result(self):
        if self.n == 0:
            return {"count": 0, "min": None, "max": None, "mean": None, "sd": None}
        mean = self.total / self.n
        variance = max(0.0, self.total_sq / self.n - mean * mean)
        return {"count": self.n, "min": self.minimum, "max": self.maximum, "mean": mean, "sd": math.sqrt(variance)}


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def tokenize_schema_text(text):
    clean = text.replace("﻿", " ").replace("<", " <").replace(">", "> ")
    tokens = []
    current = ""
    inside_tag = False

    for ch in clean:
        if ch == "<":
            inside_tag = True
            if current.strip():
                tokens.extend(current.strip().split())
            current = ""
        elif ch == ">":
            inside_tag = False
            current = ""
        elif not inside_tag:
            current += ch

    if current.strip():
        tokens.extend(current.strip().split())

    return [token for token in tokens if token.strip()]


def resolve_xml_pointer(xml_path):
    if not xml_path.exists():
        return xml_path
    text = xml_path.read_text(encoding="utf-8", errors="ignore").strip().replace("﻿", "").strip()
    if text.endswith(".xml") and len(text.split()) == 1:
        candidate = xml_path.parent / text
        if candidate.exists():
            return candidate
    return xml_path


def apply_aux_unit_metadata(fields):
    """Annotate AUX fields whose XML unit is wrong, so the report says so explicitly."""
    for field in fields:
        fix = AUX_UNIT_FIXES.get(field["name"])
        if not fix:
            continue
        declared, actual, factor, new_name = fix
        field["declared_unit"] = field.get("unit")
        field["unit"] = actual
        field["corrected_field"] = new_name
        field["corrected_unit"] = declared
        field["correction_factor"] = factor
    return fields


def parse_schema_from_flat_tokens(xml_path, kind):
    text = xml_path.read_text(encoding="utf-8", errors="ignore")
    tokens = tokenize_schema_text(text)
    fields = []
    i = 0

    while i < len(tokens):
        if i + 3 >= len(tokens):
            break

        name = tokens[i]
        try:
            dim = int(tokens[i + 1])
        except Exception:
            i += 1
            continue

        xml_type = tokens[i + 2]
        unit = tokens[i + 3]

        if xml_type not in TYPE_MAP:
            i += 1
            continue

        fmt, size = TYPE_MAP[xml_type]
        field = {"name": name, "dim": dim, "type": xml_type, "unit": unit, "fmt": fmt, "size": size}

        if kind == "rbd":
            if i + 8 >= len(tokens):
                break
            field["origin"] = tokens[i + 4]
            field["convert"] = tokens[i + 5]
            field["offset"] = float(tokens[i + 6])
            field["slope"] = float(tokens[i + 7])
            field["converted_unit"] = tokens[i + 8]
            i += 9
        else:
            i += 4

        fields.append(field)

    if not fields:
        raise ValueError("Could not parse schema fields from {}".format(xml_path))

    if kind == "aux":
        fields = apply_aux_unit_metadata(fields)

    struct_format = "<" + "".join(field["fmt"] * field["dim"] for field in fields)
    return {"name": xml_path.name, "source": str(xml_path), "mode": "xml", "fields": fields, "struct_format": struct_format, "record_size": struct.calcsize(struct_format)}


def parse_schema_from_xml_tree(xml_path, kind):
    root = ET.parse(str(xml_path)).getroot()
    fields = []

    for item in list(root):
        texts = [(child.text or "").strip() for child in list(item)]
        if len(texts) < 4:
            continue

        xml_type = texts[2]
        if xml_type not in TYPE_MAP:
            continue

        fmt, size = TYPE_MAP[xml_type]
        field = {"name": texts[0], "dim": int(texts[1]), "type": xml_type, "unit": texts[3], "fmt": fmt, "size": size}

        if kind == "rbd":
            field["origin"] = texts[4] if len(texts) > 4 else "sam4e"
            field["convert"] = texts[5] if len(texts) > 5 else "no"
            field["offset"] = float(texts[6]) if len(texts) > 6 and texts[6] else 0.0
            field["slope"] = float(texts[7]) if len(texts) > 7 and texts[7] else 1.0
            field["converted_unit"] = texts[8] if len(texts) > 8 else field["unit"]

        fields.append(field)

    if not fields:
        raise ValueError("Could not parse XML tree schema from {}".format(xml_path))

    if kind == "aux":
        fields = apply_aux_unit_metadata(fields)

    struct_format = "<" + "".join(field["fmt"] * field["dim"] for field in fields)
    return {"name": xml_path.name, "source": str(xml_path), "mode": "xml", "fields": fields, "struct_format": struct_format, "record_size": struct.calcsize(struct_format)}


def load_schema(xml_dir, kind):
    if kind == "rbd":
        candidates = [xml_dir / "HATSDataFormat.xml", xml_dir / "HATSDataFormat-120.xml", xml_dir / "HATSDataFormat-110.xml"]
        fallback = dict(FALLBACK_RBD_SCHEMA)
    else:
        candidates = [xml_dir / "HATSAuxFormat.xml"]
        fallback = dict(FALLBACK_AUX_SCHEMA)

    for candidate in candidates:
        candidate = resolve_xml_pointer(candidate)
        if not candidate.exists():
            continue
        try:
            return parse_schema_from_xml_tree(candidate, kind)
        except Exception:
            try:
                return parse_schema_from_flat_tokens(candidate, kind)
            except Exception:
                pass

    fallback["mode"] = "fallback_fixed_without_xml"
    fallback["source"] = "internal_fixed_schema"
    if kind == "aux":
        fallback["fields"] = apply_aux_unit_metadata([dict(f) for f in fallback["fields"]])
    return fallback


def unpack_record(chunk, schema):
    unpacked = struct.unpack(schema["struct_format"], chunk)
    values = {}
    index = 0

    for field in schema["fields"]:
        dim = field.get("dim", 1)
        if dim == 1:
            values[field["name"]] = unpacked[index]
        else:
            values[field["name"]] = list(unpacked[index:index + dim])
        index += dim

    return GenericRecord(values)


def iter_records(path, schema, chunk_records=65536, limit=None):
    """Stream a binary file record by record without loading it whole."""
    record_size = schema["record_size"]
    unpacker = struct.Struct(schema["struct_format"])
    emitted = 0
    with path.open("rb") as handle:
        while True:
            blob = handle.read(record_size * chunk_records)
            if not blob:
                return
            usable = len(blob) - (len(blob) % record_size)
            for offset in range(0, usable, record_size):
                if limit is not None and emitted >= limit:
                    return
                yield emitted, unpacker.unpack_from(blob, offset)
                emitted += 1
            if usable != len(blob):
                return


def sample_records(path, schema, sample_count):
    file_size = path.stat().st_size
    record_size = schema["record_size"]

    if file_size % record_size != 0:
        raise ValueError("File {} has size {}, not divisible by record size {}".format(path.name, file_size, record_size))

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


# ---------------------------------------------------------------------------
# Demodulation
# ---------------------------------------------------------------------------

def flattop_window(length):
    """Symmetric flat-top window, ISO 18431-1 type 0, as used by windowed_dft.c."""
    if length < 2:
        return [FLATTOP_CORRECTION] * length
    denominator = float(length - 1)
    window = []
    for i in range(length):
        weight = (1.0
                  - 1.9330 * math.cos(2.0 * math.pi * i / denominator)
                  + 1.2860 * math.cos(4.0 * math.pi * i / denominator)
                  - 0.3880 * math.cos(6.0 * math.pi * i / denominator)
                  + 0.0322 * math.cos(8.0 * math.pi * i / denominator))
        window.append(FLATTOP_CORRECTION * weight)
    return window


def goertzel_bin(target_frequency, window_size, sampling_frequency, bin_mode):
    """
    Frequency bin for the Goertzel recursion.

    HATS_fft.c uses floor(f*N/fs). At the nominal settings that is
    floor(20*128/1000) = 2, i.e. 15.625 Hz rather than 20 Hz. The flat-top
    window's wide, flat main lobe absorbs most of the error, but the residual
    scalloping is phase dependent and costs about 0.25% peak to peak.

    'reference' reproduces HATS_fft.c. 'exact' keeps the fractional bin, which
    removes the phase dependence. The Goertzel recursion itself does not
    require an integer bin.
    """
    exact = target_frequency * window_size / sampling_frequency
    if bin_mode == "exact":
        return exact
    return float(math.floor(exact))


def projection_vectors(window_size, target_frequency, sampling_frequency, bin_mode):
    """
    Flat-top window pre-multiplied by the analysis twiddles.

    The Goertzel recursion and a direct single-bin DFT give the same magnitude,
    but the DFT form is two dot products, which `sum(map(mul, ...))` evaluates in
    C rather than in the interpreter. Measured agreement with the recursion is
    3e-14, i.e. floating-point noise, for roughly a threefold speedup.
    """
    window = flattop_window(window_size)
    k = goertzel_bin(target_frequency, window_size, sampling_frequency, bin_mode)
    angle = 2.0 * math.pi * k / window_size
    cosines = [window[i] * math.cos(angle * i) for i in range(window_size)]
    sines = [window[i] * math.sin(angle * i) for i in range(window_size)]
    return cosines, sines


def goertzel_amplitude(signal, offset, window, coefficient, window_size):
    """
    Textbook Goertzel over signal[offset:offset+window_size]. Kept as the
    reference implementation; demodulate() uses the faster equivalent above.

    windowed_dft.c terminates the recursion one sample early (it returns s[N-2]
    and s[N-3] instead of s[N-1] and s[N-2]); the difference measured against a
    direct DFT is about 0.003%, so the correct form is used here.
    """
    s1 = 0.0
    s2 = 0.0
    for i in range(window_size):
        s0 = window[i] * signal[offset + i] + coefficient * s1 - s2
        s2 = s1
        s1 = s0
    return math.sqrt(max(0.0, s1 * s1 + s2 * s2 - coefficient * s1 * s2))


def demodulate(signal, husec, window_size=WINDOW_SIZE, steps=STEPS,
               target_frequency=TARGET_FREQUENCY, sampling_frequency=SAMPLING_FREQUENCY,
               bin_mode="reference"):
    """
    Sliding-window amplitude of the chopped signal, mirroring HATS_fft.c.

    Returns (husec_list, amplitude_list). Each amplitude is timestamped with the
    husec at the centre of its window, and divided by window_size so that it is
    already in the units of the input.
    """
    total = len(signal)
    if total < window_size or steps <= 0:
        return array("Q"), array("d")

    # HATS_fft.c: num_windows = floor((nrec - window_size + 1) / steps).
    # Kept as is so output lengths match the reference pipeline.
    num_windows = (total - window_size + 1) // steps
    if num_windows <= 0:
        return array("Q"), array("d")

    cosines, sines = projection_vectors(window_size, target_frequency, sampling_frequency, bin_mode)
    half = window_size // 2
    mul = operator.mul
    hypot = math.hypot

    samples = signal if isinstance(signal, list) else list(signal)
    out_husec = array("Q", [0]) * num_windows
    out_amplitude = array("d", [0.0]) * num_windows

    for w in range(num_windows):
        offset = w * steps
        piece = samples[offset:offset + window_size]
        out_amplitude[w] = hypot(sum(map(mul, cosines, piece)),
                                 sum(map(mul, sines, piece))) / window_size
        out_husec[w] = husec[offset + half]

    return out_husec, out_amplitude


# ---------------------------------------------------------------------------
# RBD
# ---------------------------------------------------------------------------

def interpreted_rbd_value(field, raw_value):
    value = raw_value
    if field.get("origin") == "ad7770":
        value = decode_ad7770(value)
    if field.get("convert") == "yes":
        value = value * field.get("slope", 1.0) + field.get("offset", 0.0)
    return value


def analyse_rbd(path, schema, options):
    """
    Single streaming pass over an RBD file: statistics, integrity, demodulation.

    Processes a chunk at a time and works column by column rather than record by
    record. A per-record loop over five channels costs roughly forty interpreted
    operations per record, which dominates the run time at 3.6 million records per
    hour; transposing a chunk with zip(*Struct.iter_unpack(...)) moves nearly all
    of that into C. Calibrated statistics are derived analytically from the raw
    ones, since the conversion is affine, which avoids one multiplication per
    sample per channel.
    """
    record_size = schema["record_size"]
    file_size = path.stat().st_size
    if file_size % record_size != 0:
        raise ValueError("File {} has size {}, not divisible by record size {}".format(path.name, file_size, record_size))

    date_str = detect_date_from_name(path)
    hour_str = detect_hour_from_name(path)
    hour_start_husec = int(hour_str[:2]) * HUSEC_PER_HOUR if hour_str and hour_str[:2].isdigit() else None

    names = []
    for field in schema["fields"]:
        names.extend([field["name"]] * field.get("dim", 1))
    index_of = {name: i for i, name in enumerate(names)}

    total_records = file_size // record_size
    if options["record_limit"] is not None:
        total_records = min(total_records, options["record_limit"])

    converted_fields = [f for f in schema["fields"] if f.get("convert") == "yes" or f.get("origin") == "ad7770"]
    # per channel: [min, max, sum, sum_of_squares, count] over decoded ADC units
    accumulators = {f["name"]: [None, None, 0.0, 0.0, 0] for f in converted_fields}

    golay_field = next((f for f in schema["fields"] if f["name"] == "golay"), None)
    husec_index = index_of.get("husec")
    sample_index = index_of.get("sample")

    demodulating = (options["demodulate"] and golay_field is not None and husec_index is not None
                    and total_records >= options["window_size"])
    window_size = options["window_size"]
    steps = options["steps"]
    if demodulating:
        # HATS_fft.c: num_windows = floor((nrec - window_size + 1) / steps).
        num_windows = (total_records - window_size + 1) // steps
        demodulating = num_windows > 0
    if demodulating:
        cosines, sines = projection_vectors(window_size, options["target_frequency"],
                                            options["sampling_frequency"], options["bin_mode"])
        mul = operator.mul
        hypot = math.hypot
        half = window_size // 2
        out_husec = array("Q")
        out_amplitude = array("d")
        pending_signal = []
        pending_husec = []
        consumed = 0        # absolute index of pending_signal[0]
        window_start = 0    # absolute index of the next window

    unpacker = struct.Struct(schema["struct_format"])
    subtract = operator.sub
    multiply = operator.mul

    total = 0
    before_hour = 0
    sample_leaps = 0
    husec_leaps = 0
    last_sample = None
    last_husec = None
    first_husec = None
    final_husec = None

    with path.open("rb") as handle:
        while total < total_records:
            wanted = record_size * min(RBD_CHUNK_RECORDS, total_records - total)
            blob = handle.read(wanted)
            if not blob:
                break
            usable = len(blob) - (len(blob) % record_size)
            if usable != len(blob):
                blob = blob[:usable]
            columns = list(zip(*unpacker.iter_unpack(blob)))
            if not columns:
                break
            count = len(columns[0])
            total += count

            if husec_index is not None:
                husec_column = columns[husec_index]
                if first_husec is None:
                    first_husec = husec_column[0]
                final_husec = husec_column[-1]
                if hour_start_husec is not None:
                    before_hour += sum(map(hour_start_husec.__gt__, husec_column))
                deltas = list(map(subtract, husec_column[1:], husec_column))
                husec_leaps += len(deltas) - deltas.count(HUSEC_PER_SAMPLE)
                if last_husec is not None and husec_column[0] - last_husec != HUSEC_PER_SAMPLE:
                    husec_leaps += 1
                last_husec = husec_column[-1]

            if sample_index is not None:
                sample_column = columns[sample_index]
                deltas = list(map(subtract, sample_column[1:], sample_column))
                sample_leaps += len(deltas) - deltas.count(1)
                if last_sample is not None and sample_column[0] - last_sample != 1:
                    sample_leaps += 1
                last_sample = sample_column[-1]

            for field in converted_fields:
                column = columns[index_of[field["name"]]]
                if field.get("origin") == "ad7770":
                    # 24 significant bits, sign in bit 23. Branchless: when bit 23
                    # is set, (v & 0x800000) << 1 is exactly the 0x1000000 excess.
                    column = [(v & 0x00FFFFFF) - ((v & 0x800000) << 1) for v in column]
                accumulator = accumulators[field["name"]]
                lowest = min(column)
                highest = max(column)
                accumulator[0] = lowest if accumulator[0] is None else min(accumulator[0], lowest)
                accumulator[1] = highest if accumulator[1] is None else max(accumulator[1], highest)
                accumulator[2] += sum(column)
                accumulator[3] += sum(map(multiply, column, column))
                accumulator[4] += count

                if demodulating and field is golay_field:
                    slope = field.get("slope", 1.0)
                    offset = field.get("offset", 0.0)
                    pending_signal.extend([v * slope + offset for v in column])
                    pending_husec.extend(columns[husec_index])

            if demodulating:
                # Emit every window fully contained in what has been read so far,
                # then drop the consumed prefix: memory stays flat regardless of
                # file size.
                limit = total - window_size
                while len(out_amplitude) < num_windows and window_start <= limit:
                    local = window_start - consumed
                    piece = pending_signal[local:local + window_size]
                    out_amplitude.append(hypot(sum(map(mul, cosines, piece)),
                                               sum(map(mul, sines, piece))) / window_size)
                    out_husec.append(pending_husec[local + half])
                    window_start += steps
                if window_start > consumed:
                    drop = window_start - consumed
                    del pending_signal[:drop]
                    del pending_husec[:drop]
                    consumed = window_start

    result = {
        "total_records": total,
        "first_time_utc": dt_from_husec(date_str, first_husec) if first_husec is not None else None,
        "last_time_utc": dt_from_husec(date_str, final_husec) if final_husec is not None else None,
        "integrity": {
            "sample_number_leaps": sample_leaps,
            "husec_leaps": husec_leaps,
            "records_before_nominal_hour": before_hour,
            "note": "HATS.py drops records with husec < hour*36000000; they are kept here and only counted.",
        },
        "statistics_adcu": {},
        "statistics_calibrated": {},
        "units_calibrated": {f["name"]: f.get("converted_unit") for f in converted_fields},
    }

    for field in converted_fields:
        name = field["name"]
        result["statistics_adcu"][name] = summarize_accumulator(accumulators[name])
        if field.get("convert") == "yes":
            # The conversion is affine, so the statistics transform exactly.
            result["statistics_calibrated"][name] = scale_summary(
                result["statistics_adcu"][name], field.get("slope", 1.0), field.get("offset", 0.0))

    if demodulating and len(out_amplitude):
        amplitude_values = list(out_amplitude)
        exact_bin = options["target_frequency"] * window_size / options["sampling_frequency"]
        used_bin = goertzel_bin(options["target_frequency"], window_size, options["sampling_frequency"], options["bin_mode"])
        result["demodulation"] = {
            "method": "flattop_single_bin_dft",
            "target_frequency_hz": options["target_frequency"],
            "sampling_frequency_hz": options["sampling_frequency"],
            "window_size": window_size,
            "steps": steps,
            "output_rate_hz": options["sampling_frequency"] / steps,
            "bin_mode": options["bin_mode"],
            "bin_exact": exact_bin,
            "bin_used": used_bin,
            "bin_frequency_hz": used_bin * options["sampling_frequency"] / window_size,
            "windows": len(amplitude_values),
            "unit": golay_field.get("converted_unit") if golay_field else None,
            "amplitude": summarize_values(amplitude_values),
            "first_time_utc": dt_from_husec(date_str, out_husec[0]),
            "last_time_utc": dt_from_husec(date_str, out_husec[-1]),
        }
        result["_deconv"] = (out_husec, out_amplitude, date_str)

    return result


def report_rbd(path, schema, sample_count, options):
    sampled, total_records = sample_records(path, schema, sample_count)
    date_str = detect_date_from_name(path)
    samples = []

    for idx, rec in sampled:
        row = {"index": idx}
        for field in schema["fields"]:
            raw_value = getattr(rec, field["name"])
            row[field["name"]] = raw_value
            if field.get("origin") == "ad7770" or field.get("convert") == "yes":
                row[field["name"] + "_interpreted"] = interpreted_rbd_value(field, raw_value)

        if hasattr(rec, "sec") and hasattr(rec, "ms"):
            row["datetime_utc"] = dt_from_unix_time(rec.sec, rec.ms)
        if hasattr(rec, "husec"):
            row["datetime_utc_from_husec"] = dt_from_husec(date_str, rec.husec)
        samples.append(row)

    # Ponto de injeção: hats_report_v5 passa aqui um analisador acelerado por
    # numpy. Sem ele, usa o caminho stdlib deste módulo.
    analyser = options.get("analyser") or analyse_rbd
    analysis = analyser(path, schema, options)
    deconv = analysis.pop("_deconv", None)

    report = {
        "file": path.name,
        "type": "rbd",
        "date": date_str,
        "hour": detect_hour_from_name(path),
        "parent_folder": str(path.parent),
        "file_size_bytes": path.stat().st_size,
        "record_size_bytes": schema["record_size"],
        "total_records": total_records,
        "schema_mode": schema.get("mode"),
        "schema_source": schema.get("source"),
        "schema": schema["fields"],
        "sampled_records": samples,
    }
    report.update(analysis)
    return report, deconv


# ---------------------------------------------------------------------------
# AUX
# ---------------------------------------------------------------------------

def aux_corrected_values(record):
    """Values converted into the units HATSAuxFormat.xml claims to use."""
    corrected = {}
    for name, (_declared, _actual, factor, new_name) in AUX_UNIT_FIXES.items():
        value = getattr(record, name, None)
        if value is not None:
            corrected[new_name] = float(value) * factor
    return corrected


def aux_record_valid(record):
    """
    A record is usable only when its Julian date was filled in.

    Records with jd == 0 carry a pointing solution that is a coherent snapshot
    of an earlier moment (~1053.5 s on 2026-03-17/18/19) while husec is current,
    so they cannot be paired with the RBD stream.
    """
    jd = getattr(record, "jd", None)
    return jd is not None and float(jd) != 0.0


def aux_pointing_state(record):
    azimuth = getattr(record, "azimuth", None)
    elevation = getattr(record, "elevation", None)
    ra = getattr(record, "right_ascension", None)
    dec = getattr(record, "declination", None)
    pointing_zeroed = True

    if azimuth is not None and elevation is not None and (float(azimuth) != 0.0 or float(elevation) != 0.0):
        pointing_zeroed = False
    if ra is not None and dec is not None and (float(ra) != 0.0 or float(dec) != 0.0):
        pointing_zeroed = False

    valid = aux_record_valid(record)
    return {
        "record_valid": valid,
        "pointing_zeroed": pointing_zeroed,
        # kept for backward compatibility with v3 output, but now it also
        # requires a filled Julian date
        "pointing_valid": valid and not pointing_zeroed,
    }


def estimate_stale_lag(valid_pairs, invalid_pairs):
    """
    Least-squares fit of sidereal time against husec over the valid records,
    then evaluate how far behind that line each invalid record sits.

    Returns the lag in seconds, or None when there is nothing to measure.
    """
    if len(valid_pairs) < 2 or not invalid_pairs:
        return None

    n = len(valid_pairs)
    sum_x = sum(h for h, _ in valid_pairs)
    sum_y = sum(s for _, s in valid_pairs)
    sum_xx = sum(h * h for h, _ in valid_pairs)
    sum_xy = sum(h * s for h, s in valid_pairs)
    denominator = n * sum_xx - sum_x * sum_x
    if denominator == 0:
        return None

    slope = (n * sum_xy - sum_x * sum_y) / denominator
    intercept = (sum_y - slope * sum_x) / n

    lags = [((slope * h + intercept) - s) * 3600.0 for h, s in invalid_pairs]
    mean = sum(lags) / len(lags)
    variance = sum((v - mean) ** 2 for v in lags) / len(lags)
    return {"mean_seconds": mean, "sd_seconds": math.sqrt(variance), "min_seconds": min(lags), "max_seconds": max(lags)}


def analyse_aux(path, schema, options):
    """Full pass over an AUX file, separating valid from stale records."""
    names = []
    for field in schema["fields"]:
        names.extend([field["name"]] * field.get("dim", 1))
    index_of = {name: i for i, name in enumerate(names)}

    jd_index = index_of.get("jd")
    sid_index = index_of.get("sid")
    husec_index = index_of.get("husec")
    opmode_index = index_of.get("opmode")
    object_index = index_of.get("object")

    total = 0
    valid = 0
    opmode_valid = Counter()
    opmode_stale = Counter()
    object_valid = Counter()
    valid_pairs = []
    invalid_pairs = []
    stats = {name: RunningStats() for name in ("elevation", "azimuth", "declination")}
    ra_deg_stats = RunningStats()

    for _, values in iter_records(path, schema, limit=options["record_limit"]):
        total += 1
        is_valid = jd_index is not None and float(values[jd_index]) != 0.0
        if is_valid:
            valid += 1

        if husec_index is not None and sid_index is not None:
            pair = (float(values[husec_index]), float(values[sid_index]))
            (valid_pairs if is_valid else invalid_pairs).append(pair)

        if opmode_index is not None:
            (opmode_valid if is_valid else opmode_stale)[values[opmode_index]] += 1
        if object_index is not None and is_valid:
            object_valid[values[object_index]] += 1

        if is_valid:
            for name, running in stats.items():
                if name in index_of:
                    running.add(values[index_of[name]])
            if "right_ascension" in index_of:
                ra_deg_stats.add(float(values[index_of["right_ascension"]]) * AUX_UNIT_FIXES["right_ascension"][2])

    stale = total - valid
    return {
        "total_records": total,
        "valid_records": valid,
        "stale_records": stale,
        "stale_fraction": (float(stale) / total) if total else None,
        "stale_lag": estimate_stale_lag(valid_pairs, invalid_pairs),
        "stale_record_note": (
            "Records with jd == 0 hold a pointing solution from an earlier epoch while "
            "husec is current. They are excluded from all statistics below."
        ),
        "opmode_counts_valid": {str(k): v for k, v in sorted(opmode_valid.items())},
        "opmode_counts_stale": {str(k): v for k, v in sorted(opmode_stale.items())},
        "opmode_names": {str(k): v for k, v in OPMODE_NAMES.items()},
        "object_counts_valid": {str(k): v for k, v in sorted(object_valid.items())},
        "statistics_valid": dict(
            [(name, running.result()) for name, running in stats.items()]
            + [("right_ascension_deg", ra_deg_stats.result())]
        ),
    }


def report_aux(path, schema, sample_count, options):
    sampled, total_records = sample_records(path, schema, sample_count)
    date_str = detect_date_from_name(path)
    samples = []

    for idx, rec in sampled:
        row = {"index": idx}
        for field in schema["fields"]:
            row[field["name"]] = getattr(rec, field["name"])
        row.update(aux_corrected_values(rec))
        if hasattr(rec, "husec"):
            row["datetime_utc"] = dt_from_husec(date_str, rec.husec)
        row.update(aux_pointing_state(rec))
        samples.append(row)

    report = {
        "file": path.name,
        "type": "aux",
        "date": date_str,
        "hour": detect_hour_from_name(path),
        "parent_folder": str(path.parent),
        "file_size_bytes": path.stat().st_size,
        "record_size_bytes": schema["record_size"],
        "total_records": total_records,
        "schema_mode": schema.get("mode"),
        "schema_source": schema.get("source"),
        "schema": schema["fields"],
        "unit_corrections": {
            name: {"declared_in_xml": declared, "actual": actual, "corrected_field": new_name, "factor": factor}
            for name, (declared, actual, factor, new_name) in AUX_UNIT_FIXES.items()
        },
        "sampled_records": samples,
    }
    report.update(analyse_aux(path, schema, options))
    return report


# ---------------------------------------------------------------------------
# Weather station
# ---------------------------------------------------------------------------

CONTROL_CHARS = "".join(chr(c) for c in list(range(0, 32)) + [127])
CONTROL_TABLE = {ord(c): None for c in CONTROL_CHARS}


def parse_ws_value(token, suffix):
    """
    Read 'Xx=12.3U' style fields. The pressure field ends in 'HPa' on some
    firmware and plain 'H' on others, so the unit is located rather than
    assumed to be one character.
    """
    body = token.split("=", 1)[1]
    cut = body.find(suffix)
    if cut < 0:
        cut = len(body) - 1
    return float(body[:cut])


def parse_ws_line(line):
    parts = line.strip().split(",")
    if len(parts) != 5:
        return None
    try:
        stamp = parts[0].translate(CONTROL_TABLE).strip()
        # Validate rather than pass the string through: the raw files contain
        # lines with a stray 0x7f byte in front of the timestamp.
        parsed = datetime.fromisoformat(stamp)
        return {
            "time": parsed.isoformat(),
            "station_code": parts[1].translate(CONTROL_TABLE).strip(),
            "temperature_c": parse_ws_value(parts[2], "C"),
            "humidity": parse_ws_value(parts[3], "P"),
            "pressure_hpa": parse_ws_value(parts[4], "H"),
            "repaired": stamp != parts[0].strip(),
        }
    except Exception:
        return None


def read_ws_rows(path):
    rows = []
    rejected = 0
    repaired = 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            row = parse_ws_line(raw_line)
            if row is None:
                rejected += 1
                continue
            if row.pop("repaired"):
                repaired += 1
            rows.append(row)
    return rows, rejected, repaired


def report_ws(path, sample_count):
    rows, rejected, repaired = read_ws_rows(path)
    station_codes = Counter(row["station_code"] for row in rows)
    temperatures = [row["temperature_c"] for row in rows]
    humidities = [row["humidity"] for row in rows]
    pressures = [row["pressure_hpa"] for row in rows]

    sampled = (rows[:sample_count] + rows[-sample_count:]) if len(rows) > sample_count else rows
    return {
        "file": path.name,
        "type": "ws",
        "date": detect_date_from_name(path),
        "parent_folder": str(path.parent),
        "total_valid_rows": len(rows),
        "rejected_lines": rejected,
        "repaired_timestamps": repaired,
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


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_rbd_csv(path, out_csv, schema, limit):
    date_str = detect_date_from_name(path)
    names = []
    for field in schema["fields"]:
        names.extend([field["name"]] * field.get("dim", 1))
    index_of = {name: i for i, name in enumerate(names)}

    headers = []
    for field in schema["fields"]:
        headers.append(field["name"])
        if field.get("origin") == "ad7770" or field.get("convert") == "yes":
            headers.append(field["name"] + "_interpreted")
    headers.extend(["datetime_utc", "datetime_utc_from_husec"])

    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(headers)
        for _, values in iter_records(path, schema, limit=limit):
            row = []
            for field in schema["fields"]:
                raw_value = values[index_of[field["name"]]]
                row.append(raw_value)
                if field.get("origin") == "ad7770" or field.get("convert") == "yes":
                    row.append(interpreted_rbd_value(field, raw_value))
            sec = values[index_of["sec"]] if "sec" in index_of else None
            ms = values[index_of["ms"]] if "ms" in index_of else None
            row.append(dt_from_unix_time(sec, ms) if sec is not None and ms is not None else None)
            husec = values[index_of["husec"]] if "husec" in index_of else None
            row.append(dt_from_husec(date_str, husec) if husec is not None else None)
            writer.writerow(row)


def export_aux_csv(path, out_csv, schema, limit):
    date_str = detect_date_from_name(path)
    names = []
    for field in schema["fields"]:
        names.extend([field["name"]] * field.get("dim", 1))
    index_of = {name: i for i, name in enumerate(names)}

    headers = [field["name"] for field in schema["fields"]]
    headers.extend([fix[3] for fix in AUX_UNIT_FIXES.values()])
    headers.extend(["opmode_name", "datetime_utc", "record_valid", "pointing_valid", "pointing_zeroed"])

    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(headers)
        for _, values in iter_records(path, schema, limit=limit):
            record = GenericRecord({name: values[i] for name, i in index_of.items()})
            row = [values[index_of[field["name"]]] for field in schema["fields"]]
            corrected = aux_corrected_values(record)
            row.extend([corrected.get(fix[3]) for fix in AUX_UNIT_FIXES.values()])
            opmode = getattr(record, "opmode", None)
            row.append(OPMODE_NAMES.get(opmode, "unknown") if opmode is not None else None)
            husec = getattr(record, "husec", None)
            row.append(dt_from_husec(date_str, husec) if husec is not None else None)
            state = aux_pointing_state(record)
            row.extend([state["record_valid"], state["pointing_valid"], state["pointing_zeroed"]])
            writer.writerow(row)


def export_deconv_csv(deconv, out_csv, unit):
    out_husec, out_amplitude, date_str = deconv
    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["husec", "datetime_utc", "amplitude_{}".format(unit or "raw")])
        for husec, amplitude in zip(out_husec, out_amplitude):
            writer.writerow([husec, dt_from_husec(date_str, husec), amplitude])


def export_ws_csv(path, out_csv):
    rows, _rejected, _repaired = read_ws_rows(path)
    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["time", "station_code", "temperature_c", "humidity", "pressure_hpa"])
        for row in rows:
            writer.writerow([row["time"], row["station_code"], row["temperature_c"], row["humidity"], row["pressure_hpa"]])


def write_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

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

        index[day_dir.name] = {"day_dir": day_dir, "aux_dir": aux_dir, "hours": dict(sorted(hours.items(), key=lambda item: item[0]))}
    return index


def digest_rbd(report, report_file):
    """Resumo do relatório do RBD para o day_report, com ponteiro para o arquivo completo."""
    digest = {
        "report_file": report_file,
        "file": report["file"],
        "total_records": report["total_records"],
        "first_time_utc": report.get("first_time_utc"),
        "last_time_utc": report.get("last_time_utc"),
        "integrity": report.get("integrity"),
    }
    demodulation = report.get("demodulation")
    if demodulation:
        digest["demodulation"] = {
            "windows": demodulation["windows"],
            "output_rate_hz": demodulation["output_rate_hz"],
            "bin_frequency_hz": demodulation["bin_frequency_hz"],
            "amplitude_mean": demodulation["amplitude"]["mean"],
            "unit": demodulation["unit"],
        }
    if "rbd_csv" in report:
        digest["rbd_csv"] = report["rbd_csv"]
    return digest


def digest_aux(report, report_file):
    """Resumo do relatório do AUX para o day_report."""
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
    """Resumo do relatório da estação meteorológica para o day_report."""
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


def process_day(day_key, day_info, json_dir, csv_dir, rbd_schema, aux_schema, sample_count, export_csv, csv_limit, options):
    day_report = {
        "date": day_key,
        "day_dir": str(day_info["day_dir"]),
        "aux_dir": str(day_info["aux_dir"]),
        "hours": {},
        "rbd_schema_mode": rbd_schema.get("mode"),
        "aux_schema_mode": aux_schema.get("mode"),
        "rbd_schema_source": rbd_schema.get("source"),
        "aux_schema_source": aux_schema.get("source"),
    }

    if "daily" in day_info["hours"] and "ws" in day_info["hours"]["daily"]:
        ws_path = day_info["hours"]["daily"]["ws"]
        ws_report = report_ws(ws_path, sample_count)
        ws_report_name = "{}__ws_report.json".format(day_key)
        write_json(ws_report, json_dir / ws_report_name)
        day_report["weather_station"] = digest_ws(ws_report, ws_report_name)
        if export_csv:
            export_ws_csv(ws_path, csv_dir / "{}__ws.csv".format(day_key))

    for hour_key, files in day_info["hours"].items():
        if hour_key == "daily":
            continue
        hour_report = {"hour": hour_key}

        if "rbd" in files:
            print("  {} {} rbd ...".format(day_key, hour_key), flush=True)
            rbd_report, deconv = report_rbd(files["rbd"], rbd_schema, sample_count, options)
            rbd_report_name = "{}__{}__rbd_report.json".format(day_key, hour_key)
            write_json(rbd_report, json_dir / rbd_report_name)
            # Referência, não cópia: o relatório completo já está no próprio arquivo.
            hour_report["rbd"] = digest_rbd(rbd_report, rbd_report_name)
            # O CSV do RBD é opt-in: são 3,6 M linhas e ~767 MB por hora, contra
            # 0,4 s de análise. Quem quiser pede explicitamente, e aí usa o
            # exportador dedicado (acelerado por numpy no v5, quando disponível).
            if export_csv and options.get("export_rbd_csv"):
                exporter = options.get("rbd_csv_exporter") or export_rbd_csv
                csv_path = csv_dir / "{}__{}__rbd.csv".format(day_key, hour_key)
                print("    exportando CSV do RBD ...", flush=True)
                exporter(files["rbd"], csv_path, rbd_schema, csv_limit)
                rbd_report["rbd_csv"] = csv_path.name
            if deconv and export_csv:
                unit = rbd_report.get("demodulation", {}).get("unit")
                export_deconv_csv(deconv, csv_dir / "{}__{}__deconv.csv".format(day_key, hour_key), unit)

        if "aux" in files:
            print("  {} {} aux ...".format(day_key, hour_key), flush=True)
            aux_report = report_aux(files["aux"], aux_schema, sample_count, options)
            aux_report_name = "{}__{}__aux_report.json".format(day_key, hour_key)
            write_json(aux_report, json_dir / aux_report_name)
            hour_report["aux"] = digest_aux(aux_report, aux_report_name)
            if export_csv:
                export_aux_csv(files["aux"], csv_dir / "{}__{}__aux.csv".format(day_key, hour_key), aux_schema, csv_limit)

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
    parser = argparse.ArgumentParser(description="Decode, validate and demodulate HATS data files.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--data-dir", default="Data")
    parser.add_argument("--reports-dir", default="Reports")
    parser.add_argument("--xml-dir", default="XMLTables")
    parser.add_argument("--init-project", action="store_true")
    parser.add_argument("--sample-count", type=int, default=5)
    parser.add_argument("--export-csv", action="store_true")
    parser.add_argument("--csv-limit", type=int, default=None, help="Rows per CSV export. Default: all.")
    parser.add_argument("--export-rbd-csv", action="store_true",
                        help="Também exporta o CSV do sinal bruto de 1 kHz. Desligado por padrão: "
                             "são ~3,6 M linhas e ~767 MB por hora de dados.")
    parser.add_argument("--from-data-dir", action="store_true")
    parser.add_argument("--day", default=None)
    parser.add_argument("--record-limit", type=int, default=None,
                        help="Records read per binary file. Default: all. Useful for quick runs.")
    parser.add_argument("--no-demod", action="store_true", help="Skip the 20 Hz demodulation.")
    parser.add_argument("--fft-window", type=int, default=WINDOW_SIZE)
    parser.add_argument("--fft-steps", type=int, default=STEPS)
    parser.add_argument("--fft-target-hz", type=float, default=TARGET_FREQUENCY)
    parser.add_argument("--fft-sampling-hz", type=float, default=SAMPLING_FREQUENCY)
    parser.add_argument("--fft-bin-mode", choices=["reference", "exact"], default="reference",
                        help="'reference' reproduces HATS_fft.c (floor of the bin index). "
                             "'exact' keeps the fractional bin and removes phase-dependent scalloping.")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    paths = ensure_project_structure(project_root, args.data_dir, args.reports_dir, args.xml_dir)

    if args.init_project:
        print("Project initialized at {}".format(project_root))
        print("Data folder: {}".format(paths["data_dir"]))
        print("Reports folder: {}".format(paths["reports_dir"]))
        print("XML folder: {}".format(paths["xml_dir"]))
        return

    if not args.from_data_dir:
        raise SystemExit("Use --from-data-dir to process the Data folder.")

    options = {
        "demodulate": not args.no_demod,
        "window_size": args.fft_window,
        "steps": args.fft_steps,
        "target_frequency": args.fft_target_hz,
        "sampling_frequency": args.fft_sampling_hz,
        "bin_mode": args.fft_bin_mode,
        "record_limit": args.record_limit,
        "export_rbd_csv": args.export_rbd_csv,
    }

    rbd_schema = load_schema(paths["xml_dir"], "rbd")
    aux_schema = load_schema(paths["xml_dir"], "aux")
    day_index = build_day_index(paths["data_dir"])

    if args.day:
        day_index = {k: v for k, v in day_index.items() if k == args.day}
    if not day_index:
        raise SystemExit("No day folders found inside {}.".format(paths["data_dir"]))

    reports = {}
    for day_key, day_info in day_index.items():
        reports[day_key] = process_day(day_key, day_info, paths["json_dir"], paths["csv_dir"],
                                       rbd_schema, aux_schema, args.sample_count,
                                       args.export_csv, args.csv_limit, options)

    summary = {
        "project_root": str(project_root),
        "data_dir": str(paths["data_dir"]),
        "reports_dir": str(paths["reports_dir"]),
        "xml_dir": str(paths["xml_dir"]),
        "rbd_schema_mode": rbd_schema.get("mode"),
        "aux_schema_mode": aux_schema.get("mode"),
        "rbd_schema_source": rbd_schema.get("source"),
        "aux_schema_source": aux_schema.get("source"),
        "demodulation_options": options,
        "aux_unit_corrections": {
            name: {"declared_in_xml": declared, "actual": actual, "corrected_field": new_name, "factor": factor}
            for name, (declared, actual, factor, new_name) in AUX_UNIT_FIXES.items()
        },
        "days_processed": list(reports.keys()),
        # Referências, não cópias: cada day_report já está no seu próprio arquivo.
        "day_reports": {day: "{}__day_report.json".format(day) for day in reports},
    }
    write_json(summary, paths["reports_dir"] / "summary.json")
    print("Processed {} day(s).".format(len(reports)))
    print("RBD schema: {} ({})".format(rbd_schema.get("mode"), rbd_schema.get("source")))
    print("AUX schema: {} ({})".format(aux_schema.get("mode"), aux_schema.get("source")))
    print("Summary written to: {}".format(paths["reports_dir"] / "summary.json"))


if __name__ == "__main__":
    main()
