#!/usr/bin/env python3
# Fast script to filter Reddit dumps and output directly to Parquet format
# No intermediate compression - reads zst files and writes straight to Parquet

import zstandard
import os
import sys
import time
import argparse
import re
import logging.handlers
import multiprocessing
from collections import defaultdict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Import orjson if available, otherwise use standard json
try:
    import orjson
    USE_ORJSON = True
except ImportError:
    import json
    USE_ORJSON = False
    print("Warning: orjson not installed, using standard json (slower). Install with: pip install orjson")

# Create wrapper functions for JSON operations
if USE_ORJSON:
    def json_loads(data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        elif not isinstance(data, bytes):
            data = str(data).encode('utf-8')
        return orjson.loads(data)
else:
    import json
    json_loads = json.loads

# Setup logging
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')

log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)

if not os.path.exists("logs"):
    os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(
    os.path.join("logs", "bot.log"), maxBytes=1024*1024*16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)


class FileReader:
    """Reads zst compressed ndjson files"""

    @staticmethod
    def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
        chunk = reader.read(chunk_size)
        bytes_read += chunk_size
        if previous_chunk is not None:
            chunk = previous_chunk + chunk
        try:
            return chunk.decode()
        except UnicodeDecodeError:
            if bytes_read > max_window_size:
                raise UnicodeError(
                    f"Unable to decode frame after reading {bytes_read:,} bytes")
            return FileReader.read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

    @staticmethod
    def yield_lines(file_path):
        """Yield lines from a zst compressed file"""
        with open(file_path, 'rb') as file_handle:
            buffer = ''
            reader = zstandard.ZstdDecompressor(
                max_window_size=2**31).stream_reader(file_handle)
            while True:
                chunk = FileReader.read_and_decode(reader, 2**28, (2**29) * 2)
                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")

                for line in lines[:-1]:
                    yield line

                buffer = lines[-1]
            reader.close()


def process_file(file_path, field, values, partial, regex):
    """Process a single file and return matched records"""
    matched_records = []
    lines_processed = 0
    error_lines = 0

    # Pre-optimize: extract single value if applicable
    value = None
    if len(values) == 1:
        value = min(values)

    try:
        for line in FileReader.yield_lines(file_path):
            try:
                obj = json_loads(line)
                matched = False
                observed = obj[field].lower()

                # Optimized matching logic
                if regex:
                    for reg in values:
                        if reg.search(observed):
                            matched = True
                            break
                elif partial:
                    for val in values:
                        if val in observed:
                            matched = True
                            break
                else:
                    if value is not None:
                        matched = (observed == value)
                    else:
                        matched = (observed in values)

                if matched:
                    matched_records.append(obj)
            except (KeyError, json.JSONDecodeError, AttributeError) as err:
                error_lines += 1

            lines_processed += 1

            # Log progress every 5M lines
            if lines_processed % 5000000 == 0:
                log.info(
                    f"{file_path}: {lines_processed:,} lines processed, {len(matched_records):,} matched")

    except Exception as err:
        log.error(f"Error processing {file_path}: {err}")

    log.info(
        f"Completed {file_path}: {lines_processed:,} lines, {len(matched_records):,} matched, {error_lines:,} errors")
    return matched_records


def process_file_wrapper(args):
    """Wrapper for multiprocessing"""
    return process_file(*args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Filter Reddit dumps and output to Parquet")
    parser.add_argument(
        "input", help="The input folder to recursively read files from")
    parser.add_argument(
        "--output", help="Output Parquet file path", default="output.parquet")
    parser.add_argument(
        "--field", help="Field to filter on", default="subreddit")
    parser.add_argument(
        "--value", help="Value(s) to match (comma separated, case insensitive)", default="pushshift")
    parser.add_argument(
        "--value_list", help="File with newline separated values", default=None)
    parser.add_argument(
        "--processes", help="Number of processes to use", default=10, type=int)
    parser.add_argument(
        "--file_filter", help="Regex filenames must match", default="^RC_|^RS_")
    parser.add_argument("--partial", help="Partial match instead of exact",
                        action='store_true', default=False)
    parser.add_argument("--regex", help="Treat values as regex",
                        action='store_true', default=False)
    parser.add_argument(
        "--batch_size", help="Number of files to process before writing to Parquet", default=10, type=int)

    args = parser.parse_args()

    log.info(f"Loading files from: {args.input}")
    log.info(f"Output file: {args.output}")

    # Parse values
    values = set()
    if args.value_list:
        log.info(f"Reading {args.value_list} for values")
        with open(args.value_list, 'r') as f:
            for line in f:
                values.add(line.strip())
    else:
        values = set(args.value.split(","))

    if args.regex:
        regexes = []
        for reg in values:
            regexes.append(re.compile(reg))
        values = regexes
        log.info(
            f"Checking field {args.field} against {len(values)} regex(es)")
    else:
        lower_values = set()
        for val in values:
            lower_values.add(val.strip().lower())
        values = lower_values
        if args.partial:
            log.info(
                f"Checking if any of {len(values)} value(s) are contained in field {args.field}")
        else:
            log.info(
                f"Checking if field {args.field} matches any of {len(values)} value(s)")

    # Find all input files
    input_files = []
    for subdir, dirs, files in os.walk(args.input):
        files.sort()
        for file_name in files:
            if file_name.endswith(".zst") and re.search(args.file_filter, file_name) is not None:
                input_path = os.path.join(subdir, file_name)
                input_files.append(input_path)

    log.info(f"Found {len(input_files)} files to process")

    if len(input_files) == 0:
        log.error("No files found!")
        sys.exit(1)

    def normalize_dataframe(df):
        """Normalize data types for Parquet compatibility"""
        # Common problematic fields in Reddit data
        problematic_fields = ['edited', 'archived', 'locked', 'removed', 'deleted', 'is_self',
                              'stickied', 'spoiler', 'over_18', 'pinned', 'quarantine']

        for field in problematic_fields:
            if field in df.columns:
                # Convert to string to avoid type conflicts
                df[field] = df[field].astype(str)

        # Convert all remaining object columns to string to be safe
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)

        return df

    # Process files in batches
    all_records = []
    total_processed = 0
    start_time = time.time()

    # Prepare arguments for multiprocessing
    process_args = [(f, args.field, values, args.partial, args.regex)
                    for f in input_files]

    # Process with multiprocessing
    with multiprocessing.Pool(processes=args.processes) as pool:
        for i, records in enumerate(pool.imap(process_file_wrapper, process_args)):
            all_records.extend(records)
            total_processed += 1

            log.info(
                f"Progress: {total_processed}/{len(input_files)} files, {len(all_records):,} total records")

            # Write to Parquet in batches to manage memory
            if total_processed % args.batch_size == 0 and len(all_records) > 0:
                log.info(
                    f"Writing batch to Parquet... ({len(all_records):,} records)")
                df = pd.DataFrame(all_records)
                df = normalize_dataframe(df)

                # Append to parquet file
                if total_processed == args.batch_size:
                    # First batch - create new file
                    df.to_parquet(args.output, engine='pyarrow',
                                  compression='snappy', index=False)
                else:
                    # Append to existing file
                    table = pa.Table.from_pandas(df)
                    existing_table = pq.read_table(args.output)
                    combined = pa.concat_tables([existing_table, table])
                    pq.write_table(combined, args.output, compression='snappy')

                log.info(f"Batch written, clearing memory")
                all_records = []

    # Write final batch
    if len(all_records) > 0:
        log.info(
            f"Writing final batch to Parquet... ({len(all_records):,} records)")
        df = pd.DataFrame(all_records)
        df = normalize_dataframe(df)

        if total_processed <= args.batch_size:
            df.to_parquet(args.output, engine='pyarrow',
                          compression='snappy', index=False)
        else:
            table = pa.Table.from_pandas(df)
            existing_table = pq.read_table(args.output)
            combined = pa.concat_tables([existing_table, table])
            pq.write_table(combined, args.output, compression='snappy')

    elapsed = time.time() - start_time
    log.info(
        f"Complete! Processed {total_processed} files in {elapsed:.1f} seconds")
    log.info(f"Output saved to: {args.output}")

    # Print file info
    if os.path.exists(args.output):
        file_size = os.path.getsize(args.output)
        log.info(f"Output file size: {file_size / (1024**2):.2f} MB")

        # Read and show record count
        table = pq.read_table(args.output)
        log.info(f"Total records in output: {len(table):,}")
