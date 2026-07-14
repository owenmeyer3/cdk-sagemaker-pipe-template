"""
Lambda (invoked as a Step Functions Task state, chained after TransformTask):
converts SageMaker Batch Transform output into the same JSON-Lines "data
capture" format written by a real-time endpoint's DataCaptureConfig, so
downstream Model Monitor checks/validators can consume batch and real-time
inference identically.

Assumes the TransformJob is run WITH:
    "DataProcessing": {"JoinSource": "Input", "OutputFilter": "$"}
so each .out line already contains the original input CSV columns followed
by the prediction column(s), comma-joined, in guaranteed 1:1 order with the
input - no separate input file needs to be read or correlated.

Expected invocation payload (pass these as Parameters on the Step Functions
Task state that calls this Lambda, using JSONPath / States.Format to pull
the dynamic values out of the preceding TransformTask's execution context):
{
  "transform_job_name": "<from TransformTask output / States.Format>",
  "output_s3_uri": "s3://my-bucket/batch-output/<exec-name>/",  # same as TransformOutput.S3OutputPath
  "input_data_source": {"S3DataSource": {"S3Uri": "s3://...", "S3DataType": "S3Prefix"}},
      # same structure as TransformInput.DataSource - used to auto-derive column
      # count from the first row, instead of hardcoding num_input_columns.
      # Pass "num_input_columns" explicitly instead if you'd rather skip the lookup.
  "capture_dir": "s3://my-monitor-bucket/datacapture",   # bucket + base prefix for capture output
  "execution_start_time.$": "$$.Execution.StartTime",    # ISO 8601, e.g. 2026-07-12T14:23:45.678Z
  "endpoint_name_alias": "my-batch-pseudo-endpoint",   # matches monitoring schedule destination
  "variant_name_alias": "AllTraffic",
  "content_type": "text/csv",
  "encoding": "CSV"
}

The yyyy/mm/dd/hh capture partition is derived from execution_start_time
(the pipeline execution's actual start, from the Step Functions context
object) rather than wall-clock time when this Lambda happens to run - this
keeps the capture partition consistent with $$.Execution.Name, which
TransformTask already uses to build its own output path, so both trace back
to the same execution deterministically.
"""

import base64
import csv
import io
import json
import uuid
from datetime import datetime
from urllib.parse import urlparse

import boto3

s3 = boto3.client("s3")

_RANGE_READ_BYTES = 8192  # enough for a first line unless columns are unusually wide


def _parse_s3_uri(uri: str):
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def _list_objects(bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):
                yield obj["Key"]


def _first_line_via_range(bucket: str, key: str, max_bytes: int = _RANGE_READ_BYTES) -> str:
    """Reads just enough of the object to get its first line, without
    downloading the whole file. Doubles the range and retries if no
    newline is found (handles unusually wide first rows)."""
    fetched = max_bytes
    while True:
        resp = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{fetched - 1}")
        chunk = resp["Body"].read()
        newline_idx = chunk.find(b"\n")
        if newline_idx != -1:
            return chunk[:newline_idx].decode("utf-8").strip()

        content_range = resp.get("ContentRange", "")  # e.g. "bytes 0-8191/45000"
        total_size = int(content_range.split("/")[-1]) if "/" in content_range else len(chunk)
        if fetched >= total_size:
            # entire object is one line (or object is smaller than range) - use as-is
            return chunk.decode("utf-8").strip()
        fetched = min(fetched * 4, total_size)


def infer_input_column_count(input_s3_file) -> int:

    bucket, prefix = _parse_s3_uri(input_s3_file)
    try:
        first_key = next(_list_objects(bucket, prefix))
    except StopIteration:
        raise ValueError(f"No objects found under s3://{bucket}/{prefix}")

    first_line = _first_line_via_range(bucket, first_key)
    row = next(csv.reader(io.StringIO(first_line)))
    return len(row)


def _split_joined_row(line: str, num_input_cols: int):
    """Splits a JoinSource='Input' output row into (input_part, output_part)
    strings, using csv.reader so quoted fields containing commas are
    handled correctly rather than naively split on every comma."""
    row = next(csv.reader(io.StringIO(line)))
    input_cols, output_cols = row[:num_input_cols], row[num_input_cols:]

    out_buf = io.StringIO()
    writer = csv.writer(out_buf, lineterminator="")
    writer.writerow(input_cols)
    input_part = out_buf.getvalue()

    out_buf = io.StringIO()
    writer = csv.writer(out_buf, lineterminator="")
    writer.writerow(output_cols)
    output_part = out_buf.getvalue()

    return input_part, output_part


def _read_lines(bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    for raw_line in obj["Body"].iter_lines():
        line = raw_line.decode("utf-8").strip()
        if line:
            yield line


def _parse_execution_start_time(iso_str: str) -> datetime:
    """Parses $$.Execution.StartTime, e.g. '2026-07-12T14:23:45.678Z'.
    Python's fromisoformat accepts 'Z' from 3.11 onward, but this stays
    explicit/defensive in case the Lambda runtime is ever pinned older."""
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def _build_capture_record(input_data: str, output_data: str, content_type: str,
                           encoding: str, event_id: str, inference_time: str) -> dict:
    return {
        "captureData": {
            "endpointInput": {
                "observedContentType": content_type,
                "mode": "INPUT",
                "data": base64.b64encode(input_data.encode("utf-8")).decode("ascii"),
                "encoding": encoding,
            },
            "endpointOutput": {
                "observedContentType": content_type,
                "mode": "OUTPUT",
                "data": base64.b64encode(output_data.encode("utf-8")).decode("ascii"),
                "encoding": encoding,
            },
        },
        "eventMetadata": {
            "eventId": event_id,
            "inferenceTime": inference_time,
        },
        "eventVersion": "0",
    }


def transform_out_to_data_capture_handler(event, context):
    job_name = event["transform_job_name"]
    out_bucket, out_prefix = _parse_s3_uri(event["output_s3_dir"])
    capture_bucket, capture_prefix = _parse_s3_uri(event["capture_dir"])
    endpoint_alias = event["endpoint_name_alias"]
    variant_alias = event.get("variant_name_alias", "AllTraffic")
    content_type = event.get("content_type", "text/csv")
    encoding = event.get("encoding", "CSV")

    if "num_input_columns" in event:
        num_input_cols = event["num_input_columns"]
    else:
        # Derive it straight from TransformInput.DataSource - same structure
        # CreateTransformJob takes - so no integer needs to be hand-maintained.
        num_input_cols = infer_input_column_count(event["input_s3_file"])

    exec_start = _parse_execution_start_time(event["execution_start_time"])
    partition = exec_start.strftime("%Y/%m/%d/%H")
    dest_prefix = f"{capture_prefix}/{endpoint_alias}/{variant_alias}/{partition}"
    inference_time = exec_start.strftime("%Y-%m-%dT%H:%M:%S.") + f"{exec_start.microsecond // 1000:03d}Z"

    written_files = []

    for key in _list_objects(out_bucket, out_prefix):
        if not key.endswith(".out"):
            continue

        capture_lines = []
        for row_idx, line in enumerate(_read_lines(out_bucket, key)):
            input_part, output_part = _split_joined_row(line, num_input_cols)

            event_id = str(uuid.uuid4())

            record = _build_capture_record(
                input_part, output_part, content_type, encoding, event_id, inference_time
            )
            capture_lines.append(json.dumps(record))

        if not capture_lines:
            continue

        source_filename = key.rsplit("/", 1)[-1]
        dest_key = f"{dest_prefix}/{source_filename}.jsonl"

        s3.put_object(
            Bucket=capture_bucket,
            Key=dest_key,
            Body=("\n".join(capture_lines) + "\n").encode("utf-8"),
            ContentType="application/json",
        )
        written_files.append(f"s3://{capture_bucket}/{dest_key}")

        s3.delete_object(Bucket=out_bucket, Key=key)

    return {
        "STATUS_CODE": 200,
        "TRANSFORM_JOB_NAME": job_name,
        "NUM_INPUT_COLUMNS_USED": num_input_cols,
        "CAPTURE_FILES_WRITTEN": written_files,
        "CAPTURE_DESTINATION_PREFIX": f"s3://{capture_bucket}/{dest_prefix}",
    }