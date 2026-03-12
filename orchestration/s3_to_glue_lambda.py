import json
import logging
import os
import re
from urllib.parse import unquote_plus

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

# Espera eventos de objetos no padrão: raw/dt=YYYY-MM-DD/quotes.parquet
RAW_OBJECT_RE = re.compile(r"^raw/dt=\d{4}-\d{2}-\d{2}/quotes\.parquet$")


def _is_target_key(key: str) -> bool:
    if key.endswith("/"):
        return False
    if key.endswith("_$folder$"):
        return False
    return bool(RAW_OBJECT_RE.match(key))


def _build_s3_root(bucket: str) -> str:
    env_root = os.environ.get("S3_ROOT", "").strip()
    if env_root:
        return env_root.rstrip("/")
    return f"s3://{bucket}"


def lambda_handler(event, context):
    glue_job_name = os.environ["GLUE_JOB_NAME"]
    lookback_days = os.environ.get("LOOKBACK_DAYS", "7")

    records = event.get("Records", [])
    if not records:
        logger.info("Evento sem records.")
        return {"statusCode": 200, "body": "No records"}

    started = []
    skipped = []
    failed = []

    for rec in records:
        try:
            if rec.get("eventSource") != "aws:s3":
                skipped.append({"reason": "not_s3", "record": rec})
                continue

            bucket = rec["s3"]["bucket"]["name"]
            key = unquote_plus(rec["s3"]["object"]["key"])

            if not _is_target_key(key):
                skipped.append({"reason": "key_not_target", "bucket": bucket, "key": key})
                continue

            input_s3_uri = f"s3://{bucket}/{key}"
            s3_root = _build_s3_root(bucket)

            args = {
                "--S3_ROOT": s3_root,
                "--INPUT_S3_URI": input_s3_uri,
                "--LOOKBACK_DAYS": lookback_days,
            }

            resp = glue.start_job_run(JobName=glue_job_name, Arguments=args)
            started.append(
                {
                    "bucket": bucket,
                    "key": key,
                    "job_name": glue_job_name,
                    "job_run_id": resp["JobRunId"],
                    "args": args,
                }
            )
            logger.info("Glue disparado para %s (run_id=%s)", input_s3_uri, resp["JobRunId"])
        except Exception as exc:
            logger.exception("Falha ao processar record: %s", exc)
            failed.append({"error": str(exc), "record": rec})

    result = {"started": started, "skipped": skipped, "failed": failed}
    logger.info("Resumo da execução: %s", json.dumps(result, default=str))
    return {"statusCode": 200, "body": json.dumps(result, default=str)}
