import argparse
import logging
import re
from datetime import date
from typing import Iterable, Optional

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

DT_QUOTES_RE = re.compile(r".*/dt=(\d{4}-\d{2}-\d{2})/quotes\.parquet$")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Corrige Parquet no S3 com timestamp em nanos (ns) para micros (us) "
            "em paths no formato raw/dt=YYYY-MM-DD/quotes.parquet."
        )
    )
    p.add_argument("--bucket", required=True, help="Nome do bucket S3.")
    p.add_argument("--prefix", default="raw", help="Prefixo base no bucket (default: raw).")
    p.add_argument("--dt-from", default=None, help="Data inicial (YYYY-MM-DD).")
    p.add_argument("--dt-to", default=None, help="Data final (YYYY-MM-DD).")
    p.add_argument("--max-files", type=int, default=0, help="Limite de arquivos (0 = sem limite).")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Aplica a correção (sem esta flag, executa somente dry-run).",
    )
    p.add_argument(
        "--backup-prefix",
        default="",
        help=(
            "Se informado, copia o original antes da sobrescrita para "
            "s3://bucket/<backup-prefix>/<key-original>."
        ),
    )
    p.add_argument("--profile", default=None, help="AWS profile opcional.")
    p.add_argument("--region", default=None, help="AWS region opcional.")
    return p.parse_args()


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def iter_quote_keys(s3_client, bucket: str, prefix: str) -> Iterable[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    base_prefix = prefix.rstrip("/") + "/"
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if DT_QUOTES_RE.match(key):
                yield key


def _extract_dt_from_key(key: str) -> date:
    m = DT_QUOTES_RE.match(key)
    if not m:
        raise ValueError(f"Path fora do padrão esperado: {key}")
    return date.fromisoformat(m.group(1))


def _build_arrow_s3_fs(session: boto3.Session, region: Optional[str]) -> pafs.S3FileSystem:
    creds = session.get_credentials()
    frozen = creds.get_frozen_credentials() if creds else None

    kwargs = {}
    if region:
        kwargs["region"] = region
    if frozen:
        kwargs["access_key"] = frozen.access_key
        kwargs["secret_key"] = frozen.secret_key
        if frozen.token:
            kwargs["session_token"] = frozen.token
    return pafs.S3FileSystem(**kwargs)


def _convert_ns_to_us(table: pa.Table) -> tuple[pa.Table, int]:
    new_arrays = []
    new_fields = []
    converted_cols = 0

    for field, col in zip(table.schema, table.columns):
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            target = pa.timestamp("us", tz=field.type.tz)
            new_arrays.append(pc.cast(col, target, safe=False))
            new_fields.append(pa.field(field.name, target, nullable=field.nullable, metadata=field.metadata))
            converted_cols += 1
        else:
            new_arrays.append(col)
            new_fields.append(field)

    if converted_cols == 0:
        return table, 0

    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    return pa.Table.from_arrays(new_arrays, schema=new_schema), converted_cols


def main() -> None:
    args = parse_args()

    dt_from = _parse_iso_date(args.dt_from)
    dt_to = _parse_iso_date(args.dt_to)
    if dt_from and dt_to and dt_from > dt_to:
        raise ValueError("--dt-from não pode ser maior que --dt-to.")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    s3fs = _build_arrow_s3_fs(session, args.region)

    scanned = 0
    matched = 0
    converted = 0
    skipped_no_ns = 0
    failed = 0

    logging.info(
        "Iniciando varredura: s3://%s/%s | modo=%s",
        args.bucket,
        args.prefix.rstrip("/"),
        "APPLY" if args.apply else "DRY-RUN",
    )

    for key in iter_quote_keys(s3, args.bucket, args.prefix):
        scanned += 1
        if args.max_files > 0 and matched >= args.max_files:
            break

        dt = _extract_dt_from_key(key)
        if dt_from and dt < dt_from:
            continue
        if dt_to and dt > dt_to:
            continue

        matched += 1
        object_path = f"{args.bucket}/{key}"

        try:
            table = pq.read_table(object_path, filesystem=s3fs)
            fixed_table, changed_cols = _convert_ns_to_us(table)
            if changed_cols == 0:
                skipped_no_ns += 1
                logging.info("SKIP sem colunas ns: s3://%s/%s", args.bucket, key)
                continue

            if args.apply:
                if args.backup_prefix:
                    backup_key = f"{args.backup_prefix.rstrip('/')}/{key}"
                    s3.copy_object(
                        Bucket=args.bucket,
                        CopySource={"Bucket": args.bucket, "Key": key},
                        Key=backup_key,
                    )
                    logging.info("Backup criado: s3://%s/%s", args.bucket, backup_key)

                pq.write_table(
                    fixed_table,
                    object_path,
                    filesystem=s3fs,
                    compression="snappy",
                )
                logging.info(
                    "FIXED s3://%s/%s | colunas timestamp(ns)->us=%d",
                    args.bucket,
                    key,
                    changed_cols,
                )
            else:
                logging.info(
                    "DRY-RUN corrigiria: s3://%s/%s | colunas timestamp(ns)->us=%d",
                    args.bucket,
                    key,
                    changed_cols,
                )

            converted += 1
        except Exception as exc:
            failed += 1
            logging.exception("ERRO ao processar s3://%s/%s: %s", args.bucket, key, exc)

    logging.info(
        "Resumo | scanned=%d matched=%d converted=%d skipped_no_ns=%d failed=%d mode=%s",
        scanned,
        matched,
        converted,
        skipped_no_ns,
        failed,
        "APPLY" if args.apply else "DRY-RUN",
    )


if __name__ == "__main__":
    main()
