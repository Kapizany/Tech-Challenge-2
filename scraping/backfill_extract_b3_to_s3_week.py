import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf

from extract_b3_to_s3 import (
    TICKERS_30,
    build_yf_session,
    sanitize_tickers,
    write_parquet_partitioned_daily,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reprocessa o scraping da B3 para um intervalo de datas e grava "
            "um quotes.parquet por dia em raw/dt=YYYY-MM-DD/."
        )
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Data inicial no formato YYYY-MM-DD. Default: segunda-feira desta semana.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Data final no formato YYYY-MM-DD. Default: hoje.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=6,
        help="Quantidade de tickers por batch no yfinance.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Quantidade de retries por dia em caso de falha.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=180,
        help="Tempo de espera entre retries.",
    )
    return parser.parse_args()


def _parse_iso_date(value: str | None, default: date) -> date:
    if not value:
        return default
    return date.fromisoformat(value)


def _default_start_date(today: date) -> date:
    return today - timedelta(days=today.weekday())


def _iter_dates(start_date: date, end_date: date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def _download_one_day_batched(
    tickers: list[str],
    target_date: date,
    batch_size: int,
    session,
) -> pd.DataFrame | None:
    if batch_size <= 0:
        raise ValueError("batch_size precisa ser maior que zero.")

    start_str = target_date.isoformat()
    end_exclusive = (target_date + timedelta(days=1)).isoformat()
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    rows: list[pd.DataFrame] = []

    logging.info(
        "Downloading quotes for %d tickers on %s (batch_size=%d)...",
        len(tickers),
        start_str,
        batch_size,
    )

    for batch_idx, start in enumerate(range(0, len(tickers), batch_size), start=1):
        batch = tickers[start:start + batch_size]
        tickers_str = " ".join(batch)
        logging.info(
            "Downloading batch %d/%d (%d tickers)...",
            batch_idx,
            total_batches,
            len(batch),
        )

        df = yf.download(
            tickers=tickers_str,
            start=start_str,
            end=end_exclusive,
            interval="1d",
            group_by="ticker",
            threads=True,
            progress=True,
            session=session,
        )

        if df is None or df.empty:
            logging.warning("Batch %d retornou vazio e será ignorado.", batch_idx)
            continue

        if isinstance(df.columns, pd.MultiIndex):
            for ticker in batch:
                try:
                    sub_df = df.xs(ticker, axis=1, level=1)
                except Exception:
                    sub_df = df.xs(ticker, axis=1, level=0)

                if sub_df is None or sub_df.empty:
                    continue

                sub_df = sub_df.reset_index()
                sub_df["ticker"] = ticker
                rows.append(sub_df)
        else:
            df = df.reset_index()
            df["ticker"] = batch[0]
            rows.append(df)

    if not rows:
        logging.warning("Nenhum dado retornado pelo yfinance para dt=%s.", target_date.isoformat())
        return None

    out = pd.concat(rows, ignore_index=True)
    out = out.drop_duplicates(subset=["ticker", "Date"], keep="last")

    if "Date" not in out.columns:
        raise RuntimeError("Coluna Date não encontrada após normalização.")

    out["Date"] = pd.to_datetime(out["Date"])
    out["dt"] = out["Date"].dt.date.astype(str)
    out["year"] = out["Date"].dt.year
    out["month"] = out["Date"].dt.month
    out["day"] = out["Date"].dt.day

    preferred_cols = [
        "Date",
        "ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "dt",
        "year",
        "month",
        "day",
    ]
    cols = [col for col in preferred_cols if col in out.columns] + [
        col for col in out.columns if col not in preferred_cols
    ]
    out = out[cols].sort_values(["dt", "ticker", "Date"]).reset_index(drop=True)

    # Para o backfill, o script precisa produzir exatamente a particao da data alvo.
    out = out[out["dt"] == target_date.isoformat()].reset_index(drop=True)
    if out.empty:
        logging.warning("Sem linhas para dt=%s apos normalizacao.", target_date.isoformat())
        return None

    return out


def _write_one_day_with_retries(
    day_df: pd.DataFrame,
    s3_bucket: str,
    s3_prefix: str,
    target_dt: str,
    max_retries: int,
    sleep_seconds: int,
) -> None:
    attempt = 0
    while True:
        try:
            attempt += 1
            logging.info("Writing dt=%s attempt %d/%d", target_dt, attempt, 1 + max_retries)
            out_path = write_parquet_partitioned_daily(day_df, s3_bucket, s3_prefix)
            logging.info(
                "SUCCESS dt=%s path=%s rows=%d tickers=%d",
                target_dt,
                out_path,
                len(day_df),
                day_df["ticker"].nunique(),
            )
            return
        except Exception as exc:
            logging.exception("Erro ao gravar dt=%s na tentativa %d: %s", target_dt, attempt, exc)
            if attempt == max_retries + 1:
                raise
            logging.info("Aguardando %d segundos antes do retry de dt=%s...", sleep_seconds, target_dt)
            time.sleep(sleep_seconds)


def main() -> None:
    args = parse_args()

    today = datetime.now().date()
    start_date = _parse_iso_date(args.start_date, _default_start_date(today))
    end_date = _parse_iso_date(args.end_date, today)

    if start_date > end_date:
        raise ValueError("--start-date não pode ser maior que --end-date.")

    s3_bucket = os.environ.get("RAW_BUCKET", "capizani-techchallenge")
    s3_prefix = os.environ.get("RAW_PREFIX", "raw")

    if not s3_bucket:
        raise ValueError("Defina a variável de ambiente RAW_BUCKET.")

    tickers = sanitize_tickers(TICKERS_30)
    yf_session = build_yf_session()

    processed_dates: list[str] = []
    skipped_dates: list[str] = []

    for current_date in _iter_dates(start_date, end_date):
        day_df = _download_one_day_batched(
            tickers=tickers,
            target_date=current_date,
            batch_size=args.batch_size,
            session=yf_session,
        )
        if day_df is None:
            skipped_dates.append(current_date.isoformat())
            continue

        target_dt = current_date.isoformat()
        _write_one_day_with_retries(
            day_df=day_df,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            target_dt=target_dt,
            max_retries=args.max_retries,
            sleep_seconds=args.sleep_seconds,
        )
        processed_dates.append(target_dt)

    logging.info(
        "Resumo do backfill | processadas=%s | sem_dados=%s",
        ", ".join(processed_dates) if processed_dates else "(nenhuma)",
        ", ".join(skipped_dates) if skipped_dates else "(nenhuma)",
    )


if __name__ == "__main__":
    main()
