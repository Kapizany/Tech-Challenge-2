import os
import time
import logging
from typing import List, Optional
import pandas as pd
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)



TICKERS_100: List[str] = [
    "PETR4.SA","VALE3.SA","ITUB4.SA","BBDC4.SA","ABEV3.SA","BBAS3.SA","WEGE3.SA","B3SA3.SA","MGLU3.SA","LREN3.SA",
    "SUZB3.SA","JBSS3.SA","PRIO3.SA","RAIL3.SA","RENT3.SA","HAPV3.SA","RADL3.SA","GGBR4.SA","CSNA3.SA","EMBR3.SA",
    "AZUL4.SA","GOLL4.SA","BRFS3.SA","MRFG3.SA","CVCB3.SA","AMER3.SA","VIVT3.SA","TIMS3.SA","TOTS3.SA","UGPA3.SA",
    "EQTL3.SA","CPFE3.SA","TAEE11.SA","TRPL4.SA","CMIG4.SA","ENBR3.SA","ENGI11.SA","ELET3.SA","ELET6.SA","EGIE3.SA",
    "CPLE6.SA","SBSP3.SA","SANB11.SA","BBDC3.SA","BBSE3.SA","IRBR3.SA","PSSA3.SA","QUAL3.SA","BEEF3.SA","SLCE3.SA",
    "KLBN11.SA","BRKM5.SA","USIM5.SA","GOAU4.SA","APER3.SA","CCRO3.SA","ECOR3.SA","SMTO3.SA","JHSF3.SA","CYRE3.SA",
    "MRVE3.SA","EZTC3.SA","TEND3.SA","DIRR3.SA","MULT3.SA","IGTI11.SA","ALSO3.SA","BRML3.SA","VBBR3.SA","CSAN3.SA",
    "RRRP3.SA","YDUQ3.SA","COGN3.SA","SEER3.SA","ANIM3.SA","CASH3.SA","PETZ3.SA","ASAI3.SA","GMAT3.SA","MOVI3.SA",
    "LCAM3.SA","ARZZ3.SA","SOMA3.SA","AURE3.SA","ALPA4.SA","VAMO3.SA","BLAU3.SA","HYPE3.SA","FLRY3.SA","RDOR3.SA",
    "DXCO3.SA","CIEL3.SA","NTCO3.SA","PCAR3.SA","CRFB3.SA","MYPK3.SA","POMO4.SA","RAPT4.SA","STBP3.SA","ODPV3.SA"
]


def sanitize_tickers(tickers: List[str]) -> List[str]:
    out = []
    for t in tickers:
        t = t.strip().upper()
        if t.endswith(".SA") and "?" not in t and len(t) >= 6:
            out.append(t)
    
    return list(dict.fromkeys(out)) 


def download_daily_batched(tickers: List[str], lookback_days: str = "5d", batch_size: int = 10) -> pd.DataFrame:
    """
    Baixa um período curto (ex.: 10 dias) e pega o último pregão disponível por ticker.
    Isso evita falhar em fins de semana/feriados.
    """
    if batch_size <= 0:
        raise ValueError("batch_size precisa ser maior que zero.")

    logging.info(
        "Downloading daily quotes for %d tickers via yfinance (batch_size=%d)...",
        len(tickers),
        batch_size,
    )

    total_batches = (len(tickers) + batch_size - 1) // batch_size
    rows = []
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
            period=lookback_days,
            interval="1d",
            group_by="ticker",
            threads=True,
            progress=True,
        )

        if df is None or df.empty:
            logging.warning("Batch %d retornou vazio e será ignorado.", batch_idx)
            continue

        if isinstance(df.columns, pd.MultiIndex):
            # normal: colunas multiindex quando há múltiplos tickers
            for t in batch:
                # tenta extrair por nível (às vezes varia)
                sub_simple = None
                try:
                    # formato comum: columns level 1 = ticker
                    sub_simple = df.xs(t, axis=1, level=1)
                except Exception:
                    # fallback: columns level 0 = ticker
                    sub_simple = df.xs(t, axis=1, level=0)

                if sub_simple is None or sub_simple.empty:
                    continue

                sub_simple = sub_simple.reset_index()
                sub_simple["ticker"] = t
                rows.append(sub_simple)
        else:
            # Caso de 1 ticker no batch
            df = df.reset_index()
            df["ticker"] = batch[0]
            rows.append(df)

    if not rows:
        raise RuntimeError("Não foi possível montar dataset tabular a partir dos batches do yfinance.")

    out = pd.concat(rows, ignore_index=True)
    out = out.drop_duplicates(subset=["ticker", "Date"], keep="last")

    # Normaliza nomes de colunas esperadas do yfinance
    # Date + Open High Low Close Adj Close Volume
    # (Se vier sem Adj Close, mantém)
    if "Date" not in out.columns:
        raise RuntimeError("Coluna Date não encontrada após normalização.")

    out = out.sort_values(["ticker", "Date"]).groupby("ticker", as_index=False).tail(1)

    # Partição diária baseada no date do último pregão
    out["dt"] = out["Date"].dt.date.astype(str)

    # componentes de data úteis para ETL posterior (opcional mas útil)
    out["year"] = out["Date"].dt.year
    out["month"] = out["Date"].dt.month
    out["day"] = out["Date"].dt.day

    # Ordena colunas (ajuda o Glue depois)
    preferred_cols = ["Date", "ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume", "dt", "year", "month", "day"]
    cols = [c for c in preferred_cols if c in out.columns] + [c for c in out.columns if c not in preferred_cols]
    out = out[cols]

    return out


def write_parquet_partitioned_daily(df: pd.DataFrame, s3_bucket: str, s3_prefix: str) -> str:
    """
    Salva parquet no S3 em partição diária:
      s3://bucket/prefix/dt=YYYY-MM-DD/quotes.parquet
    Se existirem múltiplos dt (anormal), salva um parquet por dt.
    """
    if "dt" not in df.columns:
        raise ValueError("df precisa ter coluna dt para particionamento diário.")

    dts = sorted(df["dt"].unique().tolist())
    if len(dts) == 1:
        dt = dts[0]
        path = f"s3://{s3_bucket}/{s3_prefix}/dt={dt}/quotes.parquet"
        df.to_parquet(path, index=False)
        return path

    # fallback multi-dt
    last_path = ""
    for dt in dts:
        part = df[df["dt"] == dt].copy()
        path = f"s3://{s3_bucket}/{s3_prefix}/dt={dt}/quotes.parquet"
        part.to_parquet(path, index=False)
        last_path = path
    return last_path


def run_with_retries(max_retries: int = 2, sleep_seconds: int = 180) -> None:
    """
    max_retries=2 => total de 3 tentativas (1 inicial + 2 retries)
    sleep_seconds=180 => 3 minutos
    """
    s3_bucket = os.environ.get("RAW_BUCKET", "capizani-techchallenge")
    s3_prefix = os.environ.get("RAW_PREFIX", "raw")

    if not s3_bucket:
        raise ValueError("Defina a variável de ambiente RAW_BUCKET (nome do bucket S3).")

    tickers = sanitize_tickers(TICKERS_100)
    if len(tickers) != 100:
        logging.warning("Lista final de tickers tem %d itens (esperado 100).", len(tickers))

    attempt = 0
    while True:
        try:
            attempt += 1
            logging.info("Attempt %d/%d", attempt, 1 + max_retries)

            df = download_daily_batched(tickers, lookback_days="5d")
            if df.empty:
                raise RuntimeError("Dataset final vazio após download e normalização.")

            out_path = write_parquet_partitioned_daily(df, s3_bucket, s3_prefix)

            logging.info("SUCCESS: gravado RAW parquet em %s", out_path)
            logging.info("Rows=%d | Unique tickers=%d | dt=%s",
                         len(df), df["ticker"].nunique(), ",".join(sorted(df["dt"].unique())))
            return

        except Exception as e:
            logging.exception("Erro na tentativa %d: %s", attempt, str(e))

            if attempt > max_retries + 1:
                # segurança extra (não deve acontecer)
                raise

            if attempt == max_retries + 1:
                logging.error("Falha após %d tentativas totais. Encerrando com erro.", attempt)
                raise

            logging.info("Aguardando %d segundos (3 min) antes do retry...", sleep_seconds)
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    run_with_retries(max_retries=2, sleep_seconds=180)
