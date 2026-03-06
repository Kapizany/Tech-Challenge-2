import sys
import re
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# -----------------------------
# Args (obrigatórios)
# -----------------------------
args = getResolvedOptions(sys.argv, ["S3_ROOT", "INPUT_S3_URI", "LOOKBACK_DAYS"])

S3_ROOT = args["S3_ROOT"].rstrip("/")
INPUT_S3_URI = args["INPUT_S3_URI"]
LOOKBACK_DAYS = 7

RAW_S3_ROOT = f"{S3_ROOT}/raw/"
REFINED_S3_PATH = f"{S3_ROOT}/refined/"

# -----------------------------
# Spark/Glue setup
# -----------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# -----------------------------
# Logging helpers
# -----------------------------
def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat(timespec="seconds")
    print(f"[{ts}Z] {msg}")

def fail(msg: str) -> None:
    log(f"ERROR: {msg}")
    raise RuntimeError(msg)

def assert_true(cond: bool, msg: str) -> None:
    if not cond:
        fail(msg)

def show_df_stats(df, name: str, sample_rows: int = 5) -> None:
    try:
        log(f"{name}: columns={len(df.columns)}")
        log(f"{name}: schema=\n{df._jdf.schema().treeString()}")
        log(f"{name}: count={df.count()}")
        df.show(sample_rows, truncate=False)
    except Exception as e:
        log(f"WARN: failed to show stats for {name}: {e}")


# -----------------------------
# Validation: inputs
# -----------------------------
log("Starting Glue job")
log(f"S3_ROOT={S3_ROOT}")
log(f"RAW_S3_ROOT={RAW_S3_ROOT}")
log(f"REFINED_S3_PATH={REFINED_S3_PATH}")
log(f"INPUT_S3_URI={INPUT_S3_URI}")
log(f"LOOKBACK_DAYS={LOOKBACK_DAYS}")

assert_true(LOOKBACK_DAYS >= 7, "LOOKBACK_DAYS must be >= 7 to compute MA7 and lag reliably.")

# Heuristic validation: enforce that input points inside RAW root (avoid accidental wrong bucket/prefix)
assert_true(
    INPUT_S3_URI.startswith(RAW_S3_ROOT),
    f"INPUT_S3_URI must be under RAW_S3_ROOT. Expected prefix {RAW_S3_ROOT}"
)


# -----------------------------
# Helpers: snake_case + date normalization
# -----------------------------
def to_snake_case(name: str) -> str:
    name = re.sub(r"[\s\.\-]+", "_", name.strip())
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"__+", "_", name)
    return name.lower()

def snake_case_columns(df):
    mapping = {c: to_snake_case(c) for c in df.columns}
    # Avoid collisions (rare, but can happen). If collision, fail fast.
    inv = {}
    for src, dst in mapping.items():
        inv.setdefault(dst, []).append(src)
    collisions = {dst: srcs for dst, srcs in inv.items() if len(srcs) > 1}
    assert_true(len(collisions) == 0, f"snake_case collision detected: {collisions}")
    for src, dst in mapping.items():
        if src != dst:
            df = df.withColumnRenamed(src, dst)
    return df

def ensure_dt_columns(df):
    """
    Ensures:
      - trade_date (date)
      - dt (yyyy-MM-dd string)
      - year/month/day (ints)
    Accepts original date column as 'date' or 'trade_date'.
    """
    if "trade_date" in df.columns:
        df = df.withColumn("trade_date", F.to_date(F.col("trade_date")))
    elif "date" in df.columns:
        df = df.withColumn("trade_date", F.to_date(F.col("date")))
    else:
        fail("Missing date column. Expected 'date' or 'trade_date' (after snake_case).")

    # dt: prefer existing dt if present; else derive from trade_date
    if "dt" in df.columns:
        df = df.withColumn("dt", F.coalesce(F.col("dt"), F.date_format(F.col("trade_date"), "yyyy-MM-dd")))
    else:
        df = df.withColumn("dt", F.date_format(F.col("trade_date"), "yyyy-MM-dd"))

    df = (
        df.withColumn("year", F.year("trade_date"))
          .withColumn("month", F.month("trade_date"))
          .withColumn("day", F.dayofmonth("trade_date"))
    )
    return df


# -----------------------------
# 1) Read the daily input file
# -----------------------------
log("Reading INPUT_S3_URI parquet...")
df_in = spark.read.parquet(INPUT_S3_URI)
df_in = snake_case_columns(df_in)
df_in = ensure_dt_columns(df_in)

# Required columns
required_cols = {"ticker", "trade_date", "dt"}
missing = required_cols - set(df_in.columns)
assert_true(len(missing) == 0, f"Missing required columns in input: {sorted(missing)}")

# Rename 2 columns per requirement (if present)
rename_map = {
    "open": "opening_price",
    "close": "closing_price",
}
for src, dst in rename_map.items():
    if src in df_in.columns:
        df_in = df_in.withColumnRenamed(src, dst)

# Numeric casting
for c in ["opening_price", "high", "low", "closing_price", "volume"]:
    if c in df_in.columns:
        df_in = df_in.withColumn(c, F.col(c).cast("double"))

# Determine processing date based on input max trade_date (should be 1 day)
max_trade_date = df_in.agg(F.max("trade_date").alias("maxd")).collect()[0]["maxd"]
assert_true(max_trade_date is not None, "Could not determine max trade_date from input.")
processing_dt = df_in.agg(F.max("dt").alias("maxdt")).collect()[0]["maxdt"]
assert_true(processing_dt is not None, "Could not determine processing dt from input.")

log(f"Processing date (dt) = {processing_dt} | trade_date_max = {max_trade_date}")

# Materialize tickers list from input (small)
tickers = [r["ticker"] for r in df_in.select("ticker").distinct().collect()]
assert_true(len(tickers) > 0, "No tickers found in INPUT_S3_URI.")
log(f"Tickers in input: {len(tickers)}")


# -----------------------------
# 2) Read RAW history (lookback window) for those tickers
# -----------------------------
log("Reading RAW history parquet (S3_ROOT/raw/) for window calculations...")
df_hist = spark.read.parquet(RAW_S3_ROOT)
df_hist = snake_case_columns(df_hist)
df_hist = ensure_dt_columns(df_hist)

# Apply same renames/casts to history
for src, dst in rename_map.items():
    if src in df_hist.columns:
        df_hist = df_hist.withColumnRenamed(src, dst)

for c in ["opening_price", "high", "low", "closing_price", "volume"]:
    if c in df_hist.columns:
        df_hist = df_hist.withColumn(c, F.col(c).cast("double"))

assert_true("ticker" in df_hist.columns, "RAW history is missing column 'ticker'.")

# Filter to input tickers + lookback window
df_work = df_hist.where(F.col("ticker").isin(tickers))

df_work = df_work.where(
    (F.col("trade_date") <= F.lit(max_trade_date)) &
    (F.col("trade_date") >= F.date_sub(F.lit(max_trade_date), LOOKBACK_DAYS))
)

# Basic sanity checks
work_count = df_work.count()
assert_true(work_count > 0, "RAW history lookback produced 0 rows. Check RAW_S3_ROOT or dt partitions.")
log(f"RAW workset rows in lookback window: {work_count}")

# Optional: ensure required price column exists for calculations
assert_true("closing_price" in df_work.columns, "RAW history must contain 'closing_price' (or 'close' in raw).")

# -----------------------------
# 3) Requirement C: date-based calculations
# -----------------------------
log("Computing lag + MA7 (Requirement C)...")
w = Window.partitionBy("ticker").orderBy("trade_date")

df_work = df_work.withColumn("prev_close", F.lag("closing_price").over(w))

df_work = df_work.withColumn(
    "daily_return_pct",
    F.when(F.col("prev_close").isNull(), F.lit(None).cast("double"))
     .otherwise((F.col("closing_price") / F.col("prev_close") - F.lit(1.0)) * F.lit(100.0))
)

w7 = w.rowsBetween(-6, 0)
df_work = df_work.withColumn("ma7_close", F.avg("closing_price").over(w7))

# Keep only processing day rows (so we don't rewrite many partitions)
df_quotes = df_work.where(F.col("dt") == F.lit(processing_dt))

quotes_count = df_quotes.count()
assert_true(quotes_count > 0, f"No rows found for processing dt={processing_dt} in lookback dataset.")
log(f"Quotes rows for dt={processing_dt}: {quotes_count}")

# -----------------------------
# 4) Requirement A: Aggregation (daily_agg)
# -----------------------------
log("Computing daily aggregation (Requirement A)...")
df_agg = (
    df_quotes.groupBy("dt", "year", "month", "day", "ticker")
      .agg(
          F.avg("closing_price").alias("avg_close"),
          F.sum(F.col("volume")).alias("sum_volume"),
          F.max("closing_price").alias("max_close"),
          F.min("closing_price").alias("min_close"),
          F.count(F.lit(1)).alias("row_count"),
      )
)

# -----------------------------
# 5) Output: single refined root, differentiated by dataset
# -----------------------------
log("Preparing unified refined output...")

# Quotes output columns
df_quotes_out = (
    df_quotes.select(
        "trade_date", "dt", "year", "month", "day", "ticker",
        # keep prices if present
        *[c for c in ["opening_price", "high", "low", "closing_price", "volume"] if c in df_quotes.columns],
        "prev_close", "daily_return_pct", "ma7_close",
    )
    .withColumn("dataset", F.lit("quotes"))
)

# Ensure all expected columns exist (for union)
base_cols = ["opening_price", "high", "low", "closing_price", "volume"]
for c in base_cols:
    if c not in df_quotes_out.columns:
        df_quotes_out = df_quotes_out.withColumn(c, F.lit(None).cast("double"))

# Agg output
df_agg_out = (
    df_agg.select(
        "dt", "year", "month", "day", "ticker",
        "avg_close", "sum_volume", "max_close", "min_close", "row_count",
    )
    .withColumn("trade_date", F.to_date(F.col("dt")))
    .withColumn("dataset", F.lit("daily_agg"))
)

# Fill non-agg columns for union
for c in base_cols + ["prev_close", "daily_return_pct", "ma7_close"]:
    df_agg_out = df_agg_out.withColumn(c, F.lit(None).cast("double"))

# Fill agg columns for quotes side
for c, t in [
    ("avg_close", "double"),
    ("sum_volume", "double"),
    ("max_close", "double"),
    ("min_close", "double"),
    ("row_count", "long"),
]:
    if c not in df_quotes_out.columns:
        df_quotes_out = df_quotes_out.withColumn(c, F.lit(None).cast(t))
    else:
        df_quotes_out = df_quotes_out.withColumn(c, F.col(c).cast(t))

# Final unified dataframe
final_cols = [
    "dataset",
    "trade_date", "dt", "year", "month", "day",
    "ticker",
    "opening_price", "high", "low", "closing_price", "volume",
    "prev_close", "daily_return_pct", "ma7_close",
    "avg_close", "sum_volume", "max_close", "min_close", "row_count",
]

df_refined = df_quotes_out.select(final_cols).unionByName(df_agg_out.select(final_cols))

log("Refined output preview:")
df_refined.show(10, truncate=False)

# -----------------------------
# 6) Write refined partitions (dt -> ticker -> dataset)
# -----------------------------
log("Writing refined parquet partitions...")
(
    df_refined.write
      .mode("overwrite")
      .partitionBy("dt", "ticker", "dataset")
      .parquet(REFINED_S3_PATH)
)

log(f"SUCCESS: wrote refined to {REFINED_S3_PATH} partitioned by dt/ticker/dataset")
log(f"SUCCESS: processed INPUT_S3_URI={INPUT_S3_URI}")