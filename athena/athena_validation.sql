USE default;

-- Teste rapido de acesso a tabela catalogada
SELECT *
FROM b3_refined
LIMIT 20;

-- Cotacoes refinadas de uma data especifica
SELECT
  trade_date,
  ticker,
  opening_price,
  high,
  low,
  closing_price,
  volume,
  prev_close,
  daily_return_pct,
  ma7_close
FROM b3_refined
WHERE dt = '2026-03-11'
  AND dataset = 'quotes'
ORDER BY ticker;

-- Agregacoes diarias geradas pelo ETL
SELECT
  dt,
  ticker,
  avg_close,
  sum_volume,
  max_close,
  min_close,
  row_count
FROM b3_refined
WHERE dt = '2026-03-11'
  AND dataset = 'daily_agg'
ORDER BY ticker;

-- Top 10 maiores variacoes diarias
SELECT
  dt,
  ticker,
  closing_price,
  prev_close,
  daily_return_pct
FROM b3_refined
WHERE dt = '2026-03-11'
  AND dataset = 'quotes'
  AND daily_return_pct IS NOT NULL
ORDER BY daily_return_pct DESC
LIMIT 10;
