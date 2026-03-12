# Scraping

Este modulo concentra os scripts responsaveis por extrair cotacoes da B3 via `yfinance`, gravar a camada `raw` no S3 e executar operacoes de manutencao sobre os arquivos gerados.

## Arquivos

### `extract_b3_to_s3.py`

Script principal de ingestao diaria.

- Baixa cotacoes de 30 ativos da B3 definidos em `TICKERS_30`.
- Consulta uma janela curta de `5d` para encontrar o ultimo pregao disponivel por ticker.
- Normaliza o dataset e grava em `s3://<bucket>/<prefix>/dt=YYYY-MM-DD/quotes.parquet`.
- Faz retry automatico em caso de falha.

Variaveis de ambiente:

- `RAW_BUCKET`: bucket de destino. Default: `capizani-techchallenge`.
- `RAW_PREFIX`: prefixo base no bucket. Default: `raw`.

Execucao:

```bash
python3 scraping/extract_b3_to_s3.py
```

Comportamento esperado:

- Em dias uteis, grava a particao do ultimo pregao encontrado.
- Em fins de semana ou feriados, continua funcionando porque consulta os ultimos dias disponiveis.
- O arquivo gerado segue o padrao consumido pela Lambda e pelo Glue:
  `raw/dt=YYYY-MM-DD/quotes.parquet`

### `backfill_extract_b3_to_s3_week.py`

Script de reprocessamento por intervalo de datas.

- Reutiliza a mesma lista de tickers e a mesma rotina de escrita do script principal.
- Processa o intervalo dia a dia.
- Faz um upload separado por data, gerando um `quotes.parquet` por dia.
- Faz retry por dia durante a escrita no S3.
- Registra no log quais datas foram processadas e quais ficaram sem dados.

Argumentos:

- `--start-date`: data inicial no formato `YYYY-MM-DD`.
- `--end-date`: data final no formato `YYYY-MM-DD`.
- `--batch-size`: quantidade de tickers por batch no `yfinance`. Default: `6`.
- `--max-retries`: retries por dia na escrita. Default: `2`.
- `--sleep-seconds`: espera entre retries. Default: `180`.

Defaults:

- `--start-date`: segunda-feira da semana atual.
- `--end-date`: data de hoje.

Execucoes comuns:

```bash
python3 scraping/backfill_extract_b3_to_s3_week.py
```

```bash
python3 scraping/backfill_extract_b3_to_s3_week.py \
  --start-date 2026-03-09 \
  --end-date 2026-03-12
```

Quando usar:

- Falha de ingestao em dias anteriores.
- Reprocessamento de uma semana especifica.
- Repovoamento de particoes `raw/dt=...` ausentes.

Observacao:

- O processamento dia a dia favorece um evento S3 por particao criada, o que facilita o disparo da Lambda para cada data reprocessada.

### `fix_raw_parquet_timestamps.py`

Script de manutencao para corrigir arquivos Parquet no S3 com colunas `timestamp[ns]`, convertendo para `timestamp[us]`.

Motivacao:

- Glue e Spark podem falhar ao ler timestamps em nanos.
- Os arquivos alvo seguem o padrao `raw/dt=YYYY-MM-DD/quotes.parquet`.

Argumentos principais:

- `--bucket`: bucket S3. Obrigatorio.
- `--prefix`: prefixo base. Default: `raw`.
- `--dt-from`: data inicial para filtrar particoes.
- `--dt-to`: data final para filtrar particoes.
- `--max-files`: limita quantos arquivos processar. `0` significa sem limite.
- `--apply`: aplica a alteracao. Sem esta flag, roda em dry-run.
- `--backup-prefix`: se informado, cria backup antes de sobrescrever.
- `--profile`: AWS profile opcional.
- `--region`: AWS region opcional.

Dry-run:

```bash
python3 scraping/fix_raw_parquet_timestamps.py \
  --bucket capizani-techchallenge \
  --dt-from 2026-03-01 \
  --dt-to 2026-03-12
```

Aplicando a correcao com backup:

```bash
python3 scraping/fix_raw_parquet_timestamps.py \
  --bucket capizani-techchallenge \
  --dt-from 2026-03-01 \
  --dt-to 2026-03-12 \
  --backup-prefix backup/raw-before-ts-fix \
  --apply
```

## Dependencias

As dependencias Python estao em `requirements.txt`. As principais para os scripts deste modulo sao:

- `pandas`
- `pyarrow`
- `s3fs`
- `boto3`
- `yfinance`
- `curl_cffi`

Instalacao local:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scraping/requirements.txt
```

## Docker

O `Dockerfile` atual foi pensado para a ingestao diaria e empacota apenas `extract_b3_to_s3.py`.

Build:

```bash
docker build -t techchallenge-scraping ./scraping
```

Run:

```bash
docker run --rm \
  -e RAW_BUCKET=capizani-techchallenge \
  -e RAW_PREFIX=raw \
  techchallenge-scraping
```

Limitacao atual:

- A imagem nao copia `backfill_extract_b3_to_s3_week.py`.
- A imagem nao copia `fix_raw_parquet_timestamps.py`.

Se voce quiser rodar backfill ou manutencao dentro do container, ajuste o `Dockerfile` para copiar esses arquivos tambem.

## Fluxo do modulo

1. `extract_b3_to_s3.py` grava `raw/dt=YYYY-MM-DD/quotes.parquet`.
2. O evento do S3 pode acionar a Lambda em `orchestration/`.
3. A Lambda inicia o Glue Job para processamento da camada seguinte.
4. `fix_raw_parquet_timestamps.py` corrige particoes antigas quando necessario.
5. `backfill_extract_b3_to_s3_week.py` recompõe datas faltantes.
