# S3 -> Lambda -> Glue

Guia operacional do fluxo que observa novos arquivos em `raw/`, dispara a Lambda e inicia o Glue Job com os argumentos esperados pelo ETL.

## Fluxo

1. O scraping grava arquivos em `s3://<bucket>/raw/dt=YYYY-MM-DD/quotes.parquet`.
2. O bucket S3 publica um evento `ObjectCreated`.
3. A Lambda `s3_to_glue_lambda.py` filtra apenas chaves no padrao `raw/dt=YYYY-MM-DD/quotes.parquet`.
4. A Lambda chama `glue:StartJobRun`.
5. O Glue Job processa o arquivo informado em `--INPUT_S3_URI`.

## Variaveis usadas nos comandos

Preencha estas variaveis no shell antes de executar os passos:

```bash
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=<seu-account-id>
export BUCKET=capizani-techchallenge
export GLUE_JOB_NAME="Refine B3 data"
export LAMBDA_NAME=s3-raw-to-glue-trigger
export LAMBDA_ROLE_NAME=lambda-s3-glue-role
export CATALOG_DATABASE=default
export CATALOG_TABLE=b3_refined
```

Observacao:

- O repositorio nao mantem mais `account id` fixo em arquivos de documentacao.
- A policy IAM em `orchestration/policy-lambda-start-glue.json` usa placeholders e deve ser renderizada antes do `put-role-policy`.

## 1) Criar a role da Lambda

Crie um arquivo `trust-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Crie a role:

```bash
aws iam create-role \
  --role-name "$LAMBDA_ROLE_NAME" \
  --assume-role-policy-document file://trust-policy.json
```

Renderize a policy inline com seus valores:

```bash
sed \
  -e "s/__AWS_REGION__/$AWS_REGION/g" \
  -e "s/__AWS_ACCOUNT_ID__/$AWS_ACCOUNT_ID/g" \
  -e "s/__GLUE_JOB_NAME__/$GLUE_JOB_NAME/g" \
  orchestration/policy-lambda-start-glue.json > /tmp/policy-lambda-start-glue.rendered.json
```

Anexe a policy:

```bash
aws iam put-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-name lambda-start-glue-inline \
  --policy-document file:///tmp/policy-lambda-start-glue.rendered.json
```

Permissoes adicionais normalmente necessarias:

```bash
aws iam attach-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

Se a Lambda for executada dentro de VPC, anexe tambem:

```bash
aws iam attach-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
```

Se a funcao nao precisa acessar recursos privados, mantenha a Lambda fora de VPC. Isso evita a necessidade de permissoes para ENI.

## 2) Publicar o codigo da Lambda

Empacote o handler:

```bash
cd orchestration
zip -j /tmp/s3_to_glue_lambda.zip s3_to_glue_lambda.py
cd -
```

Crie a funcao:

```bash
aws lambda create-function \
  --function-name "$LAMBDA_NAME" \
  --runtime python3.12 \
  --handler s3_to_glue_lambda.lambda_handler \
  --role "arn:aws:iam::$AWS_ACCOUNT_ID:role/$LAMBDA_ROLE_NAME" \
  --zip-file fileb:///tmp/s3_to_glue_lambda.zip \
  --timeout 60 \
  --memory-size 256 \
  --environment "Variables={GLUE_JOB_NAME=$GLUE_JOB_NAME,S3_ROOT=s3://$BUCKET,LOOKBACK_DAYS=7,CATALOG_DATABASE=$CATALOG_DATABASE,CATALOG_TABLE=$CATALOG_TABLE}"
```

Se a funcao ja existir:

```bash
aws lambda update-function-code \
  --function-name "$LAMBDA_NAME" \
  --zip-file fileb:///tmp/s3_to_glue_lambda.zip
```

Atualize configuracao e variaveis quando necessario:

```bash
aws lambda update-function-configuration \
  --function-name "$LAMBDA_NAME" \
  --handler s3_to_glue_lambda.lambda_handler \
  --environment "Variables={GLUE_JOB_NAME=$GLUE_JOB_NAME,S3_ROOT=s3://$BUCKET,LOOKBACK_DAYS=7,CATALOG_DATABASE=$CATALOG_DATABASE,CATALOG_TABLE=$CATALOG_TABLE}"
```

## 3) Permitir que o S3 invoque a Lambda

```bash
aws lambda add-permission \
  --function-name "$LAMBDA_NAME" \
  --statement-id s3invoke-raw-quotes \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn "arn:aws:s3:::$BUCKET"
```

Se esse statement ja existir, remova-o antes de recriar:

```bash
aws lambda remove-permission \
  --function-name "$LAMBDA_NAME" \
  --statement-id s3invoke-raw-quotes
```

## 4) Configurar notificacao no bucket

Renderize o ARN da Lambda dentro do arquivo de notificacao:

```bash
LAMBDA_ARN=$(aws lambda get-function \
  --function-name "$LAMBDA_NAME" \
  --query 'Configuration.FunctionArn' \
  --output text)

sed "s|LAMBDA_ARN|$LAMBDA_ARN|g" \
  orchestration/s3-notification.json > /tmp/s3-notification.rendered.json
```

Publique a configuracao:

```bash
aws s3api put-bucket-notification-configuration \
  --bucket "$BUCKET" \
  --notification-configuration file:///tmp/s3-notification.rendered.json
```

## 5) Teste rapido

Suba um arquivo de teste no padrao monitorado:

- `s3://<bucket>/raw/dt=2026-03-08/quotes.parquet`

Verifique logs da Lambda:

```bash
aws logs tail "/aws/lambda/$LAMBDA_NAME" --since 10m --follow
```

Verifique os ultimos runs do Glue:

```bash
aws glue get-job-runs --job-name "$GLUE_JOB_NAME" --max-results 5
```

Verifique a tabela catalogada:

```bash
aws glue get-table \
  --database-name "$CATALOG_DATABASE" \
  --name "$CATALOG_TABLE"
```

## Troubleshooting

### Erro: `The provided execution role does not have permissions to call CreateNetworkInterface on EC2`

Causa:

- A Lambda esta configurada para VPC, mas a role nao tem as permissoes necessarias.

Correcao:

- Remova a configuracao de VPC se ela nao for necessaria.
- Ou anexe `AWSLambdaVPCAccessExecutionRole`.

### Erro: `Unable to import module`

Causa:

- O handler configurado nao corresponde ao arquivo enviado no ZIP.

Correcao:

- Use `s3_to_glue_lambda.lambda_handler`.
- Garanta que `s3_to_glue_lambda.py` esteja na raiz do ZIP.

### Erro: `AccessDeniedException` ao chamar `StartJobRun`

Causa:

- A role da Lambda nao tem permissao para iniciar o Glue Job correto.

Correcao:

- Re-renderize `orchestration/policy-lambda-start-glue.json` com `AWS_REGION`, `AWS_ACCOUNT_ID` e `GLUE_JOB_NAME`.
- Reaplique a policy inline na role da Lambda.

### Erro: `AccessDeniedException` ao atualizar tabela ou particao no Glue Catalog

Causa:

- A role do Glue Job nao tem permissao para criar ou atualizar objetos no Data Catalog.

Correcao:

- Adicione permissoes como `glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`, `glue:GetPartition`, `glue:BatchCreatePartition` e `glue:UpdatePartition` na role usada pelo job Glue.
- Confirme tambem permissao de escrita em `s3://<bucket>/refined/`.

### Erro: `MalformedPolicyDocument`

Causa:

- O JSON da policy renderizada esta invalido ou os placeholders nao foram substituidos.

Correcao:

- Valide o arquivo renderizado antes do `put-role-policy`:

```bash
jq . /tmp/policy-lambda-start-glue.rendered.json
```

## Observacoes importantes

- O filtro do bucket em `orchestration/s3-notification.json` captura qualquer `raw/*quotes.parquet`.
- O handler faz a validacao final e so processa o padrao `raw/dt=YYYY-MM-DD/quotes.parquet`.
- Reenvios do mesmo arquivo podem gerar mais de um evento e mais de um `JobRun`.
- O argumento `LOOKBACK_DAYS` e enviado pela Lambda e usado pelo Glue para montar a janela historica de calculo.
- O Glue Job tambem pode receber `CATALOG_DATABASE` e `CATALOG_TABLE`; por default ele usa `default.b3_refined`.
