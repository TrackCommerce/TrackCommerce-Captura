# TrackCommerce-Captura — Deploy do ETL (`script-leitura.py`) na AWS Lambda

Este guia descreve como implantar o `script-leitura.py` como uma função **AWS Lambda (Python 3.12)** acionada automaticamente **uma única vez por ciclo** sempre que o **Bucket S3 Bronze** receber um arquivo `*_sistema.csv` (**Opção A**).

---

## Visão Geral do Fluxo

1. O coletor (`script-escrita.py`) particiona os CSVs por dia (`YYYY-MM-DD`) e envia ao **Bucket Bronze** em `<id_empresa>/<identificador_canonico>/<YYYY-MM-DD>/`, subindo os arquivos auxiliares **primeiro** (`_particoes.csv`, `_processos.csv`, `_conexoes_rede.csv`) e **`_sistema.csv` por último**.
2. O gatilho S3 (`s3:ObjectCreated:*`, **Suffix: `_sistema.csv`**) dispara a função Lambda (`script-leitura.lambda_handler`) **apenas 1 vez por ciclo**, eliminando as 4 invocações e a duplicidade de execução.
3. A função baixa `_sistema.csv` + os auxiliares correspondentes (`_processos.csv` e `_particoes.csv`) em `/tmp/track-commerce`, valida a empresa/instância na API Express via HTTP (**Via 2**: `GET {API_BASE_URL}/agente/resolver-instancia/<identificador>`), processa as métricas com `pandas` e grava:
   - **Bucket Silver:**
     - `<id_empresa>/<identificador>/<YYYY-MM-DD>/<identificador>_sistema.csv`
   - **Bucket Gold (estruturado e leve para as dashboards do Analista de Infraestrutura/TI):**
     - `<id_empresa>/<identificador>/atual.json` *(snapshot instantâneo + Top 5 processos CPU/RAM + partições)*
     - `<id_empresa>/<identificador>/dashboard.json` *(snapshot + janelas agregadas de 15m das últimas 24h de CPU, RAM, Disco e Rede + resumos)*
     - `<id_empresa>/<identificador>/historico/<YYYY-MM-DD>.json` *(histórico detalhado restrito ao dia)*

> A escrita concorrente de `indice_maquinas.json` global foi removida da Lambda para eliminar *race conditions* entre múltiplas instâncias/empresas. A fonte de verdade da lista de instâncias é o MySQL (`instancia`), consultado pelo back-end Express.

---

## Pré-requisitos

- **3 Buckets S3 distintos** criados na mesma região (ex.: `us-east-1`):
  - Bucket **Bronze** (entrada de CSVs brutos)
  - Bucket **Silver** (saída de CSVs tratados)
  - Bucket **Gold** (saída de JSONs leves e histórico diário)
- Conta AWS com permissão para criar/editar funções Lambda, Layers, Roles IAM e Triggers S3.

> **Importante:** nunca configure o mesmo bucket como Bronze e Silver/Gold, pois isso causaria loop infinito de execuções. O código também bloqueia essa configuração.

---

## Passo 1 — Empacotar o código (`.zip`)

Com a **Via 2** (consulta à API Express via `urllib.request` nativo do Python), a Lambda **não precisa** de `mysql-connector-python`, `python-dotenv` nem configuração de VPC. Basta empacotar `script-leitura.py` e `eventos.json`:

```bash
zip lambda-etl-leitura.zip script-leitura.py eventos.json
```

---

## Passo 2 — Criar a função AWS Lambda

1. No Console da AWS, acesse **Lambda → Functions → Create function**.
2. Selecione **Author from scratch**:
   - **Function name:** `trackcommerce-etl-leitura` (ou nome de sua preferência)
   - **Runtime:** `Python 3.12`
   - **Architecture:** `x86_64`
3. Em **Permissions**, escolha criar uma nova Role com permissões básicas do Lambda (ou selecione `LabRole` no AWS Academy).
4. Clique em **Create function**.

---

## Passo 3 — Enviar o código e configurar o Handler

1. Na aba **Code**, clique em **Upload from → .zip file** e envie o `lambda-etl-leitura.zip`.
2. Na seção **Runtime settings**, clique em **Edit** e ajuste:
   - **Handler:** `script-leitura.lambda_handler`
3. Clique em **Save**.

---

## Passo 4 — Adicionar a Layer do `pandas`

O `boto3` já vem nativo no runtime Python 3.12 do Lambda. Para fornecer o `pandas`, adicione a Layer oficial gerenciada pela AWS:

1. Na aba **Code**, role até **Layers** e clique em **Add a layer**.
2. Selecione **AWS layers**.
3. Escolha **`AWSSDKPandas-Python312`** e selecione a versão mais recente disponível.
4. Clique em **Add**.

---

## Passo 5 — Configurar Variáveis de Ambiente

Acesse **Configuration → Environment variables → Edit** e adicione:

| Chave | Obrigatória | Descrição / Exemplo |
| --- | :---: | --- |
| `AWS_BUCKET_SILVER` | Sim | Nome do Bucket S3 Silver (ex.: `trackcommerce-silver`) |
| `AWS_BUCKET_GOLD` | Sim | Nome do Bucket S3 Gold (ex.: `trackcommerce-gold`) |
| `API_BASE_URL` | Recomendada | URL da API Express para separação autoritativa via BD (**Via 2**, ex.: `https://api.seu-dominio.com`) |
| `API_INTERNAL_KEY` | Recomendada | Chave secreta compartilhada com a API Express (mesmo valor de `API_INTERNAL_KEY` no `.env` do Node) |
| `AWS_UPLOAD` | Não | Padrão `True` no Lambda (mantém envio ao S3 ativo) |
| `API_TIMEOUT_S` | Não | Timeout em segundos da consulta HTTP à API (padrão `5`) |
| `API_CACHE_TTL_S` | Não | Cache em memória da instância resolvida em containers warm (padrão `300`) |

> **Não** é necessário definir `AWS_ACCESS_KEY_ID` ou `AWS_SECRET_ACCESS_KEY` no Lambda: o código utiliza automaticamente as credenciais da **IAM Execution Role**.

---

## Passo 6 — Configurar Permissões IAM da Execution Role

Garanta que a **Execution role** possua permissão de logs no CloudWatch e acesso aos 3 buckets S3:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LerBucketBronze",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::SEU-BUCKET-BRONZE",
        "arn:aws:s3:::SEU-BUCKET-BRONZE/*"
      ]
    },
    {
      "Sid": "EscreverBucketSilver",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::SEU-BUCKET-SILVER/*"
    },
    {
      "Sid": "EscreverBucketGold",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::SEU-BUCKET-GOLD/*"
    }
  ]
}
```

---

## Passo 7 — Ajustar Memória e Timeout

Acesse **Configuration → General configuration → Edit**:

- **Memory:** `512 MB` (ou `1024 MB` para instâncias com alto volume diário)
- **Timeout:** `1 min 0 sec` (60 segundos)
- **Ephemeral storage (`/tmp`):** `512 MB` (o código limpa `/tmp/track-commerce` a cada execução)

---

## Passo 8 — Configurar o Gatilho (Trigger) do Bucket Bronze (Opção A)

1. Na tela da função Lambda, clique em **Add trigger**.
2. Selecione **S3**.
3. Configure:
   - **Bucket:** selecione o seu **Bucket Bronze**
   - **Event types:** `All object create events` (`s3:ObjectCreated:*`)
   - **Prefix:** *(deixe vazio para monitorar todas as empresas e instâncias)*
   - **Suffix:** **`_sistema.csv`** *(essencial para disparar apenas 1 vez por ciclo e evitar duplicidade!)*
4. Marque a caixa de confirmação sobre invocação recursiva (garantindo que os buckets de saída Silver e Gold são diferentes do Bronze).
5. Clique em **Add**.

---

## Passo 9 — Testar e Monitorar

1. Envie um lote de teste ao **Bucket Bronze** em `<id_empresa>/<identificador>/<YYYY-MM-DD>/` subindo `_particoes.csv`, `_processos.csv` e por último `_sistema.csv`.
2. No CloudWatch Logs da Lambda, confirme que ocorreu **apenas 1 execução** com `[S3 DOWNLOAD]`, `[S3 UPLOAD]` e `[LAMBDA OK]`.
3. Verifique:
   - **Bucket Silver:** `<id_empresa>/<identificador>/<YYYY-MM-DD>/<identificador>_sistema.csv`
   - **Bucket Gold:** `<id_empresa>/<identificador>/atual.json`, `dashboard.json` e `historico/<YYYY-MM-DD>.json`.
