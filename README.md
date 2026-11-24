# TrackCommerce-Captura — Deploy do ETL (`script-leitura.py`) na AWS Lambda

Este guia descreve como implantar o `script-leitura.py` como uma função **AWS Lambda (Python 3.12)** acionada automaticamente sempre que o **Bucket S3 Bronze** receber um novo arquivo `.csv`.

---

## Visão Geral do Fluxo

1. Um novo arquivo `.csv` é enviado ao **Bucket Bronze** (`<empresa>/<identificador>_sistema.csv` ou `<empresa>/<identificador>_processos.csv`).
2. O gatilho S3 (`s3:ObjectCreated:*`) dispara a função Lambda (`script-leitura.lambda_handler`).
3. A função baixa o arquivo no diretório temporário `/tmp/track-commerce`, processa as métricas com `pandas` e grava:
   - **Bucket Silver:** `<empresa>/<identificador>_sistema.csv`
   - **Bucket Gold:**
     - `<empresa>/<identificador>_sistema.json`
     - `indice_maquinas_empresa_<empresa>.json` *(atualizado de forma incremental)*
     - `indice_maquinas.json` *(atualizado de forma incremental)*

> Arquivos auxiliares `*_particoes.csv` e `*_conexoes_rede.csv` são ignorados automaticamente.

---

## Pré-requisitos

- **3 Buckets S3 distintos** criados na mesma região (ex.: `us-east-1`):
  - Bucket **Bronze** (entrada de CSVs brutos)
  - Bucket **Silver** (saída de CSVs tratados)
  - Bucket **Gold** (saída de JSONs consolidados e índices)
- Conta AWS com permissão para criar/editar funções Lambda, Layers, Roles IAM e Triggers S3.

> **Importante:** nunca configure o mesmo bucket como Bronze e Silver/Gold, pois isso causaria loop infinito de execuções. O código também bloqueia essa configuração.

---

## Passo 1 — Empacotar o código (`.zip`)

Na pasta `TrackCommerce-Captura`, gere um arquivo `.zip` contendo o `script-leitura.py` e o `eventos.json` (usado no cálculo de próximo pico):

```bash
zip lambda-etl-leitura.zip script-leitura.py eventos.json
```

### (Opcional) Incluir suporte a banco de dados MySQL

Por padrão, o Lambda usa os metadados de empresa presentes nos arquivos Bronze e **não precisa** do `mysql-connector-python` nem do `python-dotenv`. Caso queira consultar o MySQL a partir do Lambda, instale o conector na pasta do pacote antes de gerar o `.zip`:

```bash
mkdir -p pacote_lambda
pip install mysql-connector-python -t pacote_lambda --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12
cp script-leitura.py eventos.json pacote_lambda/
cd pacote_lambda && zip -r ../lambda-etl-leitura.zip .
```

---

## Passo 2 — Criar a função AWS Lambda

1. No Console da AWS, acesse **Lambda → Functions → Create function**.
2. Selecione **Author from scratch**:
   - **Function name:** `trackcommerce-etl-leitura` (ou nome de sua preferência)
   - **Runtime:** `Python 3.12`
   - **Architecture:** `x86_64`
3. Em **Permissions**, escolha criar uma nova Role com permissões básicas do Lambda (ou selecione a Role do laboratório, como `LabRole` no AWS Academy, caso utilize ambiente acadêmico).
4. Clique em **Create function**.

---

## Passo 3 — Enviar o código e configurar o Handler

1. Na aba **Code**, clique em **Upload from → .zip file** e envie o `lambda-etl-leitura.zip`.
2. Na seção **Runtime settings**, clique em **Edit** e ajuste:
   - **Handler:** `script-leitura.lambda_handler`
3. Clique em **Save**.

> Caso prefira colar o código diretamente no editor padrão `lambda_function.py` do console, mantenha o Handler como `lambda_function.lambda_handler` e garanta que `eventos.json` esteja na raiz da função.

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
| `AWS_UPLOAD` | Não | Padrão `True` no Lambda (mantém envio ao S3 ativo) |
| `DB_HOST` | Não | Host do MySQL (se omitido, usa metadados do Bronze) |
| `DB_PORT` | Não | Porta do MySQL (padrão `3306`) |
| `DB_USER` | Não | Usuário do MySQL |
| `DB_PASS` | Não | Senha do MySQL (também aceita `DB_PASSWORD`) |
| `DB_NAME` | Não | Nome do banco MySQL (também aceita `DB_DATABASE`) |

> **Não** é necessário definir `AWS_ACCESS_KEY_ID` ou `AWS_SECRET_ACCESS_KEY` no Lambda: o código utiliza automaticamente as credenciais da **IAM Execution Role**.

---

## Passo 6 — Configurar Permissões IAM da Execution Role

Acesse **Configuration → Permissions**, clique no nome da **Execution role** e garanta que ela possua permissão de logs no CloudWatch e acesso aos 3 buckets S3 (substitua pelos nomes reais dos seus buckets):

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
      "Sid": "LerEscreverBucketGold",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::SEU-BUCKET-GOLD",
        "arn:aws:s3:::SEU-BUCKET-GOLD/*"
      ]
    }
  ]
}
```

> `s3:GetObject` no Bucket Gold é necessário para a atualização incremental dos índices (`indice_maquinas_empresa_<empresa>.json` e `indice_maquinas.json`).

---

## Passo 7 — Ajustar Memória e Timeout

Acesse **Configuration → General configuration → Edit**:

- **Memory:** `512 MB` (ou `1024 MB` se os CSVs Bronze tiverem grande volume de linhas)
- **Timeout:** `1 min 0 sec` (60 segundos)
- **Ephemeral storage (`/tmp`):** `512 MB` (padrão, suficiente pois o código limpa `/tmp/track-commerce` a cada execução)

---

## Passo 8 — Configurar o Gatilho (Trigger) do Bucket Bronze

1. Na tela da função Lambda, clique em **Add trigger**.
2. Selecione **S3**.
3. Configure:
   - **Bucket:** selecione o seu **Bucket Bronze**
   - **Event types:** `All object create events` (`s3:ObjectCreated:*`)
   - **Prefix:** *(deixe vazio para monitorar todas as pastas de empresas)*
   - **Suffix:** `.csv`
4. Marque a caixa de confirmação sobre invocação recursiva (garantindo que os buckets de saída Silver e Gold são diferentes do Bronze).
5. Clique em **Add**.

---

## Passo 9 — Testar e Monitorar

1. Faça upload manual de um arquivo `<empresa>/<identificador>_sistema.csv` no **Bucket Bronze**.
2. Na função Lambda, acesse **Monitor → View CloudWatch logs** para acompanhar a execução (`[S3 DOWNLOAD]`, `[S3 UPLOAD]`, `[LAMBDA OK]`).
3. Verifique no **Bucket Silver** o arquivo `<empresa>/<identificador>_sistema.csv` e no **Bucket Gold** os arquivos `<empresa>/<identificador>_sistema.json`, `indice_maquinas_empresa_<empresa>.json` e `indice_maquinas.json`.
