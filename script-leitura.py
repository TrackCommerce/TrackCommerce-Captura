"""
==========================================================================================
 TRACK COMMERCE - ETL DE MÉTRICAS (BRONZE -> SILVER / GOLD) - AWS LAMBDA (PYTHON 3.12)
==========================================================================================

 Entrada : Evento S3 (Bucket Bronze, Suffix _sistema.csv) no AWS Lambda ou ./track-commerce/bronze/**/*.csv
 Saída   :
   SILVER (Análise R e Pricing - inclui empresa_id e empresa_nome em todas as linhas)
     <pasta_empresa>/<identificador>/<YYYY-MM-DD>/<identificador>_sistema.csv
   GOLD   (JSON estruturado e leve orientado ao Front-end do Analista)
     <pasta_empresa>/<identificador>/atual.json
     <pasta_empresa>/<identificador>/dashboard.json
     <pasta_empresa>/<identificador>/historico/<YYYY-MM-DD>.json

 Integração com BD (Via 2 - Recomendada na Lambda):
   Consulta HTTP à API Express (GET {API_BASE_URL}/agente/resolver-instancia/<identificador>)
   usando urllib nativo + header X-API-KEY (sem driver MySQL nem configuração de VPC).

 Usa pandas (Layer AWSSDKPandas-Python312 no Lambda) + boto3 (nativo no Lambda)
 + python-dotenv e mysql.connector (opcionais apenas para uso local).
==========================================================================================
"""



import glob
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime
from urllib.parse import quote, unquote_plus

import pandas as pd

try:
    import mysql.connector as mysql_connector
except ImportError:
    mysql_connector = None

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception

# ==========================================================================================
# CONFIGURAÇÕES E INTEGRAÇÕES
# ==========================================================================================
EM_LAMBDA = bool(os.getenv("AWS_LAMBDA_FUNCTION_NAME"))

# Resolver .env relativo ao diretório do script antes de ler variáveis de ambiente
BASE_DIR_PROJETO = os.path.dirname(os.path.abspath(__file__))
if load_dotenv is not None:
    load_dotenv(os.path.join(BASE_DIR_PROJETO, ".env"))

# No AWS Lambda, apenas /tmp permite escrita
DIR_BASE = os.getenv("TRACKCOMMERCE_DIR") or (
    "/tmp/track-commerce" if EM_LAMBDA else os.path.join(BASE_DIR_PROJETO, "track-commerce")
)
DIR_BRONZE = os.path.join(DIR_BASE, "bronze")
DIR_SILVER = os.path.join(DIR_BASE, "silver")
DIR_GOLD = os.path.join(DIR_BASE, "gold")
ARQUIVO_EVENTOS = os.path.join(BASE_DIR_PROJETO, "eventos.json")

DELIMITADOR_SAIDA = ";"
FORMATO_TIMESTAMP = "%Y-%m-%d %H:%M:%S"
JANELA_DESVIO = 60
BYTES_POR_GB = 1024 ** 3
BYTES_POR_MB = 1024 ** 2

LIMITE_RAM_ATENCAO = 70.0
LIMITE_RAM_CRITICO = 85.0

COLUNAS_OBRIGATORIAS = ["identifier", "timestamp"]

# Integração Via 2 (Lambda -> API Express -> MySQL)
API_BASE_URL = os.getenv("API_BASE_URL", "").strip().rstrip("/")
API_INTERNAL_KEY = os.getenv("API_INTERNAL_KEY", "").strip()
API_TIMEOUT_S = float(os.getenv("API_TIMEOUT_S", "5"))
API_CACHE_TTL_S = int(os.getenv("API_CACHE_TTL_S", "300"))
_cache_api_instancias = {}

# S3 Download / Upload (no Lambda, upload vem habilitado por padrão)
AWS_BAIXAR = os.getenv("AWS_BAIXAR", "False").lower() in ("true", "1", "t", "yes")
AWS_UPLOAD = os.getenv("AWS_UPLOAD", "True" if EM_LAMBDA else "False").lower() in ("true", "1", "t", "yes")
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")
AWS_BUCKET_BRONZE = os.getenv("AWS_BUCKET_BRONZE", "").strip()
AWS_BUCKET_SILVER = os.getenv("AWS_BUCKET_SILVER", "").strip()
AWS_BUCKET_GOLD = os.getenv("AWS_BUCKET_GOLD", "").strip()

METRICAS_DESVIO = [
    "cpu_percent", "load_1m", "cpu_iowait_pct",
    "ram_percent", "swap_percent",
    "disco_percent", "disco_leitura_bytes_s", "disco_escrita_bytes_s",
    "disco_iops_total", "disco_latencia_ms",
    "rede_upload_bytes_s", "rede_download_bytes_s", "rede_perda_pacotes_pct",
]

REGEX_DATA_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# AWS S3, API INTERNA (VIA 2) E BANCO DE DADOS

_cliente_s3_cache = None


def obter_cliente_s3():
    global _cliente_s3_cache
    if not boto3:
        return None
    if _cliente_s3_cache is not None:
        return _cliente_s3_cache

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        _cliente_s3_cache = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN or None,
            region_name=AWS_REGION,
        )
    else:
        # No AWS Lambda (ou EC2 com IAM Role), usa automaticamente as credenciais da Role
        _cliente_s3_cache = boto3.client("s3", region_name=AWS_REGION)
    return _cliente_s3_cache


def baixar_bronze_s3():
    """Baixa automaticamente os CSVs do S3 para a pasta local bronze preservando subpastas."""
    if not (AWS_BAIXAR and AWS_BUCKET_BRONZE):
        return

    s3 = obter_cliente_s3()
    if not s3:
        print("[AVISO ETL] Configurações AWS incompletas para download.")
        return

    print("[S3 ETL] Iniciando busca por arquivos no Bucket S3 (Bronze)...")
    try:
        os.makedirs(DIR_BRONZE, exist_ok=True)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=AWS_BUCKET_BRONZE):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key or not key.endswith(".csv"):
                    continue
                partes = [p for p in key.strip("/").split("/") if p and p not in (".", "..")]
                if not partes:
                    continue
                destino = os.path.join(DIR_BRONZE, *partes)
                os.makedirs(os.path.dirname(destino), exist_ok=True)
                try:
                    s3.download_file(AWS_BUCKET_BRONZE, key, destino)
                    print(f"  [S3 DOWNLOAD] Baixado: {key} -> {destino}")
                except Exception as err:
                    print(f"  [AVISO S3 DOWNLOAD] Falha ao baixar {key}: {err}")
    except Exception as erro:
        print(f"  [ERRO S3 DOWNLOAD] Falha ao sincronizar com S3: {erro}")


def enviar_para_s3(caminho_local, chave_s3, bucket, documento=None, propagar_erro=False):
    """Faz o upload de um arquivo local ou documento JSON para o bucket S3."""
    if not AWS_UPLOAD:
        return

    s3 = obter_cliente_s3()
    if not s3:
        if propagar_erro:
            raise RuntimeError("Cliente boto3 indisponível para envio ao S3.")
        return

    try:
        if documento is None:
            s3.upload_file(caminho_local, bucket, chave_s3)
        else:
            s3.put_object(
                Bucket=bucket,
                Key=chave_s3,
                ContentType="application/json",
                Body=json.dumps(documento, ensure_ascii=False).encode("utf-8"),
            )
        print(f"  [S3 UPLOAD] Enviado: s3://{bucket}/{chave_s3}")
    except Exception as erro:
        print(f"  [ERRO S3 UPLOAD] Falha ao enviar {chave_s3} para {bucket}: {erro}")
        if propagar_erro:
            raise


def consultar_instancia_api(identificador):
    """
    Via 2: consulta a API Express (GET /agente/resolver-instancia/<identificador>)
    para obter empresa e nome autoritativos do MySQL sem driver MySQL nem VPC na Lambda.
    """
    base_url = os.getenv("API_BASE_URL", API_BASE_URL).strip().rstrip("/")
    if not base_url or not identificador:
        return None

    chave = normalizar_identificador(identificador)
    if not chave:
        return None

    agora = time.monotonic()
    em_cache = _cache_api_instancias.get(chave)
    if em_cache and (agora - em_cache["ts"]) < API_CACHE_TTL_S:
        return em_cache["dados"]

    api_key = os.getenv("API_INTERNAL_KEY", API_INTERNAL_KEY).strip()
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-API-KEY"] = api_key

    url = f"{base_url}/agente/resolver-instancia/{quote(chave, safe='')}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=API_TIMEOUT_S) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        dados = {
            "id_instancia": payload.get("id_instancia"),
            "empresa_id": para_texto(payload.get("id_empresa")),
            "empresa_nome": para_texto(payload.get("empresa")),
            "vm_nome": para_texto(payload.get("nome")),
        }
        if dados["empresa_id"]:
            _cache_api_instancias[chave] = {"ts": agora, "dados": dados}
            return dados
    except urllib.error.HTTPError as erro:
        print(f"  [AVISO API] Falha ao resolver instância '{chave}' (HTTP {erro.code}).")
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as erro:
        print(f"  [AVISO API] API indisponível ao resolver '{chave}': {erro}")
    return None


def resolver_metadados_empresa(identificador, df_bronze, mapa_bd=None, prefixo_empresa=None):
    """
    Resolve vínculo instância -> empresa em ordem de autoridade:
      1. API Express (Via 2 -> MySQL autoritativo)
      2. Mapa direto do MySQL (quando DB_HOST configurado em uso local)
      3. Metadados do próprio CSV Bronze
      4. Prefixo da empresa no caminho S3
      5. Fallback padrão
    """
    info_api = consultar_instancia_api(identificador) or {}
    info_bd = (mapa_bd or {}).get(identificador, {})

    prefixo_valido = prefixo_empresa if (prefixo_empresa and str(prefixo_empresa).isdecimal()) else None

    empresa_id = (
        info_api.get("empresa_id")
        or info_bd.get("empresa_id")
        or ultimo_valido_bronze(df_bronze, "enterprise_identifier", "empresa_id")
        or prefixo_valido
        or "sem_empresa"
    )
    empresa_nome = (
        info_api.get("empresa_nome")
        or info_bd.get("empresa_nome")
        or ultimo_valido_bronze(df_bronze, "enterprise_reason", "empresa_nome")
        or "Empresa Desconhecida"
    )
    vm_nome = (
        info_api.get("vm_nome")
        or info_bd.get("vm_nome")
        or ultimo_valido_bronze(df_bronze, "vm_nome", "hostname")
    )
    id_instancia = info_api.get("id_instancia") or info_bd.get("id_instancia")

    return {
        "id_instancia": id_instancia,
        "empresa_id": empresa_id,
        "empresa_nome": empresa_nome,
        "vm_nome": vm_nome,
    }


def carregar_maquinas_bd():
    """Busca opcional direta no MySQL (uso local/CLI quando DB_HOST estiver configurado)."""
    if mysql_connector is None:
        return {}

    db_host = os.getenv("DB_HOST", "").strip()
    if not db_host:
        return {}

    try:
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME") or os.getenv("DB_DATABASE")
        db_port = int(os.getenv("DB_PORT", "3306"))
        conexao = mysql_connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            database=db_name,
            connection_timeout=5,
        )
    except Exception as erro:
        print(f"[AVISO BD] Indisponível, utilizando API/Bronze: {erro}")
        return {}
    try:
        cursor = conexao.cursor(dictionary=True)
        cursor.execute('''
            SELECT i.id_instancia, i.identificador AS identifier, i.nome AS vm_name,
                   e.id_empresa AS enterprise_identifier, e.razao_social AS social_reason
            FROM empresa AS e
            INNER JOIN instancia AS i ON i.fk_empresa = e.id_empresa;''')
        mapa = {}
        for r in cursor.fetchall():
            ident = normalizar_identificador(r["identifier"])
            if ident:
                mapa[ident] = {
                    "id_instancia": r.get("id_instancia"),
                    "empresa_id": para_texto(r["enterprise_identifier"]),
                    "empresa_nome": para_texto(r["social_reason"]),
                    "vm_nome": para_texto(r["vm_name"]),
                }
        return mapa
    finally:
        conexao.close()

# UTILITÁRIOS PANDAS / JSON

def para_texto(valor):
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    texto = str(valor).strip()
    return texto if texto and texto.lower() not in ("none", "null", "nan") else None


def normalizar_identificador(valor):
    texto = para_texto(valor)
    return texto.lower().replace("-", "").replace("_", "") if texto else ""

def nome_seguro(texto):
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(texto))[:120]


def col_num(df, coluna):
    """Retorna uma Series numérica (float) para a coluna, ou NaNs se não existir."""
    if coluna not in df.columns:
        return pd.Series(float("nan"), index=df.index, dtype="float64")
    return pd.to_numeric(df[coluna].astype(str).str.replace(",", ".", regex=False), errors="coerce")


 
def col_pos(df, coluna):
    s = col_num(df, coluna)
    return s.where(s >= 0)
 
def dividir(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b
 


def col_pct(df, coluna):
    s = col_num(df, coluna)
    return s.where((s >= 0) & (s <= 100))


def delta_pos(serie):
    d = serie.diff()
    return d.where(d >= 0)


def parse_nucleos(valor):
    texto = para_texto(valor)
    if not texto:
        return []
    res = []
    for p in texto.split("|"):
        try:
            v = float(p.strip().replace(",", "."))
            if 0 <= v <= 100:
                res.append(round(v, 2))
        except ValueError:
            continue
    return res


def limpar_valor_json(v):
    """Converte tipos NumPy/Pandas e NaNs para tipos nativos do Python."""
    if isinstance(v, list):
        return [limpar_valor_json(x) for x in v]
    if v is None or pd.isna(v):
        return None
    if hasattr(v, "item"):
        return v.item()
    return v


def ultimo_valido(df, coluna):
    if coluna not in df.columns:
        return None
    for v in reversed(df[coluna].tolist()):
        if isinstance(v, list):
            if v:
                return v
        elif v is not None and not pd.isna(v):
            return limpar_valor_json(v)
    return None


def ultimo_valido_bronze(df, *colunas):
    for col in colunas:
        if col in df.columns:
            validos = df[col].dropna().astype(str).str.strip()
            validos = validos[~validos.str.lower().isin(["", "none", "null", "nan"])]
            if not validos.empty:
                return validos.iloc[-1]
    return None


def estatisticas_serie(serie):
    s = pd.to_numeric(serie, errors="coerce").dropna()
    if s.empty:
        return {
            "amostras": 0, "min": None, "media": None, "mediana": None,
            "p95": None, "max": None, "desvio_padrao": None,
            f"desvio_padrao_ultimos_{JANELA_DESVIO}": None,
        }
    ultimos = s.tail(JANELA_DESVIO)
    return {
        "amostras": int(len(s)),
        "min": round(float(s.min()), 2),
        "media": round(float(s.mean()), 2),
        "mediana": round(float(s.median()), 2),
        "p95": round(float(s.quantile(0.95)), 2),
        "max": round(float(s.max()), 2),
        "desvio_padrao": round(float(s.std(ddof=0)), 3) if len(s) >= 2 else None,
        f"desvio_padrao_ultimos_{JANELA_DESVIO}": round(float(ultimos.std(ddof=0)), 3) if len(ultimos) >= 2 else None,
    }

# LEITURA BRONZE (PANDAS)

def ler_bronze(caminho):
    if not os.path.isfile(caminho) or os.path.getsize(caminho) == 0:
        return pd.DataFrame()
    df = None
    try:
        # Engine C (rápido e baixo uso de RAM) para CSV padrão delimitado por ';'
        df = pd.read_csv(caminho, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False)
        df.columns = [c.strip() for c in df.columns]
    except Exception:
        df = None

    if df is None or any(c not in df.columns for c in COLUNAS_OBRIGATORIAS):
        try:
            df = pd.read_csv(caminho, sep=None, engine="python", encoding="utf-8", on_bad_lines="skip")
            df.columns = [c.strip() for c in df.columns]
        except Exception:
            return pd.DataFrame()

    if any(c not in df.columns for c in COLUNAS_OBRIGATORIAS):
        return pd.DataFrame()

    df["_ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["identifier", "_ts"])
    df = df.sort_values("_ts").drop_duplicates(subset=["identifier", "_ts"], keep="last")
    return df.reset_index(drop=True)


# TRANSFORMAÇÃO VETORIZADA (PANDAS)

def transformar_dataframe(df_bronze, empresa):
    df = df_bronze.sort_values("_ts").drop_duplicates(subset=["identifier", "_ts"], keep="last").reset_index(drop=True)

    intervalo = df["_ts"].diff().dt.total_seconds()
    intervalo = intervalo.where(intervalo > 0)

    def taxa(coluna):
        return (delta_pos(col_pos(df, coluna)) / intervalo).round(2)

    # CPU modos
    modos = ["cpu_user", "cpu_system", "cpu_nice", "cpu_idle", "cpu_iowait", "cpu_irq", "cpu_softirq", "cpu_steal"]
    deltas_modos = pd.DataFrame({m: delta_pos(col_pos(df, m)) for m in modos})
    total_modos = deltas_modos.sum(axis=1, min_count=1).replace(0, float("nan"))

    nucleos_logicos = col_pos(df, "cpu_count_logical")
    load_1m = col_pos(df, "load_1m")

    # RAM
    ram_total = col_pos(df, "ram_total")
    ram_used = col_pos(df, "ram_used")
    ram_avail = col_pos(df, "ram_available")
    ram_buffers = col_pos(df, "ram_buffers")
    ram_cached = col_pos(df, "ram_cached")
    ram_slab = col_pos(df, "ram_slab")
    buff_cache = pd.DataFrame({"b": ram_buffers, "c": ram_cached, "s": ram_slab}).sum(axis=1, min_count=1)

    # Swap
    swap_total = col_pos(df, "swap_total")
    swap_used = col_pos(df, "swap_used")
    swap_free = col_pos(df, "swap_free")
    swap_sin_mb_s = ((delta_pos(col_pos(df, "swap_sin")) / BYTES_POR_MB) / intervalo).round(2)
    swap_sout_mb_s = ((delta_pos(col_pos(df, "swap_sout")) / BYTES_POR_MB) / intervalo).round(2)

    # Disco
    d_leituras = delta_pos(col_pos(df, "disk_read_count"))
    d_escritas = delta_pos(col_pos(df, "disk_write_count"))
    d_t_leitura = delta_pos(col_pos(df, "disk_read_time"))
    d_t_escrita = delta_pos(col_pos(df, "disk_write_time"))
    d_io_total = pd.DataFrame({"r": d_leituras, "w": d_escritas}).sum(axis=1, min_count=1)
    d_t_total = pd.DataFrame({"r": d_t_leitura, "w": d_t_escrita}).sum(axis=1, min_count=1)

    # Rede
    rede_upload_bytes_s = taxa("network_bytes_sent")
    rede_download_bytes_s = taxa("network_bytes_recv")
    rede_upload_mb_m = ((rede_upload_bytes_s * 60) / BYTES_POR_MB).round(2)
    rede_download_mb_m = ((rede_download_bytes_s * 60) / BYTES_POR_MB).round(2)
    rede_upload_mil_pacotes_m = ((taxa("network_packets_sent") * 60) / 1000.0).round(2)
    rede_download_mil_pacotes_m = ((taxa("network_packets_recv") * 60) / 1000.0).round(2)

    d_pac = pd.DataFrame({
        "s": delta_pos(col_pos(df, "network_packets_sent")),
        "r": delta_pos(col_pos(df, "network_packets_recv")),
    }).sum(axis=1, min_count=1)
    d_drop = pd.DataFrame({
        "i": delta_pos(col_pos(df, "network_dropin")),
        "o": delta_pos(col_pos(df, "network_dropout")),
    }).sum(axis=1, min_count=1)
    d_err = pd.DataFrame({
        "i": delta_pos(col_pos(df, "network_errin")),
        "o": delta_pos(col_pos(df, "network_errout")),
    }).sum(axis=1, min_count=1)
    perda_pct = ((d_drop / (d_pac + d_drop).replace(0, float("nan"))) * 100).round(3)

    net_est = col_pos(df, "net_established")
    net_tw = col_pos(df, "net_time_wait")
    net_cw = col_pos(df, "net_close_wait")
    soma_tcp_3 = pd.DataFrame({"e": net_est, "t": net_tw, "c": net_cw}).sum(axis=1, min_count=1).replace(0, float("nan"))

    # CPU por núcleo
    if "cpu_por_nucleo" in df.columns:
        cpu_nucleos_series = df["cpu_por_nucleo"].apply(parse_nucleos)
    else:
        cpu_nucleos_series = pd.Series([[] for _ in range(len(df))], index=df.index)

    out = pd.DataFrame({
        "empresa_id": empresa["empresa_id"],
        "empresa_nome": empresa["empresa_nome"],
 
        "identifier": df["identifier"].apply(para_texto),
 
        "vm_nome": empresa.get("vm_nome"),
        "hostname": df["hostname"].apply(para_texto) if "hostname" in df.columns else None,
        "os": df["os"].apply(para_texto) if "os" in df.columns else None,
        "timestamp": df["_ts"].dt.strftime(FORMATO_TIMESTAMP),
        "intervalo_segundos": intervalo.round(3),

        # CPU
        "cpu_percent": col_pct(df, "cpu_percent").round(2),
        "cpu_freq_mhz": col_pos(df, "cpu_freq_current").round(2),
        "cpu_nucleos_logicos": nucleos_logicos,
        "cpu_nucleos_fisicos": col_pos(df, "cpu_count_physical"),
        "cpu_por_nucleo": cpu_nucleos_series,
        "load_1m": load_1m.round(3),
        "load_5m": col_pos(df, "load_5m").round(3),
        "load_15m": col_pos(df, "load_15m").round(3),
        "load_por_nucleo": (load_1m / nucleos_logicos.replace(0, float("nan"))).round(3),
        "cpu_user_pct": ((deltas_modos["cpu_user"] / total_modos) * 100).round(2),
        "cpu_sys_pct": ((deltas_modos["cpu_system"] / total_modos) * 100).round(2),
        "cpu_nice_pct": ((deltas_modos["cpu_nice"] / total_modos) * 100).round(2),
        "cpu_idle_pct": ((deltas_modos["cpu_idle"] / total_modos) * 100).round(2),
        "cpu_iowait_pct": ((deltas_modos["cpu_iowait"] / total_modos) * 100).round(2),
        "qtd_processos": col_pos(df, "process_count"),
        "qtd_threads": col_pos(df, "thread_count"),

        # RAM
        "ram_percent": col_pct(df, "ram_percent").round(2),
        "ram_total_gb": (ram_total / BYTES_POR_GB).round(3),
        "ram_disponivel_gb": (ram_avail / BYTES_POR_GB).round(3),
        "ram_usada_gb": (ram_used / BYTES_POR_GB).round(3),
        "ram_cached_gb": (ram_cached / BYTES_POR_GB).round(3),
        "ram_buffers_gb": (ram_buffers / BYTES_POR_GB).round(3),
        "ram_shared_gb": (col_pos(df, "ram_shared") / BYTES_POR_GB).round(3),
        "ram_slab_gb": (ram_slab / BYTES_POR_GB).round(3),
        "ram_buff_cache_usado_gb": (buff_cache / BYTES_POR_GB).round(3),
        "ram_buff_cache_total_gb": (ram_total / BYTES_POR_GB).round(3),

        # SWAP
        "swap_percent": col_pct(df, "swap_percent").round(2),
        "swap_total_gb": (swap_total / BYTES_POR_GB).round(3),
        "swap_disponivel_gb": (swap_free / BYTES_POR_GB).round(3),
        "swap_usada_gb": (swap_used / BYTES_POR_GB).round(3),
        "swap_sin_mb_s": swap_sin_mb_s,
        "swap_sout_mb_s": swap_sout_mb_s,

        # DISCO
        "disco_percent": col_pct(df, "disk_root_percent").round(2),
        "disco_total_gb": (col_pos(df, "disk_root_total") / BYTES_POR_GB).round(3),
        "disco_usado_gb": (col_pos(df, "disk_root_used") / BYTES_POR_GB).round(3),
        "disco_livre_gb": (col_pos(df, "disk_root_free") / BYTES_POR_GB).round(3),
        "disco_leitura_bytes_s": taxa("disk_read_bytes"),
        "disco_escrita_bytes_s": taxa("disk_write_bytes"),
        "disco_leitura_mb_s": (taxa("disk_read_bytes") / BYTES_POR_MB).round(2),
        "disco_escrita_mb_s": (taxa("disk_write_bytes") / BYTES_POR_MB).round(2),
        "disco_iops_leitura": taxa("disk_read_count"),
        "disco_iops_escrita": taxa("disk_write_count"),
        "disco_iops_total": (d_io_total / intervalo).round(2),
        "disco_latencia_leitura_ms": (d_t_leitura / d_leituras.replace(0, float("nan"))).round(3),
        "disco_latencia_escrita_ms": (d_t_escrita / d_escritas.replace(0, float("nan"))).round(3),
        "disco_latencia_ms": (d_t_total / d_io_total.replace(0, float("nan"))).round(3),

        # Hardware / Billing (preservados no Silver para pricing.py)
        "vcpus": col_pos(df, "vcpus").fillna(nucleos_logicos),
        "ram_gb": col_pos(df, "ram_gb").fillna((ram_total / BYTES_POR_GB).round(2)),
        "disk_gb": col_pos(df, "disk_gb").fillna((col_pos(df, "disk_root_total") / BYTES_POR_GB).round(2)),
        "declared_monthly_cost": col_pos(df, "declared_monthly_cost"),

        # REDE
        "rede_upload_bytes_s": rede_upload_bytes_s,
        "rede_download_bytes_s": rede_download_bytes_s,
        "rede_upload_mb_m": rede_upload_mb_m,
        "rede_download_mb_m": rede_download_mb_m,
        "rede_upload_mil_pacotes_m": rede_upload_mil_pacotes_m,
        "rede_download_mil_pacotes_m": rede_download_mil_pacotes_m,
        "rede_perda_pacotes_local": d_drop.round(0),
        "rede_perda_pacotes_pct": perda_pct,
        "rede_erros_s": (d_err / intervalo).round(2),
        "rede_established": net_est,
        "rede_time_wait": net_tw,
        "rede_close_wait": net_cw,
        "rede_conn_total": col_pos(df, "net_conn_total"),
        "rede_established_pct": ((net_est / soma_tcp_3) * 100).round(1),
        "rede_time_wait_pct": ((net_tw / soma_tcp_3) * 100).round(1),
        "rede_close_wait_pct": ((net_cw / soma_tcp_3) * 100).round(1),
        "ec2_public_ip": df["ec2_public_ip"].apply(para_texto) if "ec2_public_ip" in df.columns else None,
        "ec2_instance_type": df["ec2_instance_type"].apply(para_texto) if "ec2_instance_type" in df.columns else None,
        "ec2_region": df["ec2_region"].apply(para_texto) if "ec2_region" in df.columns else None,
        "ec2_operating_system": df["ec2_operating_system"].apply(para_texto) if "ec2_operating_system" in df.columns else None,
        "ec2_license_model": df["ec2_license_model"].apply(para_texto) if "ec2_license_model" in df.columns else None,
        "globalping_regiao": df["globalping_regiao"].apply(para_texto) if "globalping_regiao" in df.columns else None,
        "globalping_target": df["globalping_target"].apply(para_texto) if "globalping_target" in df.columns else None,
        "globalping_latencia_ms": col_pos(df, "globalping_latencia_ms").round(2),
        "globalping_perda_pct": col_pct(df, "globalping_perda_pct").round(2),
        "globalping_perda_pacotes": col_pos(df, "globalping_perda_pacotes").round(0),
    })

    # Desvio padrão móvel vetorizado
    for m in METRICAS_DESVIO:
        if m in out.columns:
            out[f"{m}_dp{JANELA_DESVIO}"] = (
                pd.to_numeric(out[m], errors="coerce")
                .rolling(window=JANELA_DESVIO, min_periods=2)
                .std(ddof=0)
                .round(3)
            )

    return out

# CÁLCULOS DA DASHBOARD DE RAM (PANDAS)

def calcular_proximo_pico(caminho_eventos=ARQUIVO_EVENTOS):
    if not os.path.isfile(caminho_eventos):
        return None
    try:
        with open(caminho_eventos, "r", encoding="utf-8") as f:
            eventos = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None

    hoje = datetime.now().date()
    dias_semana = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
    proximo = None

    for ev in eventos:
        try:
            dt = datetime.strptime(ev["data"], "%Y-%m-%d").date()
        except (KeyError, ValueError):
            continue
        diff = (dt - hoje).days
        if diff >= 0 and (proximo is None or diff < proximo["dias_restantes"]):
            proximo = {
                "evento": ev["evento"],
                "data": ev["data"],
                "data_formatada": f"{dias_semana[dt.weekday()]} {dt.strftime('%d/%m')}",
                "dias_restantes": diff,
            }
    return proximo


def calcular_previsao_ram(df, janela=30):
    sub = df[["timestamp", "ram_percent"]].tail(janela).copy()
    sub["_ts"] = pd.to_datetime(sub["timestamp"], errors="coerce")
    sub["ram_percent"] = pd.to_numeric(sub["ram_percent"], errors="coerce")
    sub = sub.dropna(subset=["_ts", "ram_percent"])

    if len(sub) < 2:
        return {"minutos_ate_critico": None, "status": "estavel", "taxa_pct_por_hora": 0.0, "projecao_percent": None}

    xs = (sub["_ts"] - sub["_ts"].iloc[0]).dt.total_seconds() / 60.0
    ys = sub["ram_percent"]

    den = ((xs - xs.mean()) ** 2).sum()
    inclinacao = float(((xs - xs.mean()) * (ys - ys.mean())).sum() / den) if den > 0 else 0.0
    atual = float(ys.iloc[-1])

    if atual >= LIMITE_RAM_CRITICO:
        return {"minutos_ate_critico": 0, "status": "critico", "taxa_pct_por_hora": round(inclinacao * 60, 2), "projecao_percent": atual}
    if inclinacao <= 0.001:
        return {"minutos_ate_critico": None, "status": "estavel", "taxa_pct_por_hora": round(inclinacao * 60, 2), "projecao_percent": atual}

    minutos = (LIMITE_RAM_CRITICO - atual) / inclinacao
    status = "critico" if minutos <= 30 else ("atencao" if minutos <= 60 else "seguro")
    return {
        "minutos_ate_critico": round(minutos),
        "status": status,
        "taxa_pct_por_hora": round(inclinacao * 60, 2),
        "projecao_percent": LIMITE_RAM_CRITICO,
    }


def contar_alertas_ram_24h(df):
    if df.empty:
        return {"total": 0, "atencao": 0, "critico": 0}

    sub = df[["timestamp", "ram_percent"]].copy()
    sub["_ts"] = pd.to_datetime(sub["timestamp"], errors="coerce")
    sub["ram_percent"] = pd.to_numeric(sub["ram_percent"], errors="coerce")
    sub = sub.dropna(subset=["_ts", "ram_percent"])
    if sub.empty:
        return {"total": 0, "atencao": 0, "critico": 0}

    limite_24h = sub["_ts"].max() - pd.Timedelta(hours=24)
    sub = sub[sub["_ts"] >= limite_24h]

    estados = pd.Series("normal", index=sub.index)
    estados = estados.mask(sub["ram_percent"] >= LIMITE_RAM_ATENCAO, "atencao")
    estados = estados.mask(sub["ram_percent"] >= LIMITE_RAM_CRITICO, "critico")

    transicoes = estados[estados != estados.shift(1)]
    atencao = int((transicoes == "atencao").sum())
    critico = int((transicoes == "critico").sum())
    return {"total": atencao + critico, "atencao": atencao, "critico": critico}


def processar_processos(caminhos_csv):
    """
    Processa os CSVs de processos gerando:
      - cpu_top5: Top 5 processos por uso de CPU (Dashboards Geral e CPU do Analista)
      - ram: maior_uso e maior_crescimento (Dashboard de RAM do Analista)
    """
    vazio = {"cpu_top5": [], "ram": {"maior_uso": [], "maior_crescimento": []}}
    dfs = []
    for caminho in caminhos_csv:
        if os.path.isfile(caminho) and os.path.getsize(caminho) > 0:
            try:
                dfs.append(pd.read_csv(caminho, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False))
            except Exception:
                continue
    if not dfs:
        return vazio

    df = pd.concat(dfs, ignore_index=True)
    if "timestamp" not in df.columns or "pid" not in df.columns:
        return vazio

    df["_ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["memory_rss_mb"] = pd.to_numeric(df.get("memory_rss_mb"), errors="coerce").fillna(0.0)
    df["memory_percent"] = pd.to_numeric(df.get("memory_percent"), errors="coerce").fillna(0.0)
    df["cpu_percent"] = pd.to_numeric(df.get("cpu_percent"), errors="coerce").fillna(0.0)
    df = df.dropna(subset=["pid", "_ts"]).sort_values("_ts")

    if df.empty:
        return vazio

    ultimo_ts = df["_ts"].max()
    agrupado = df.groupby("pid").agg(
        processo=("name", "last"),
        ts_ini=("_ts", "first"),
        ts_fim=("_ts", "last"),
        rss_ini=("memory_rss_mb", "first"),
        rss_fim=("memory_rss_mb", "last"),
        ram_percent=("memory_percent", "last"),
        cpu_percent=("cpu_percent", "last"),
    ).reset_index()

    agrupado = agrupado[(ultimo_ts - agrupado["ts_fim"]).dt.total_seconds() <= 60]
    if agrupado.empty:
        return vazio

    horas = (agrupado["ts_fim"] - agrupado["ts_ini"]).dt.total_seconds() / 3600.0
    agrupado["tendencia_mb_h"] = ((agrupado["rss_fim"] - agrupado["rss_ini"]) / horas).where(horas > 0.001, 0.0).round(1)
    agrupado["ram_mb"] = agrupado["rss_fim"].round(1)
    agrupado["ram_percent"] = agrupado["ram_percent"].round(1)
    agrupado["cpu_percent"] = agrupado["cpu_percent"].round(1)
    agrupado["pid"] = agrupado["pid"].astype(str)

    # Top 5 CPU (usado nas dashboards Geral e CPU)
    top_cpu_df = agrupado.sort_values(by=["cpu_percent", "ram_percent"], ascending=[False, False]).head(5)
    cpu_top5 = [
        {
            "pid": r["pid"],
            "nome": para_texto(r["processo"]) or "desconhecido",
            "cpu_percent": limpar_valor_json(r["cpu_percent"]),
            "memory_percent": limpar_valor_json(r["ram_percent"]),
            "ram_mb": limpar_valor_json(r["ram_mb"]),
        }
        for r in top_cpu_df.to_dict(orient="records")
    ]

    cols_ram = ["pid", "processo", "cpu_percent", "ram_mb", "ram_percent", "tendencia_mb_h"]
    return {
        "cpu_top5": cpu_top5,
        "ram": {
            "maior_uso": agrupado.nlargest(5, "ram_mb")[cols_ram].to_dict(orient="records"),
            "maior_crescimento": agrupado.nlargest(5, "tendencia_mb_h")[cols_ram].to_dict(orient="records"),
        },
    }


def processar_processos_ram(caminhos_csv):
    """Alias retrocompatível que devolve apenas o bloco de processos de RAM."""
    return processar_processos(caminhos_csv)["ram"]


def processar_particoes(caminhos_csv):
    """Lê o snapshot mais recente das partições em *_particoes.csv para a Dashboard de Disco."""
    dfs = []
    for caminho in caminhos_csv:
        if os.path.isfile(caminho) and os.path.getsize(caminho) > 0:
            try:
                dfs.append(pd.read_csv(caminho, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False))
            except Exception:
                continue
    if not dfs:
        return []

    df = pd.concat(dfs, ignore_index=True)
    if "timestamp" not in df.columns or "mountpoint" not in df.columns:
        return []

    df["_ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["_ts", "mountpoint"]).sort_values("_ts")
    if df.empty:
        return []

    ultimas = df.drop_duplicates(subset=["mountpoint"], keep="last")
    resultado = []
    for _, r in ultimas.iterrows():
        total = pd.to_numeric(r.get("total"), errors="coerce")
        usado = pd.to_numeric(r.get("used"), errors="coerce")
        livre = pd.to_numeric(r.get("free"), errors="coerce")
        pct = pd.to_numeric(r.get("percent"), errors="coerce")
        resultado.append({
            "device": para_texto(r.get("device")),
            "mountpoint": para_texto(r.get("mountpoint")),
            "fstype": para_texto(r.get("fstype")),
            "total_gb": round(float(total) / BYTES_POR_GB, 2) if not pd.isna(total) else None,
            "usado_gb": round(float(usado) / BYTES_POR_GB, 2) if not pd.isna(usado) else None,
            "livre_gb": round(float(livre) / BYTES_POR_GB, 2) if not pd.isna(livre) else None,
            "percent": round(float(pct), 2) if not pd.isna(pct) else None,
        })
    return resultado


# CÁLCULOS DA DASHBOARD DE REDE (PANDAS)

def _ultimo_nao_nulo(serie):
    s = pd.to_numeric(serie, errors="coerce").dropna()
    return float(s.iloc[-1]) if not s.empty else float("nan")


def _soma_valida(serie):
    s = pd.to_numeric(serie, errors="coerce").dropna()
    return float(s.sum()) if not s.empty else float("nan")


def _media_valida(serie):
    s = pd.to_numeric(serie, errors="coerce").dropna()
    return float(s.mean()) if not s.empty else float("nan")


def agregar_janelas_15m_rede(df):
    """Agrega as leituras de rede em janelas de 15 minutos."""
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()

    sub = df.copy()
    sub["_ts"] = pd.to_datetime(sub["timestamp"], errors="coerce")
    sub = sub.dropna(subset=["_ts"]).sort_values("_ts")
    if sub.empty:
        return pd.DataFrame()

    sub["_janela"] = sub["_ts"].dt.floor("15min")

    colunas_soma = ["rede_perda_pacotes_local"]
    colunas_media = [
        "rede_download_mb_m", "rede_upload_mb_m",
        "rede_download_mil_pacotes_m", "rede_upload_mil_pacotes_m",
        "rede_perda_pacotes_pct",
    ]
    colunas_ultimo = [
        "rede_established", "rede_time_wait", "rede_close_wait", "rede_conn_total",
        "rede_established_pct", "rede_time_wait_pct", "rede_close_wait_pct",
        "globalping_latencia_ms", "globalping_perda_pct", "globalping_perda_pacotes",
    ]

    linhas = []
    for janela, grupo in sub.groupby("_janela", sort=True):
        reg = {"_ts": janela, "timestamp": janela.strftime(FORMATO_TIMESTAMP)}
        for col in colunas_soma:
            if col in grupo.columns:
                val = _soma_valida(grupo[col])
                reg[col] = round(val, 0) if not pd.isna(val) else None
        for col in colunas_media:
            if col in grupo.columns:
                val = _media_valida(grupo[col])
                reg[col] = round(val, 2) if not pd.isna(val) else None
        for col in colunas_ultimo:
            if col in grupo.columns:
                val = _ultimo_nao_nulo(grupo[col])
                reg[col] = round(val, 2) if not pd.isna(val) else None
        linhas.append(reg)

    return pd.DataFrame(linhas)


def _serie_limpa(df_base, coluna):
    if df_base is None or df_base.empty or coluna not in df_base.columns:
        return []
    s = pd.to_numeric(df_base[coluna], errors="coerce").dropna()
    return [limpar_valor_json(round(float(v), 2)) for v in s.tolist()]


def _calcular_tendencia_serie(valores):
    vals = [float(v) for v in valores if v is not None and not pd.isna(v)]
    if len(vals) < 2:
        return {"valor": 0, "direcao": "estavel", "texto": "Tendência desde última hora: 0%"}

    atual = vals[-1]
    anteriores = vals[:-1]
    media_ant = sum(anteriores) / len(anteriores)

    if media_ant == 0:
        pct = 100 if atual > 0 else 0
    else:
        pct = round(((atual - media_ant) / media_ant) * 100)

    sinal = "+" if pct > 0 else ""
    direcao = "alta" if pct > 0 else ("baixa" if pct < 0 else "estavel")
    return {
        "valor": int(pct),
        "direcao": direcao,
        "texto": f"Tendência desde última hora: {sinal}{pct}%",
    }


def _filtrar_ultima_hora(df_base):
    if df_base is None or df_base.empty or "timestamp" not in df_base.columns:
        return pd.DataFrame()
    sub = df_base.copy()
    sub["_ts"] = pd.to_datetime(sub["timestamp"], errors="coerce")
    sub = sub.dropna(subset=["_ts"]).sort_values("_ts")
    if sub.empty:
        return pd.DataFrame()
    limite = sub["_ts"].max() - pd.Timedelta(hours=1)
    return sub[sub["_ts"] >= limite]


def calcular_tendencias_rede(df, df_15m):
    """Calcula a tendência percentual da última hora para as 7 métricas da dashboard."""
    janelas_1h = _filtrar_ultima_hora(df_15m)
    raw_1h = _filtrar_ultima_hora(df)

    mapa_metricas = {
        "perda_local_pct": "rede_perda_pacotes_local",
        "perda_externa_pct": "globalping_perda_pacotes",
        "conexoes_established_pct": "rede_established_pct",
        "volume_download_pct": "rede_download_mb_m",
        "volume_upload_pct": "rede_upload_mb_m",
        "pacotes_download_pct": "rede_download_mil_pacotes_m",
        "pacotes_upload_pct": "rede_upload_mil_pacotes_m",
    }

    resultado = {}
    for chave, col in mapa_metricas.items():
        vals = _serie_limpa(janelas_1h, col)
        if len(vals) < 2:
            vals = _serie_limpa(raw_1h, col)
        resultado[chave] = _calcular_tendencia_serie(vals)
    return resultado


def _prever_metrica_linear(df_base, coluna, percentual=False, janela=12):
    """Calcula taxa de variação/hora e projeção para 1h por regressão linear, sem comparar alertas."""
    if df_base is None or df_base.empty or coluna not in df_base.columns:
        return {
            "atual": None, "taxa_por_hora": 0.0,
            "projecao_1h": None, "tendencia_direcao": "estavel",
        }

    sub = df_base[["timestamp", coluna]].tail(janela).copy()
    sub["_ts"] = pd.to_datetime(sub["timestamp"], errors="coerce")
    sub[coluna] = pd.to_numeric(sub[coluna], errors="coerce")
    sub = sub.dropna(subset=["_ts", coluna])

    if sub.empty:
        return {
            "atual": None, "taxa_por_hora": 0.0,
            "projecao_1h": None, "tendencia_direcao": "estavel",
        }

    atual = round(float(sub[coluna].iloc[-1]), 2)
    if len(sub) < 2:
        return {
            "atual": atual, "taxa_por_hora": 0.0,
            "projecao_1h": atual, "tendencia_direcao": "estavel",
        }

    xs = (sub["_ts"] - sub["_ts"].iloc[0]).dt.total_seconds() / 60.0
    ys = sub[coluna]
    den = ((xs - xs.mean()) ** 2).sum()
    inclinacao = float(((xs - xs.mean()) * (ys - ys.mean())).sum() / den) if den > 0 else 0.0
    taxa_h = round(inclinacao * 60.0, 2)
    dir_tend = "alta" if inclinacao > 0.001 else ("baixa" if inclinacao < -0.001 else "estavel")

    projecao = round(atual + taxa_h, 2)
    if percentual:
        projecao = max(0.0, min(100.0, projecao))
    else:
        projecao = max(0.0, projecao)

    return {
        "atual": atual,
        "taxa_por_hora": taxa_h,
        "projecao_1h": projecao,
        "tendencia_direcao": dir_tend,
    }


def calcular_previsao_rede(df, df_15m):
    """Retorna projeções puramente numéricas das métricas de rede (comparação de alertas fica no Java)."""
    base = df_15m if (df_15m is not None and len(df_15m) >= 2) else df

    metricas = {
        "perda_local": _prever_metrica_linear(base, "rede_perda_pacotes_local"),
        "perda_externa": _prever_metrica_linear(base, "globalping_perda_pacotes"),
        "conexoes_established_pct": _prever_metrica_linear(base, "rede_established_pct", percentual=True),
        "volume_download_mb_m": _prever_metrica_linear(base, "rede_download_mb_m"),
        "volume_upload_mb_m": _prever_metrica_linear(base, "rede_upload_mb_m"),
        "pacotes_download_mil_m": _prever_metrica_linear(base, "rede_download_mil_pacotes_m"),
        "pacotes_upload_mil_m": _prever_metrica_linear(base, "rede_upload_mil_pacotes_m"),
        "latencia_externa_ms": _prever_metrica_linear(base, "globalping_latencia_ms"),
    }

    return {
        "metricas": metricas,
        "proximo_pico": calcular_proximo_pico(),
    }


def montar_janelas_15m_gold_rede(df_15m):
    if df_15m is None or df_15m.empty:
        return {
            "labels": [], "timestamps": [],
            "perda_local_pacotes": [], "perda_externa_pacotes": [], "latencia_externa_ms": [],
            "conexoes_established_pct": [],
            "volume_download_mb_m": [], "volume_upload_mb_m": [],
            "pacotes_download_mil_m": [], "pacotes_upload_mil_m": [],
            "historico_15m": [],
        }

    ultimas_5 = df_15m.tail(5).copy()
    timestamps = [str(t) for t in ultimas_5["timestamp"].tolist()]
    labels = [pd.to_datetime(t).strftime("%H:%M") for t in timestamps]
    if labels:
        labels[-1] = "Agora"

    def _lista(col):
        if col not in ultimas_5.columns:
            return []
        return [limpar_valor_json(v) for v in ultimas_5[col].tolist()]

    ultimas_24h = df_15m.tail(96).drop(columns=["_ts"], errors="ignore")
    hist_15m = [{k: limpar_valor_json(v) for k, v in r.items()} for r in ultimas_24h.to_dict(orient="records")]

    return {
        "labels": labels,
        "timestamps": timestamps,
        "perda_local_pacotes": _lista("rede_perda_pacotes_local"),
        "perda_externa_pacotes": _lista("globalping_perda_pacotes"),
        "latencia_externa_ms": _lista("globalping_latencia_ms"),
        "conexoes_established_pct": _lista("rede_established_pct"),
        "volume_download_mb_m": _lista("rede_download_mb_m"),
        "volume_upload_mb_m": _lista("rede_upload_mb_m"),
        "pacotes_download_mil_m": _lista("rede_download_mil_pacotes_m"),
        "pacotes_upload_mil_m": _lista("rede_upload_mil_pacotes_m"),
        "historico_15m": hist_15m,
    }


# JANELAS 15M UNIFICADAS (DASHBOARDS DO ANALISTA: GERAL, CPU, RAM, DISCO E REDE)

def _p95_valido(serie):
    s = pd.to_numeric(serie, errors="coerce").dropna()
    return float(s.quantile(0.95)) if not s.empty else float("nan")


def agregar_janelas_15m_geral(df):
    """Agrega CPU, RAM, Disco e Rede em janelas de 15 minutos prontas para os gráficos."""
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()

    sub = df.copy()
    sub["_ts"] = pd.to_datetime(sub["timestamp"], errors="coerce")
    sub = sub.dropna(subset=["_ts"]).sort_values("_ts")
    if sub.empty:
        return pd.DataFrame()

    sub["_janela"] = sub["_ts"].dt.floor("15min")

    colunas_media = [
        "cpu_percent", "load_1m", "load_5m", "load_15m",
        "ram_percent", "ram_usada_gb", "ram_disponivel_gb", "ram_buff_cache_usado_gb",
        "disco_percent", "disco_leitura_mb_s", "disco_escrita_mb_s",
        "disco_iops_leitura", "disco_iops_escrita", "disco_latencia_ms",
        "rede_download_mb_m", "rede_upload_mb_m",
        "rede_download_mil_pacotes_m", "rede_upload_mil_pacotes_m",
        "rede_perda_pacotes_pct",
    ]
    colunas_soma = ["rede_perda_pacotes_local"]
    colunas_ultimo = [
        "rede_established", "rede_time_wait", "rede_close_wait", "rede_conn_total",
        "rede_established_pct", "rede_time_wait_pct", "rede_close_wait_pct",
        "globalping_latencia_ms", "globalping_perda_pct", "globalping_perda_pacotes",
    ]

    linhas = []
    for janela, grupo in sub.groupby("_janela", sort=True):
        reg = {"_ts": janela, "timestamp": janela.strftime(FORMATO_TIMESTAMP)}
        for col in colunas_media:
            if col in grupo.columns:
                val = _media_valida(grupo[col])
                reg[col] = round(val, 2) if not pd.isna(val) else None
        if "disco_latencia_ms" in grupo.columns:
            p95 = _p95_valido(grupo["disco_latencia_ms"])
            reg["disco_latencia_p95_ms"] = round(p95, 2) if not pd.isna(p95) else None
        for col in colunas_soma:
            if col in grupo.columns:
                val = _soma_valida(grupo[col])
                reg[col] = round(val, 0) if not pd.isna(val) else None
        for col in colunas_ultimo:
            if col in grupo.columns:
                val = _ultimo_nao_nulo(grupo[col])
                reg[col] = round(val, 2) if not pd.isna(val) else None
        linhas.append(reg)

    return pd.DataFrame(linhas)


def montar_janelas_15m_gold_geral(df_15m):
    """Monta as séries de 5 pontos (gráficos rápidos) e últimas 24h (96 janelas de 15m)."""
    campos_series = {
        "cpu_uso_percent": "cpu_percent",
        "cpu_load_1m": "load_1m",
        "cpu_load_5m": "load_5m",
        "cpu_load_15m": "load_15m",
        "ram_uso_percent": "ram_percent",
        "ram_usada_gb": "ram_usada_gb",
        "ram_disponivel_gb": "ram_disponivel_gb",
        "ram_buff_cache_gb": "ram_buff_cache_usado_gb",
        "disco_uso_percent": "disco_percent",
        "disco_leitura_mb_s": "disco_leitura_mb_s",
        "disco_escrita_mb_s": "disco_escrita_mb_s",
        "disco_iops_leitura": "disco_iops_leitura",
        "disco_iops_escrita": "disco_iops_escrita",
        "disco_latencia_media_ms": "disco_latencia_ms",
        "disco_latencia_p95_ms": "disco_latencia_p95_ms",
        "rede_perda_local": "rede_perda_pacotes_local",
        "rede_perda_externa": "globalping_perda_pacotes",
        "rede_latencia_externa_ms": "globalping_latencia_ms",
        "rede_conexoes_established_pct": "rede_established_pct",
        "rede_download_mb_m": "rede_download_mb_m",
        "rede_upload_mb_m": "rede_upload_mb_m",
        "rede_pacotes_download_mil_m": "rede_download_mil_pacotes_m",
        "rede_pacotes_upload_mil_m": "rede_upload_mil_pacotes_m",
    }
    if df_15m is None or df_15m.empty:
        return {"labels": [], "timestamps": [], **{k: [] for k in campos_series}, "historico_15m": []}

    ultimas_5 = df_15m.tail(5).copy()
    timestamps = [str(t) for t in ultimas_5["timestamp"].tolist()]
    labels = [pd.to_datetime(t).strftime("%H:%M") for t in timestamps]
    if labels:
        labels[-1] = "Agora"

    def _lista(col):
        if col not in ultimas_5.columns:
            return []
        return [limpar_valor_json(v) for v in ultimas_5[col].tolist()]

    ultimas_24h = df_15m.tail(96).drop(columns=["_ts"], errors="ignore")
    hist_15m = [{k: limpar_valor_json(v) for k, v in r.items()} for r in ultimas_24h.to_dict(orient="records")]

    return {
        "labels": labels,
        "timestamps": timestamps,
        **{chave: _lista(col) for chave, col in campos_series.items()},
        "historico_15m": hist_15m,
    }


# SILVER (CSV) E GOLD (JSON LEVE ESTRUTURADO)

def _extrair_data_caminho(caminho_ou_chave):
    """Extrai YYYY-MM-DD de caminhos particionados como <empresa>/<id>/2026-11-27/<arq>.csv."""
    for parte in reversed(str(caminho_ou_chave).replace("\\", "/").split("/")):
        if REGEX_DATA_DIR.match(parte):
            return parte
    return None


def gravar_silver(pasta_empresa, identificador, df, data_referencia=None):
    if data_referencia:
        diretorio = os.path.join(DIR_SILVER, pasta_empresa, identificador, data_referencia)
    else:
        diretorio = os.path.join(DIR_SILVER, pasta_empresa, identificador)
    os.makedirs(diretorio, exist_ok=True)
    caminho = os.path.join(diretorio, f"{identificador}_sistema.csv")

    df_out = df.copy()
    if "cpu_por_nucleo" in df_out.columns:
        df_out["cpu_por_nucleo"] = df_out["cpu_por_nucleo"].apply(
            lambda v: "|".join(str(x) for x in v) if isinstance(v, list) and v else None
        )
    df_out.to_csv(caminho, sep=DELIMITADOR_SAIDA, index=False, na_rep="NA", encoding="utf-8")
    return caminho


def historico(df, campos):
    colunas = ["timestamp"]
    for c in campos:
        if c in df.columns:
            colunas.append(c)
        chave_dp = f"{c}_dp{JANELA_DESVIO}"
        if chave_dp in df.columns:
            colunas.append(chave_dp)

    registros = df[colunas].to_dict(orient="records")
    return [{k: limpar_valor_json(v) for k, v in r.items()} for r in registros]


def montar_gold_estruturado(df, processos=None, particoes=None, id_instancia=None, data_referencia=None):
    """
    Gera os 3 artefatos Gold leves e orientados às telas do Analista de Infraestrutura/TI:
      - atual.json: snapshot instantâneo (~5-15 KB) para cards e KPIs
      - dashboard.json: atual + janelas 15m (24h) + resumos (~50-150 KB) para os gráficos
      - historico/<YYYY-MM-DD>.json: histórico detalhado limitado apenas ao dia de referência
    """
    proc_info = processos if isinstance(processos, dict) else {"cpu_top5": [], "ram": {"maior_uso": [], "maior_crescimento": []}}
    proc_cpu = proc_info.get("cpu_top5", [])
    proc_ram = proc_info.get("ram", {"maior_uso": [], "maior_crescimento": []})
    lista_particoes = particoes or []

    ult = lambda c: ultimo_valido(df, c)
    est = lambda c: estatisticas_serie(df[c]) if c in df.columns else estatisticas_serie(pd.Series(dtype="float64"))

    df_15m_geral = agregar_janelas_15m_geral(df)
    janelas_geral = montar_janelas_15m_gold_geral(df_15m_geral)
    df_15m_rede = agregar_janelas_15m_rede(df)
    janelas_rede = montar_janelas_15m_gold_rede(df_15m_rede)
    tendencias_rede = calcular_tendencias_rede(df, df_15m_rede)
    previsibilidade_rede = calcular_previsao_rede(df, df_15m_rede)
    ult_15m = lambda c: ultimo_valido(df_15m_rede, c) if not df_15m_rede.empty else ult(c)

    ultimo_ts_str = str(df["timestamp"].iloc[-1])
    data_ref = data_referencia or ultimo_ts_str[:10]

    bloco_empresa = {
        "id": ult("empresa_id"),
        "nome": ult("empresa_nome"),
    }
    bloco_maquina = {
        "id_instancia": id_instancia,
        "identifier": ult("identifier"),
        "identifier_canonico": normalizar_identificador(ult("identifier")),
        "nome": ult("vm_nome"),
        "hostname": ult("hostname"),
        "os": ult("os"),
        "primeiro_registro": str(df["timestamp"].iloc[0]),
        "ultimo_registro": ultimo_ts_str,
        "data_referencia": data_ref,
        "total_registros": int(len(df)),
        "janela_desvio_padrao": JANELA_DESVIO,
    }

    cpu_atual = {
        "uso_percent": ult("cpu_percent"),
        "por_nucleo": ult("cpu_por_nucleo") or [],
        "load_average": {"1m": ult("load_1m"), "5m": ult("load_5m"), "15m": ult("load_15m")},
        "nucleos_logicos": ult("cpu_nucleos_logicos"),
        "qtd_processos": ult("qtd_processos"),
        "qtd_threads": ult("qtd_threads"),
        "modos_percent": {
            "user": ult("cpu_user_pct"), "sys": ult("cpu_sys_pct"),
            "nice": ult("cpu_nice_pct"), "idle": ult("cpu_idle_pct"),
            "iowait": ult("cpu_iowait_pct"),
        },
    }
    ram_atual = {
        "uso_percent": ult("ram_percent"),
        "total_gb": ult("ram_total_gb"),
        "disponivel_gb": ult("ram_disponivel_gb"),
        "usada_gb": ult("ram_usada_gb"),
        "modos_gb": {
            "cached": ult("ram_cached_gb"), "buffers": ult("ram_buffers_gb"),
            "shared": ult("ram_shared_gb"), "slab": ult("ram_slab_gb"),
            "available": ult("ram_disponivel_gb"),
        },
        "buffers_cache": {
            "usado_gb": ult("ram_buff_cache_usado_gb"),
            "total_gb": ult("ram_buff_cache_total_gb"),
        },
    }
    disco_atual = {
        "uso_percent": ult("disco_percent"),
        "total_gb": ult("disco_total_gb"),
        "usado_gb": ult("disco_usado_gb"),
        "livre_gb": ult("disco_livre_gb"),
        "leitura_bytes_s": ult("disco_leitura_bytes_s"),
        "escrita_bytes_s": ult("disco_escrita_bytes_s"),
        "leitura_mb_s": ult("disco_leitura_mb_s"),
        "escrita_mb_s": ult("disco_escrita_mb_s"),
        "iops": {
            "leitura": ult("disco_iops_leitura"),
            "escrita": ult("disco_iops_escrita"),
            "total": ult("disco_iops_total"),
        },
        "latencia_ms": {
            "leitura": ult("disco_latencia_leitura_ms"),
            "escrita": ult("disco_latencia_escrita_ms"),
            "media": ult("disco_latencia_ms"),
            "p95": est("disco_latencia_ms").get("p95"),
        },
    }
    swap_atual = {
        "uso_percent": ult("swap_percent"),
        "total_gb": ult("swap_total_gb"),
        "disponivel_gb": ult("swap_disponivel_gb"),
        "usada_gb": ult("swap_usada_gb"),
        "sin_mb_s": ult("swap_sin_mb_s"),
        "sout_mb_s": ult("swap_sout_mb_s"),
    }
    rede_atual = {
        "conexoes": {
            "established": ult("rede_established"),
            "time_wait": ult("rede_time_wait"),
            "close_wait": ult("rede_close_wait"),
            "total": ult("rede_conn_total"),
            "established_pct": ult("rede_established_pct"),
            "time_wait_pct": ult("rede_time_wait_pct"),
            "close_wait_pct": ult("rede_close_wait_pct"),
        },
        "perda_pacotes_local": ult_15m("rede_perda_pacotes_local"),
        "perda_pacotes_pct": ult("rede_perda_pacotes_pct"),
        "upload_bytes_s": ult("rede_upload_bytes_s"),
        "download_bytes_s": ult("rede_download_bytes_s"),
        "volume_download_mb_m": ult_15m("rede_download_mb_m"),
        "volume_upload_mb_m": ult_15m("rede_upload_mb_m"),
        "pacotes_download_mil_m": ult_15m("rede_download_mil_pacotes_m"),
        "pacotes_upload_mil_m": ult_15m("rede_upload_mil_pacotes_m"),
    }
    rede_globalping = {
        "regiao": ult("globalping_regiao") or "BR",
        "target_host": ult("globalping_target") or ult("ec2_public_ip"),
        "latencia_externa_ms": ult("globalping_latencia_ms"),
        "perda_externa_pacotes": ult("globalping_perda_pacotes"),
        "perda_externa_pct": ult("globalping_perda_pct"),
        "latencia_externa_graf": janelas_rede["latencia_externa_ms"],
        "perda_externa_pacotes_graf": janelas_rede["perda_externa_pacotes"],
    }

    previsao_ram = calcular_previsao_ram(df)
    alertas_ram_24h = contar_alertas_ram_24h(df)
    proximo_pico = calcular_proximo_pico()

    gold_atual = {
        "empresa": bloco_empresa,
        "maquina": bloco_maquina,
        "cpu": {
            "atual": cpu_atual,
            "processos_top5": proc_cpu,
        },
        "ram": {
            "atual": ram_atual,
            "previsao": previsao_ram,
            "alertas_24h": alertas_ram_24h,
            "proximo_pico": proximo_pico,
            "processos": proc_ram,
        },
        "disco": {
            "atual": disco_atual,
            "particoes": lista_particoes,
        },
        "swap": {
            "atual": swap_atual,
        },
        "rede": {
            "atual": rede_atual,
            "globalping": rede_globalping,
            "tendencias": tendencias_rede,
        },
    }

    gold_dashboard = {
        "empresa": bloco_empresa,
        "maquina": bloco_maquina,
        "janelas_15m": janelas_geral,
        "cpu": {
            "atual": cpu_atual,
            "resumo": {"uso_percent": est("cpu_percent"), "load_1m": est("load_1m")},
            "processos_top5": proc_cpu,
        },
        "ram": {
            "atual": ram_atual,
            "resumo": {"uso_percent": est("ram_percent")},
            "previsao": previsao_ram,
            "alertas_24h": alertas_ram_24h,
            "proximo_pico": proximo_pico,
            "processos": proc_ram,
        },
        "disco": {
            "atual": disco_atual,
            "particoes": lista_particoes,
            "resumo": {
                "uso_percent": est("disco_percent"),
                "iops_total": est("disco_iops_total"),
                "latencia_ms": est("disco_latencia_ms"),
            },
        },
        "swap": {
            "atual": swap_atual,
            "resumo": {"uso_percent": est("swap_percent")},
        },
        "rede": {
            "atual": rede_atual,
            "globalping": rede_globalping,
            "janelas_15m": janelas_rede,
            "tendencias": tendencias_rede,
            "previsibilidade": previsibilidade_rede,
            "resumo": {
                "upload_bytes_s": est("rede_upload_bytes_s"),
                "download_bytes_s": est("rede_download_bytes_s"),
                "volume_download_mb_m": est("rede_download_mb_m"),
                "volume_upload_mb_m": est("rede_upload_mb_m"),
                "pacotes_download_mil_m": est("rede_download_mil_pacotes_m"),
                "pacotes_upload_mil_m": est("rede_upload_mil_pacotes_m"),
                "perda_pacotes_local": est("rede_perda_pacotes_local"),
                "perda_pacotes_pct": est("rede_perda_pacotes_pct"),
                "latencia_externa_ms": est("globalping_latencia_ms"),
            },
        },
    }

    # Histórico diário estritamente limitado ao dia de referência
    df_dia = df[df["timestamp"].astype(str).str.startswith(data_ref)]
    if df_dia.empty:
        df_dia = df

    gold_historico = {
        "empresa": bloco_empresa,
        "maquina": bloco_maquina,
        "data_referencia": data_ref,
        "cpu": {
            "historico": historico(df_dia, [
                "cpu_percent", "cpu_por_nucleo", "load_1m", "load_5m", "load_15m",
                "cpu_user_pct", "cpu_sys_pct", "cpu_nice_pct", "cpu_idle_pct",
                "cpu_iowait_pct", "qtd_processos",
            ]),
        },
        "ram": {
            "historico": historico(df_dia, [
                "ram_percent", "ram_usada_gb", "ram_disponivel_gb", "ram_cached_gb",
                "ram_buffers_gb", "ram_shared_gb", "ram_slab_gb", "ram_buff_cache_usado_gb",
            ]),
        },
        "disco": {
            "historico": historico(df_dia, [
                "disco_percent", "disco_leitura_bytes_s", "disco_escrita_bytes_s",
                "disco_leitura_mb_s", "disco_escrita_mb_s",
                "disco_iops_leitura", "disco_iops_escrita", "disco_iops_total",
                "disco_latencia_leitura_ms", "disco_latencia_escrita_ms", "disco_latencia_ms",
            ]),
        },
        "swap": {
            "historico": historico(df_dia, ["swap_percent", "swap_usada_gb", "swap_sin_mb_s", "swap_sout_mb_s"]),
        },
        "rede": {
            "historico": historico(df_dia, [
                "rede_upload_bytes_s", "rede_download_bytes_s",
                "rede_upload_mb_m", "rede_download_mb_m",
                "rede_upload_mil_pacotes_m", "rede_download_mil_pacotes_m",
                "rede_perda_pacotes_local", "rede_perda_pacotes_pct",
                "rede_established", "rede_time_wait", "rede_close_wait", "rede_conn_total",
                "rede_established_pct", "rede_time_wait_pct", "rede_close_wait_pct",
                "globalping_latencia_ms", "globalping_perda_pct", "globalping_perda_pacotes",
            ]),
        },
    }

    return {
        "data_referencia": data_ref,
        "atual": gold_atual,
        "dashboard": gold_dashboard,
        "historico": gold_historico,
    }


def montar_gold(df, processos_ram=None):
    """Compatibilidade retroativa: retorna o documento dashboard estruturado."""
    proc = {"cpu_top5": [], "ram": processos_ram or {"maior_uso": [], "maior_crescimento": []}}
    return montar_gold_estruturado(df, processos=proc)["dashboard"]


# EXECUÇÃO AWS LAMBDA (OPÇÃO A: ORIENTADA APENAS A *_sistema.csv NO BUCKET BRONZE)


def _limpar_dir_temporario():
    """Limpa o diretório /tmp usado pelo ETL entre invocações warm do Lambda."""
    if DIR_BASE.startswith("/tmp") and os.path.isdir(DIR_BASE):
        shutil.rmtree(DIR_BASE, ignore_errors=True)


def _baixar_objeto_s3_opcional(s3, bucket, chave, destino):
    """Baixa um objeto do S3 para destino. Retorna False se o objeto não existir."""
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    try:
        s3.download_file(bucket, chave, destino)
        print(f"  [S3 DOWNLOAD] s3://{bucket}/{chave} -> {destino}")
        return True
    except ClientError as erro:
        codigo = erro.response.get("Error", {}).get("Code", "")
        if codigo in ("NoSuchKey", "404", "NotFound"):
            return False
        raise


def _resolver_chaves_evento_bronze(chave_s3):
    """
    Opção A — Eliminação de duplicidade:
    Dispara processamento SOMENTE em *_sistema.csv (o coletor sobe auxiliares antes e
    _sistema.csv por último; no S3 configure o gatilho com Suffix=_sistema.csv).
    Retorna: (chave_sistema, chave_processos, chave_particoes, motivo).
    """
    if not chave_s3 or not chave_s3.lower().endswith(".csv"):
        return None, None, None, "nao_csv"

    nome_arq = os.path.basename(chave_s3)
    prefixo = os.path.dirname(chave_s3)

    if nome_arq.endswith(("_particoes.csv", "_conexoes_rede.csv", "_processos.csv")):
        return None, None, None, "arquivo_auxiliar_ignorado"

    if nome_arq.endswith("_sistema.csv"):
        nome_proc = nome_arq.replace("_sistema.csv", "_processos.csv")
        nome_part = nome_arq.replace("_sistema.csv", "_particoes.csv")
        chave_proc = f"{prefixo}/{nome_proc}" if prefixo else nome_proc
        chave_part = f"{prefixo}/{nome_part}" if prefixo else nome_part
        return chave_s3, chave_proc, chave_part, "sistema"

    return chave_s3, None, None, "csv_generico"


def processar_arquivo_bronze_s3(s3, bucket_bronze, chave_s3, mapa_bd, bucket_silver, bucket_gold):
    """Processa um *_sistema.csv do Bucket Bronze e grava Silver + Gold leve (atual, dashboard, historico/dia)."""
    chave_sistema, chave_processos, chave_particoes, tipo = _resolver_chaves_evento_bronze(chave_s3)
    if not chave_sistema:
        print(f"[LAMBDA] Ignorando objeto '{chave_s3}' ({tipo}).")
        return {"chave": chave_s3, "status": "ignorado", "motivo": tipo}

    destino_sistema = os.path.join(DIR_BRONZE, chave_sistema.lstrip("/"))
    baixou_sistema = _baixar_objeto_s3_opcional(s3, bucket_bronze, chave_sistema, destino_sistema)
    if not baixou_sistema:
        print(f"[LAMBDA] Arquivo de sistema '{chave_sistema}' não encontrado no Bronze.")
        return {"chave": chave_s3, "status": "ignorado", "motivo": "sistema_nao_encontrado"}

    caminhos_processos = []
    if chave_processos:
        destino_proc = os.path.join(DIR_BRONZE, chave_processos.lstrip("/"))
        if _baixar_objeto_s3_opcional(s3, bucket_bronze, chave_processos, destino_proc):
            caminhos_processos.append(destino_proc)

    caminhos_particoes = []
    if chave_particoes:
        destino_part = os.path.join(DIR_BRONZE, chave_particoes.lstrip("/"))
        if _baixar_objeto_s3_opcional(s3, bucket_bronze, chave_particoes, destino_part):
            caminhos_particoes.append(destino_part)

    df_lido = ler_bronze(destino_sistema)
    if df_lido.empty:
        print(f"[LAMBDA] Nenhuma linha válida encontrada em '{chave_sistema}'.")
        return {"chave": chave_s3, "status": "ignorado", "motivo": "csv_vazio_ou_invalido"}

    os.makedirs(DIR_SILVER, exist_ok=True)
    os.makedirs(DIR_GOLD, exist_ok=True)

    prefixo_empresa = chave_sistema.split("/", 1)[0] if "/" in chave_sistema else None
    data_chave = _extrair_data_caminho(chave_sistema)
    proc_dados = processar_processos(caminhos_processos)
    part_dados = processar_particoes(caminhos_particoes)
    maquinas_processadas = []

    for ident_raw, df_bronze in df_lido.groupby("identifier"):
        identificador = normalizar_identificador(ident_raw)
        if not identificador:
            continue

        df_bronze = (
            df_bronze.sort_values("_ts")
            .drop_duplicates(subset=["identifier", "_ts"], keep="last")
            .reset_index(drop=True)
        )

        # Via 2: consulta autoritativa na API Express (com fallback seguro para BD/Bronze)
        empresa = resolver_metadados_empresa(
            identificador=identificador,
            df_bronze=df_bronze,
            mapa_bd=mapa_bd,
            prefixo_empresa=prefixo_empresa,
        )

        df_tratado = transformar_dataframe(df_bronze, empresa)
        if df_tratado.empty:
            continue

        nome = nome_seguro(identificador)
        pasta_empresa = nome_seguro(empresa["empresa_id"])
        data_ref = data_chave or str(df_tratado["timestamp"].iloc[-1])[:10]

        # 1. Silver -> S3 Silver (<empresa>/<id>/<YYYY-MM-DD>/<id>_sistema.csv)
        caminho_silver = gravar_silver(pasta_empresa, nome, df_tratado, data_referencia=data_ref)
        chave_s3_silver = f"{pasta_empresa}/{nome}/{data_ref}/{os.path.basename(caminho_silver)}"
        enviar_para_s3(caminho_silver, chave_s3_silver, bucket=bucket_silver, propagar_erro=True)

        # 2. Gold leve estruturado -> S3 Gold (sem escrita concorrente em índice global)
        pacote_gold = montar_gold_estruturado(
            df_tratado,
            processos=proc_dados,
            particoes=part_dados,
            id_instancia=empresa.get("id_instancia"),
            data_referencia=data_ref,
        )

        chave_gold_atual = f"{pasta_empresa}/{nome}/atual.json"
        chave_gold_dashboard = f"{pasta_empresa}/{nome}/dashboard.json"
        chave_gold_historico = f"{pasta_empresa}/{nome}/historico/{data_ref}.json"

        enviar_para_s3(None, chave_gold_atual, bucket=bucket_gold, documento=pacote_gold["atual"], propagar_erro=True)
        enviar_para_s3(None, chave_gold_dashboard, bucket=bucket_gold, documento=pacote_gold["dashboard"], propagar_erro=True)
        enviar_para_s3(None, chave_gold_historico, bucket=bucket_gold, documento=pacote_gold["historico"], propagar_erro=True)

        maquinas_processadas.append({
            "identificador": identificador,
            "id_instancia": empresa.get("id_instancia"),
            "empresa_id": empresa["empresa_id"],
            "data_referencia": data_ref,
            "registros": len(df_tratado),
            "silver": f"s3://{bucket_silver}/{chave_s3_silver}",
            "gold_atual": f"s3://{bucket_gold}/{chave_gold_atual}",
            "gold_dashboard": f"s3://{bucket_gold}/{chave_gold_dashboard}",
            "gold_historico": f"s3://{bucket_gold}/{chave_gold_historico}",
        })
        print(f"  [LAMBDA OK] {identificador} ({empresa['empresa_nome']} | {data_ref}): {len(df_tratado)} registros processados.")

    return {
        "chave": chave_s3,
        "status": "processado",
        "maquinas": maquinas_processadas,
    }


def lambda_handler(event, context):
    """
    Handler AWS Lambda (Python 3.12) acionado por eventos S3 (ObjectCreated, Suffix=_sistema.csv)
    no Bucket Bronze.
    """
    bucket_silver = os.getenv("AWS_BUCKET_SILVER", AWS_BUCKET_SILVER).strip()
    bucket_gold = os.getenv("AWS_BUCKET_GOLD", AWS_BUCKET_GOLD).strip()

    if not bucket_silver or not bucket_gold:
        raise ValueError("Configure as variáveis de ambiente AWS_BUCKET_SILVER e AWS_BUCKET_GOLD no Lambda.")
    if bucket_silver == bucket_gold:
        raise ValueError("AWS_BUCKET_SILVER e AWS_BUCKET_GOLD devem ser buckets distintos.")

    s3 = obter_cliente_s3()
    if s3 is None:
        raise RuntimeError("Biblioteca boto3 não disponível no ambiente Lambda.")

    registros = (event or {}).get("Records", [])
    if not registros:
        print("[LAMBDA] Evento sem Records S3 recebido.")
        return {
            "statusCode": 200,
            "body": json.dumps({"mensagem": "Nenhum registro S3 para processar.", "resultados": []}, ensure_ascii=False),
        }

    _limpar_dir_temporario()
    resultados = []
    chaves_processadas = set()

    try:
        # Via 2 usa API_BASE_URL por instância; carregar_maquinas_bd só atua se DB_HOST estiver definido
        mapa_bd = carregar_maquinas_bd()

        for record in registros:
            s3_info = record.get("s3", {})
            bucket_bronze = s3_info.get("bucket", {}).get("name", "").strip()
            chave_raw = s3_info.get("object", {}).get("key", "")
            chave_s3 = unquote_plus(chave_raw)

            if not bucket_bronze or not chave_s3:
                resultados.append({"status": "ignorado", "motivo": "registro_incompleto"})
                continue

            if bucket_bronze in (bucket_silver, bucket_gold):
                raise ValueError(
                    f"Bucket de origem ({bucket_bronze}) não pode ser igual ao Silver ou Gold (evita loop infinito)."
                )

            chave_sistema, _, _, motivo = _resolver_chaves_evento_bronze(chave_s3)
            if not chave_sistema:
                resultados.append({"chave": chave_s3, "status": "ignorado", "motivo": motivo})
                continue

            chave_dedup = (bucket_bronze, chave_sistema)
            if chave_dedup in chaves_processadas:
                resultados.append({"chave": chave_s3, "status": "ignorado", "motivo": "duplicado_no_lote"})
                continue
            chaves_processadas.add(chave_dedup)

            res = processar_arquivo_bronze_s3(
                s3=s3,
                bucket_bronze=bucket_bronze,
                chave_s3=chave_s3,
                mapa_bd=mapa_bd,
                bucket_silver=bucket_silver,
                bucket_gold=bucket_gold,
            )
            resultados.append(res)
    finally:
        _limpar_dir_temporario()

    return {
        "statusCode": 200,
        "body": json.dumps({"resultados": resultados}, ensure_ascii=False),
    }


# EXECUÇÃO LOCAL (CLI)

def main():
    if AWS_BAIXAR or AWS_UPLOAD:
        configurados = [b for b in (AWS_BUCKET_BRONZE, AWS_BUCKET_SILVER, AWS_BUCKET_GOLD) if b]
        if len(configurados) != len(set(configurados)):
            raise ValueError("Configure buckets distintos para Bronze, Silver e Gold.")
        if (AWS_BAIXAR and not AWS_BUCKET_BRONZE) or (AWS_UPLOAD and not (AWS_BUCKET_SILVER and AWS_BUCKET_GOLD)):
            raise ValueError("Bucket ausente para a operação S3 habilitada.")
    # 1. Download do S3 se ativado
    baixar_bronze_s3()

    if not os.path.isdir(DIR_BRONZE):
        print(f"[ERRO] Diretório bronze não encontrado: {DIR_BRONZE}")
        return

    arquivos = sorted(glob.glob(os.path.join(DIR_BRONZE, "**", "*.csv"), recursive=True))
    if not arquivos:
        print(f"[AVISO] Nenhum CSV encontrado em {DIR_BRONZE}")
        return

    os.makedirs(DIR_SILVER, exist_ok=True)
    os.makedirs(DIR_GOLD, exist_ok=True)

    mapa_bd = carregar_maquinas_bd()

    dfs_por_maquina_dia = {}
    arquivos_processos = {}
    arquivos_particoes = {}

    for caminho in arquivos:
        nome_base = os.path.basename(caminho)
        data_arq = _extrair_data_caminho(caminho)

        if nome_base.endswith("_conexoes_rede.csv"):
            continue
        if nome_base.endswith("_particoes.csv"):
            chave_id = normalizar_identificador(nome_base.replace("_particoes.csv", ""))
            if chave_id:
                arquivos_particoes.setdefault((chave_id, data_arq), []).append(caminho)
            continue
        if nome_base.endswith("_processos.csv"):
            chave_id = normalizar_identificador(nome_base.replace("_processos.csv", ""))
            if chave_id:
                arquivos_processos.setdefault((chave_id, data_arq), []).append(caminho)
            continue

        print(f"Lendo {caminho}")
        df_lido = ler_bronze(caminho)
        if df_lido.empty:
            continue

        rel = os.path.relpath(caminho, DIR_BRONZE).replace("\\", "/")
        prefixo_emp = rel.split("/", 1)[0] if "/" in rel else None

        for ident_raw, grupo_df in df_lido.groupby("identifier"):
            chave_id = normalizar_identificador(ident_raw)
            if not chave_id:
                continue
            dia_ref = data_arq or str(grupo_df["_ts"].iloc[-1].strftime("%Y-%m-%d"))
            dfs_por_maquina_dia.setdefault((chave_id, dia_ref), {"dfs": [], "prefixo": prefixo_emp})["dfs"].append(grupo_df)

    if not dfs_por_maquina_dia:
        print("[AVISO] Nenhuma linha válida encontrada nos arquivos bronze.")
        return

    maquinas_unicas = set()
    empresas_unicas = set()

    for (identificador, data_ref), bloco in sorted(dfs_por_maquina_dia.items()):
        df_bronze = pd.concat(bloco["dfs"], ignore_index=True)
        df_bronze = df_bronze.sort_values("_ts").drop_duplicates(subset=["identifier", "_ts"], keep="last").reset_index(drop=True)

        empresa = resolver_metadados_empresa(
            identificador=identificador,
            df_bronze=df_bronze,
            mapa_bd=mapa_bd,
            prefixo_empresa=bloco["prefixo"],
        )

        df_tratado = transformar_dataframe(df_bronze, empresa)
        if df_tratado.empty:
            continue

        nome = nome_seguro(identificador)
        pasta_empresa = nome_seguro(empresa["empresa_id"])

        # Silver
        caminho_silver = gravar_silver(pasta_empresa, nome, df_tratado, data_referencia=data_ref)
        chave_s3_silver = f"{pasta_empresa}/{nome}/{data_ref}/{os.path.basename(caminho_silver)}"
        enviar_para_s3(caminho_silver, chave_s3_silver, bucket=AWS_BUCKET_SILVER)

        # Auxiliares (processos e partições do dia ou legado sem data)
        caminhos_proc = arquivos_processos.get((identificador, data_ref)) or arquivos_processos.get((identificador, None), [])
        caminhos_part = arquivos_particoes.get((identificador, data_ref)) or arquivos_particoes.get((identificador, None), [])
        proc_dados = processar_processos(caminhos_proc)
        part_dados = processar_particoes(caminhos_part)

        # Gold leve estruturado
        pacote_gold = montar_gold_estruturado(
            df_tratado,
            processos=proc_dados,
            particoes=part_dados,
            id_instancia=empresa.get("id_instancia"),
            data_referencia=data_ref,
        )

        dir_gold_maq = os.path.join(DIR_GOLD, pasta_empresa, nome)
        dir_gold_hist = os.path.join(dir_gold_maq, "historico")
        os.makedirs(dir_gold_hist, exist_ok=True)

        caminho_atual = os.path.join(dir_gold_maq, "atual.json")
        caminho_dashboard = os.path.join(dir_gold_maq, "dashboard.json")
        caminho_historico = os.path.join(dir_gold_hist, f"{data_ref}.json")

        for caminho_arq, doc, chave_s3 in (
            (caminho_atual, pacote_gold["atual"], f"{pasta_empresa}/{nome}/atual.json"),
            (caminho_dashboard, pacote_gold["dashboard"], f"{pasta_empresa}/{nome}/dashboard.json"),
            (caminho_historico, pacote_gold["historico"], f"{pasta_empresa}/{nome}/historico/{data_ref}.json"),
        ):
            with open(caminho_arq, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            enviar_para_s3(caminho_arq, chave_s3, bucket=AWS_BUCKET_GOLD, documento=doc)

        maquinas_unicas.add((pasta_empresa, nome))
        empresas_unicas.add(pasta_empresa)
        print(f"  OK {identificador} ({empresa['empresa_nome']} | {data_ref}): {len(df_tratado)} registros -> {caminho_silver} | {dir_gold_maq}")

    print(f"Concluído: {len(maquinas_unicas)} máquina(s) de {len(empresas_unicas)} empresa(s) processada(s).")


if __name__ == "__main__":
    main()
