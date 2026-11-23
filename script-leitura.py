"""
==========================================================================================
 TRACK COMMERCE - ETL DE MÉTRICAS (BRONZE -> SILVER / GOLD) - VERSÃO CORRIGIDA
==========================================================================================

 Entrada : ./track-commerce/bronze/*.csv (ou via S3 Bucket)
 Saída   :
   SILVER (Análise R - inclui empresa_id e empresa_nome em todas as linhas e caminhos)
     ./track-commerce/silver/<empresa_id>/<identificador>_sistema.csv
   GOLD   (JSON estruturado com histórico completo)
     ./track-commerce/gold/<empresa_id>/<identificador>_sistema.json
     ./track-commerce/gold/<empresa_id>/indice_maquinas.json
     ./track-commerce/gold/indice_maquinas.json

Usa pandas + python-dotenv + mysql.connector (opcional BD) + boto3 (opcional S3).
==========================================================================================
"""



import glob
import json
import os
from datetime import datetime


import mysql.connector
import pandas as pd
from dotenv import load_dotenv

try:
    import boto3
except ImportError:
    boto3 = None

# ==========================================================================================
# CONFIGURAÇÕES E INTEGRAÇÕES
# ==========================================================================================
# Resolver .env relativo ao diretório do script antes de ler variáveis de ambiente
BASE_DIR_PROJETO = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR_PROJETO, ".env"))

# permitir override por variável de ambiente
DIR_BASE = os.getenv("TRACKCOMMERCE_DIR") or os.path.join(BASE_DIR_PROJETO, "track-commerce")
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

# S3 Download / Upload (Opcional)
AWS_BAIXAR = os.getenv("AWS_BAIXAR", "False").lower() in ("true", "1", "t", "yes")
AWS_UPLOAD = os.getenv("AWS_UPLOAD", "False").lower() in ("true", "1", "t", "yes")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
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

# AWS S3 E BANCO DE DADOS

def obter_cliente_s3():
    if not (boto3 and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        return None
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN or None,
        region_name=AWS_REGION,
    )

def baixar_bronze_s3():
    """Baixa automaticamente os CSVs do S3 para a pasta local bronze antes do processamento."""
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
                # esperar formato <empresa>/<arquivo>.csv (empresa pode ser id ou nome)
                if "/" in key:
                    empresa_id, nome_arquivo = key.split("/", 1)
                    pasta_destino = os.path.join(DIR_BRONZE, empresa_id)
                    os.makedirs(pasta_destino, exist_ok=True)
                    destino = os.path.join(pasta_destino, nome_arquivo)
                else:
                    nome_arquivo = os.path.basename(key)
                    destino = os.path.join(DIR_BRONZE, nome_arquivo)
                try:
                    s3.download_file(AWS_BUCKET_BRONZE, key, destino)
                    print(f"  [S3 DOWNLOAD] Baixado: {key} -> {destino}")
                except Exception as err:
                    print(f"  [AVISO S3 DOWNLOAD] Falha ao baixar {key}: {err}")
    except Exception as erro:
        print(f"  [ERRO S3 DOWNLOAD] Falha ao sincronizar com S3: {erro}")

def enviar_para_s3(caminho_local, chave_s3, bucket, documento=None):
    """Faz o upload de um arquivo local para o bucket S3."""
    if not AWS_UPLOAD:
        return
    
    s3 = obter_cliente_s3()
    if not s3:
        return

    try:
        if documento is None:
            s3.upload_file(caminho_local, bucket, chave_s3)
        else:
            s3.put_object(Bucket=bucket, Key=chave_s3, ContentType="application/json",
                          Body=json.dumps(documento, ensure_ascii=False).encode("utf-8"))
        print(f"  [S3 UPLOAD] Enviado: {chave_s3}")
    except Exception as erro:
        print(f"  [ERRO S3 UPLOAD] Falha ao enviar {caminho_local}: {erro}")


def carregar_maquinas_bd():
    """Busca no BD o vínculo máquina -> empresa. Se inacessível, retorna {}."""
    try:
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        # aceitar nomes alternativos usados em outros componentes
        db_pass = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME") or os.getenv("DB_DATABASE")
        conexao = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
        )
    except mysql.connector.Error as erro:
        print(f"[AVISO BD] Indisponível, utilizando dados de empresa do Bronze: {erro}")
        return {}
    try:
        cursor = conexao.cursor(dictionary=True)
        cursor.execute('''
            SELECT i.identificador AS identifier, i.nome AS vm_name,
                   e.id_empresa AS enterprise_identifier, e.razao_social AS social_reason
            FROM empresa AS e
            INNER JOIN instancia AS i ON i.fk_empresa = e.id_empresa;''')
        mapa = {}
        for r in cursor.fetchall():
            ident = normalizar_identificador(r["identifier"])
            if ident:
                mapa[ident] = {"empresa_id": para_texto(r["enterprise_identifier"]),
                               "empresa_nome": para_texto(r["social_reason"]),
                               "vm_nome": para_texto(r["vm_name"])}
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
    try:
        df = pd.read_csv(caminho, sep=None, engine="python", encoding="utf-8", on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()

    df.columns = [c.strip() for c in df.columns]
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
        "disco_iops_leitura": taxa("disk_read_count"),
        "disco_iops_escrita": taxa("disk_write_count"),
        "disco_iops_total": (d_io_total / intervalo).round(2),
        "disco_latencia_leitura_ms": (d_t_leitura / d_leituras.replace(0, float("nan"))).round(3),
        "disco_latencia_escrita_ms": (d_t_escrita / d_escritas.replace(0, float("nan"))).round(3),
        "disco_latencia_ms": (d_t_total / d_io_total.replace(0, float("nan"))).round(3),

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
        "declared_monthly_cost": pd.to_numeric(df["declared_monthly_cost"], errors="coerce") if "declared_monthly_cost" in df.columns else None,
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


def processar_processos_ram(caminhos_csv):
    dfs = []
    for caminho in caminhos_csv:
        if os.path.isfile(caminho) and os.path.getsize(caminho) > 0:
            try:
                dfs.append(pd.read_csv(caminho, sep=";", encoding="utf-8", on_bad_lines="skip"))
            except Exception:
                continue
    if not dfs:
        return {"maior_uso": [], "maior_crescimento": []}

    df = pd.concat(dfs, ignore_index=True)
    df["_ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["memory_rss_mb"] = pd.to_numeric(df["memory_rss_mb"], errors="coerce")
    df["memory_percent"] = pd.to_numeric(df["memory_percent"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["pid", "_ts", "memory_rss_mb"]).sort_values("_ts")

    if df.empty:
        return {"maior_uso": [], "maior_crescimento": []}

    ultimo_ts = df["_ts"].max()
    agrupado = df.groupby("pid").agg(
        processo=("name", "last"),
        ts_ini=("_ts", "first"),
        ts_fim=("_ts", "last"),
        rss_ini=("memory_rss_mb", "first"),
        rss_fim=("memory_rss_mb", "last"),
        ram_percent=("memory_percent", "last"),
    ).reset_index()

    agrupado = agrupado[(ultimo_ts - agrupado["ts_fim"]).dt.total_seconds() <= 60]
    horas = (agrupado["ts_fim"] - agrupado["ts_ini"]).dt.total_seconds() / 3600.0
    agrupado["tendencia_mb_h"] = ((agrupado["rss_fim"] - agrupado["rss_ini"]) / horas).where(horas > 0.001, 0.0).round(1)
    agrupado["ram_mb"] = agrupado["rss_fim"].round(1)
    agrupado["ram_percent"] = agrupado["ram_percent"].round(1)
    agrupado["pid"] = agrupado["pid"].astype(str)

    cols = ["pid", "processo", "ram_mb", "ram_percent", "tendencia_mb_h"]
    return {
        "maior_uso": agrupado.nlargest(5, "ram_mb")[cols].to_dict(orient="records"),
        "maior_crescimento": agrupado.nlargest(5, "tendencia_mb_h")[cols].to_dict(orient="records"),
    }


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


# SILVER (CSV) E GOLD (JSON)

def gravar_silver(pasta_empresa, identificador, df):
    diretorio = os.path.join(DIR_SILVER, pasta_empresa)
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


def montar_gold(df, processos_ram=None):
    ult = lambda c: ultimo_valido(df, c)
    est = lambda c: estatisticas_serie(df[c]) if c in df.columns else estatisticas_serie(pd.Series(dtype="float64"))
    df_15m_rede = agregar_janelas_15m_rede(df)
    janelas_rede = montar_janelas_15m_gold_rede(df_15m_rede)
    tendencias_rede = calcular_tendencias_rede(df, df_15m_rede)
    previsibilidade_rede = calcular_previsao_rede(df, df_15m_rede)
    ult_15m = (lambda c: ultimo_valido(df_15m_rede, c) if not df_15m_rede.empty else ult(c))

    return {
        "empresa": {
            "id": ult("empresa_id"),
            "nome": ult("empresa_nome"),
        },
        "maquina": {
            "identifier": ult("identifier"),
            "nome": ult("vm_nome"),
            "hostname": ult("hostname"),
            "os": ult("os"),
            "primeiro_registro": str(df["timestamp"].iloc[0]),
            "ultimo_registro": str(df["timestamp"].iloc[-1]),
            "total_registros": int(len(df)),
            "janela_desvio_padrao": JANELA_DESVIO,
        },
        "cpu": {
            "atual": {
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
            },
            "resumo": {"uso_percent": est("cpu_percent"), "load_1m": est("load_1m")},
            "historico": historico(df, [
                "cpu_percent", "cpu_por_nucleo", "load_1m", "load_5m", "load_15m",
                "cpu_user_pct", "cpu_sys_pct", "cpu_nice_pct", "cpu_idle_pct",
                "cpu_iowait_pct", "qtd_processos",
            ]),
        },
        "ram": {
            "atual": {
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
            },
            "resumo": {"uso_percent": est("ram_percent")},
            "previsao": calcular_previsao_ram(df),
            "alertas_24h": contar_alertas_ram_24h(df),
            "proximo_pico": calcular_proximo_pico(),
            "processos": processos_ram or {"maior_uso": [], "maior_crescimento": []},
            "historico": historico(df, [
                "ram_percent", "ram_usada_gb", "ram_disponivel_gb", "ram_cached_gb",
                "ram_buffers_gb", "ram_shared_gb", "ram_slab_gb", "ram_buff_cache_usado_gb",
            ]),
        },
        "disco": {
            "atual": {
                "uso_percent": ult("disco_percent"),
                "total_gb": ult("disco_total_gb"),
                "usado_gb": ult("disco_usado_gb"),
                "livre_gb": ult("disco_livre_gb"),
                "leitura_bytes_s": ult("disco_leitura_bytes_s"),
                "escrita_bytes_s": ult("disco_escrita_bytes_s"),
                "iops": {
                    "leitura": ult("disco_iops_leitura"),
                    "escrita": ult("disco_iops_escrita"),
                    "total": ult("disco_iops_total"),
                },
                "latencia_ms": {
                    "leitura": ult("disco_latencia_leitura_ms"),
                    "escrita": ult("disco_latencia_escrita_ms"),
                    "media": ult("disco_latencia_ms"),
                },
            },
            "resumo": {
                "uso_percent": est("disco_percent"),
                "iops_total": est("disco_iops_total"),
                "latencia_ms": est("disco_latencia_ms"),
            },
            "historico": historico(df, [
                "disco_percent", "disco_leitura_bytes_s", "disco_escrita_bytes_s",
                "disco_iops_leitura", "disco_iops_escrita", "disco_iops_total",
                "disco_latencia_leitura_ms", "disco_latencia_escrita_ms", "disco_latencia_ms",
            ]),
        },
        "swap": {
            "atual": {
                "uso_percent": ult("swap_percent"),
                "total_gb": ult("swap_total_gb"),
                "disponivel_gb": ult("swap_disponivel_gb"),
                "usada_gb": ult("swap_usada_gb"),
                "sin_mb_s": ult("swap_sin_mb_s"),
                "sout_mb_s": ult("swap_sout_mb_s"),
            },
            "resumo": {"uso_percent": est("swap_percent")},
            "historico": historico(df, ["swap_percent", "swap_usada_gb", "swap_sin_mb_s", "swap_sout_mb_s"]),
        },
        "rede": {
            "atual": {
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
            },
            "globalping": {
                "regiao": ult("globalping_regiao") or "BR",
                "target_host": ult("globalping_target") or ult("ec2_public_ip"),
                "latencia_externa_ms": ult("globalping_latencia_ms"),
                "perda_externa_pacotes": ult("globalping_perda_pacotes"),
                "perda_externa_pct": ult("globalping_perda_pct"),
                "latencia_externa_graf": janelas_rede["latencia_externa_ms"],
                "perda_externa_pacotes_graf": janelas_rede["perda_externa_pacotes"],
            },
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
            "historico": historico(df, [
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

# EXECUÇÃO

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

    dfs_por_maquina = {}
    arquivos_processos = {}

    for caminho in arquivos:
        if "_particoes.csv" in caminho or "_conexoes_rede.csv" in caminho:
            continue
        if "_processos.csv" in caminho:
            nome_arq = os.path.basename(caminho).replace("_processos.csv", "")
            chave = normalizar_identificador(nome_arq)
            if chave:
                arquivos_processos.setdefault(chave, []).append(caminho)
            continue

        print(f"Lendo {caminho}")
        df_lido = ler_bronze(caminho)
        if df_lido.empty:
            continue

        for ident_raw, grupo_df in df_lido.groupby("identifier"):
            chave = normalizar_identificador(ident_raw)
            if chave:
                dfs_por_maquina.setdefault(chave, []).append(grupo_df)

    if not dfs_por_maquina:
        print("[AVISO] Nenhuma linha válida encontrada nos arquivos bronze.")
        return

    indice = {}
    for identificador, lista_dfs in dfs_por_maquina.items():
        df_bronze = pd.concat(lista_dfs, ignore_index=True)
        df_bronze = df_bronze.sort_values("_ts").drop_duplicates(subset=["identifier", "_ts"], keep="last").reset_index(drop=True)

        empresa_id = (
            mapa_bd.get(identificador, {}).get("empresa_id")
            or ultimo_valido_bronze(df_bronze, "enterprise_identifier", "empresa_id")
            or "sem_empresa"
        )
        empresa_nome = (
            mapa_bd.get(identificador, {}).get("empresa_nome")
            or ultimo_valido_bronze(df_bronze, "enterprise_reason", "empresa_nome")
            or "Empresa Desconhecida"
        )
        vm_nome = mapa_bd.get(identificador, {}).get("vm_nome")

        empresa = {
            "empresa_id": empresa_id,
            "empresa_nome": empresa_nome,
            "vm_nome": vm_nome,
        }

        df_tratado = transformar_dataframe(df_bronze, empresa)
        if df_tratado.empty:
            continue

        nome = nome_seguro(identificador)
        pasta_empresa = nome_seguro(empresa_id)

        # Silver
        caminho_silver = gravar_silver(pasta_empresa, nome, df_tratado)
        chave_s3_silver = f"{pasta_empresa}/{os.path.basename(caminho_silver)}"
        enviar_para_s3(caminho_silver, chave_s3_silver, bucket=AWS_BUCKET_SILVER)

        # Gold
        proc_ram = processar_processos_ram(arquivos_processos.get(identificador, []))
        gold = montar_gold(df_tratado, processos_ram=proc_ram)

        dir_gold_empresa = os.path.join(DIR_GOLD, pasta_empresa)
        os.makedirs(dir_gold_empresa, exist_ok=True)
        caminho_gold = os.path.join(dir_gold_empresa, f"{nome}_sistema.json")

        with open(caminho_gold, "w", encoding="utf-8") as f:
            json.dump(gold, f, ensure_ascii=False, indent=2)

        chave_s3_gold_maq = f"{pasta_empresa}/{os.path.basename(caminho_gold)}"
        enviar_para_s3(caminho_gold, chave_s3_gold_maq, bucket=AWS_BUCKET_GOLD)

        grupo = indice.setdefault(pasta_empresa, {
            "empresa_id": empresa_id,
            "empresa_nome": empresa_nome,
            "maquinas": [],
        })
        grupo["maquinas"].append({
            **gold["maquina"],
            "arquivo": f"{pasta_empresa}/{os.path.basename(caminho_gold)}",
            "cpu_percent": gold["cpu"]["atual"]["uso_percent"],
            "ram_percent": gold["ram"]["atual"]["uso_percent"],
            "disco_percent": gold["disco"]["atual"]["uso_percent"],
        })
        print(f"  OK {identificador} ({empresa_nome}): {len(df_tratado)} registros -> {caminho_silver} | {caminho_gold}")

    grupos_s3 = []
    for pasta_empresa, grupo in indice.items():
        caminho_indice_emp = os.path.join(DIR_GOLD, pasta_empresa, "indice_maquinas.json")
        with open(caminho_indice_emp, "w", encoding="utf-8") as f:
            json.dump(grupo, f, ensure_ascii=False, indent=2)

        grupo_s3 = {**grupo, "maquinas": [
            {**m, "arquivo": os.path.basename(m["arquivo"])} for m in grupo["maquinas"]
        ]}
        grupos_s3.append(grupo_s3)
        enviar_para_s3(caminho_indice_emp, f"indice_maquinas_empresa_{pasta_empresa}.json",
                       AWS_BUCKET_GOLD, documento=grupo_s3)

    caminho_indice_global = os.path.join(DIR_GOLD, "indice_maquinas.json")
    with open(caminho_indice_global, "w", encoding="utf-8") as f:
        json.dump({"empresas": list(indice.values())}, f, ensure_ascii=False, indent=2)

    enviar_para_s3(caminho_indice_global, "indice_maquinas.json", AWS_BUCKET_GOLD,
                   documento={"empresas": grupos_s3})

    total = sum(len(g["maquinas"]) for g in indice.values())
    print(f"Concluído: {total} máquina(s) de {len(indice)} empresa(s) processada(s).")


if __name__ == "__main__":
    main()
