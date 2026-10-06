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

 Usa biblioteca padrão do Python + mysql.connector (opcional BD) + boto3 (opcional S3).
==========================================================================================
"""

import csv
import glob
import json
import math
import os
import statistics
from collections import deque
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv

try:
    import boto3
except ImportError:
    boto3 = None

# ==========================================================================================
# CONFIGURAÇÕES E INTEGRAÇÕES
# ==========================================================================================
load_dotenv()

DIR_BASE = "./track-commerce"
DIR_BRONZE = os.path.join(DIR_BASE, "bronze")
DIR_SILVER = os.path.join(DIR_BASE, "silver")
DIR_GOLD = os.path.join(DIR_BASE, "gold")

DELIMITADOR_SAIDA = ";"
FORMATO_TIMESTAMP = "%Y-%m-%d %H:%M:%S"
JANELA_DESVIO = 60
BYTES_POR_GB = 1024 ** 3

COLUNAS_OBRIGATORIAS = ["identifier", "timestamp"]

# S3 Download / Upload (Opcional)
AWS_BAIXAR = os.getenv("AWS_BAIXAR", "False").lower() in ("true", "1", "t", "yes")
AWS_BUCKET = os.getenv("AWS_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")
AWS_PREFIXO = "track-commerce/bronze"


def baixar_bronze_s3():
    """Baixa automaticamente os CSVs do S3 para a pasta local bronze antes do processamento."""
    if not (AWS_BAIXAR and boto3 and AWS_BUCKET and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        return
    print("[S3 ETL] Iniciando busca por arquivos no Bucket S3...")
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN or None,
            region_name=AWS_REGION,
        )
        os.makedirs(DIR_BRONZE, exist_ok=True)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=AWS_BUCKET, Prefix=AWS_PREFIXO):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv"):
                    nome_arquivo = os.path.basename(key)
                    destino = os.path.join(DIR_BRONZE, nome_arquivo)
                    s3.download_file(AWS_BUCKET, key, destino)
                    print(f"  [S3 ETL] Baixado: {key} -> {destino}")
    except Exception as erro:
        print(f"  [AVISO ETL] Falha ao sincronizar com S3: {erro}")


def carregar_maquinas_bd():
    """Busca no BD o vínculo máquina -> empresa. Se inacessível, retorna {}."""
    try:
        conexao = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
        )
    except mysql.connector.Error as erro:
        print(f"[AVISO BD] Indisponível, utilizando dados de empresa do Bronze: {erro}")
        return {}
    try:
        cursor = conexao.cursor(dictionary=True)
        cursor.execute('''
            SELECT i.identificador AS identifier, i.nome AS vm_name,
                   e.id_empresa AS enterprise_identifier, e.razao_social AS social_reason
            FROM Empresa AS e
            INNER JOIN Instancias AS i ON i.fk_empresa = e.id_empresa;''')
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


def normalizar_identificador(valor):
    return para_texto(valor).lower().replace("-", "").replace("_", "") if valor else ""


# ==========================================================================================
# UTILITÁRIOS
# ==========================================================================================

def para_float(valor):
    if valor is None:
        return None
    texto = str(valor).strip().replace(",", ".")
    if texto == "" or texto.lower() in ("none", "nan", "null"):
        return None
    if texto.lower() == "true":
        return 1.0
    if texto.lower() == "false":
        return 0.0
    try:
        numero = float(texto)
    except ValueError:
        return None
    if math.isnan(numero) or math.isinf(numero):
        return None
    return numero


def para_percentual(valor):
    numero = para_float(valor)
    return numero if numero is not None and 0 <= numero <= 100 else None


def para_positivo(valor):
    numero = para_float(valor)
    return numero if numero is not None and numero >= 0 else None


def para_texto(valor):
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto if texto and texto.lower() not in ("none", "null", "nan") else None


def arredondar(valor, casas=2):
    return None if valor is None else round(valor, casas)


def para_gb(valor_bytes):
    return None if valor_bytes is None else round(valor_bytes / BYTES_POR_GB, 3)


def dividir(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


def somar(*valores):
    validos = [v for v in valores if v is not None]
    return sum(validos) if validos else None


def ler_timestamp(texto):
    texto = para_texto(texto)
    if not texto:
        return None
    for formato in (FORMATO_TIMESTAMP, "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    return None


def desvio(valores):
    limpos = [v for v in valores if v is not None]
    if len(limpos) < 2:
        return None
    return round(statistics.pstdev(limpos), 3)


def percentil(valores, p):
    if not valores:
        return None
    ordenados = sorted(valores)
    pos = (len(ordenados) - 1) * (p / 100)
    inf, sup = math.floor(pos), math.ceil(pos)
    return ordenados[inf] if inf == sup else ordenados[inf] * (sup - pos) + ordenados[sup] * (pos - inf)


def estatisticas(valores):
    limpos = [v for v in valores if v is not None]
    if not limpos:
        return {"amostras": 0, "min": None, "media": None, "mediana": None,
                "p95": None, "max": None, "desvio_padrao": None,
                f"desvio_padrao_ultimos_{JANELA_DESVIO}": None}
    return {
        "amostras": len(limpos),
        "min": arredondar(min(limpos)),
        "media": arredondar(statistics.fmean(limpos)),
        "mediana": arredondar(statistics.median(limpos)),
        "p95": arredondar(percentil(limpos, 95)),
        "max": arredondar(max(limpos)),
        "desvio_padrao": desvio(limpos),
        f"desvio_padrao_ultimos_{JANELA_DESVIO}": desvio(limpos[-JANELA_DESVIO:]),
    }



# leitura bronze


def ler_bronze(caminho):
    if not os.path.isfile(caminho) or os.path.getsize(caminho) == 0:
        return []
    try:
        with open(caminho, "r", newline="", encoding="utf-8", errors="ignore") as f:
            amostra = f.read(4096)
            f.seek(0)
            try:
                delimitador = csv.Sniffer().sniff(amostra, delimiters=";,\t|").delimiter
            except csv.Error:
                delimitador = ";"
            leitor = csv.DictReader(f, delimiter=delimitador)
            cabecalho = [c.strip() for c in (leitor.fieldnames or [])]
            if any(c not in cabecalho for c in COLUNAS_OBRIGATORIAS):
                return []
            linhas = []
            for bruta in leitor:
                linha = {(k or "").strip(): v for k, v in bruta.items() if k}
                ts = ler_timestamp(linha.get("timestamp"))
                if ts is None or not para_texto(linha.get("identifier")):
                    continue
                linha["_ts"] = ts
                linhas.append(linha)
    except (OSError, csv.Error):
        return []

    linhas.sort(key=lambda l: l["_ts"])
    unicas, vistos = [], set()
    for linha in linhas:
        chave = (linha.get("identifier"), linha["_ts"])
        if chave not in vistos:
            vistos.add(chave)
            unicas.append(linha)
    return unicas


def colunas_nucleos(linha):
    texto_nucleos = para_texto(linha.get("cpu_por_nucleo"))
    if texto_nucleos:
        partes = texto_nucleos.split("|")
        res = [para_percentual(p) for p in partes if para_percentual(p) is not None]
        if res:
            return res
    nucleos = {}
    for chave, valor in linha.items():
        if chave.startswith("cpu_core_"):
            sufixo = chave.replace("cpu_core_", "")
            if sufixo.isdigit():
                nucleos[int(sufixo)] = para_percentual(valor)
    return [nucleos[i] for i in sorted(nucleos)]


# transformação


def delta(atual, anterior, coluna):
    a = para_positivo(atual.get(coluna))
    b = para_positivo(anterior.get(coluna)) if anterior else None
    if a is None or b is None:
        return None
    d = a - b
    return d if d >= 0 else None


def transformar_linha(linha, anterior, empresa):
    ts = linha["_ts"]
    intervalo = (ts - anterior["_ts"]).total_seconds() if anterior else None
    if intervalo is not None and intervalo <= 0:
        intervalo = None

    def taxa(coluna):
        return arredondar(dividir(delta(linha, anterior, coluna), intervalo))

    modos = ["cpu_user", "cpu_system", "cpu_nice", "cpu_idle", "cpu_iowait", "cpu_irq", "cpu_softirq", "cpu_steal"]
    deltas_modos = {m: delta(linha, anterior, m) for m in modos}
    total_modos = somar(*deltas_modos.values())
    pct_modo = {m: arredondar(dividir(d, total_modos) * 100) if dividir(d, total_modos) is not None else None
                for m, d in deltas_modos.items()}

    nucleos_logicos = para_positivo(linha.get("cpu_count_logical"))
    load_1m = para_positivo(linha.get("load_1m"))

    ram_total = para_positivo(linha.get("ram_total"))
    ram_used = para_positivo(linha.get("ram_used"))
    ram_avail = para_positivo(linha.get("ram_available"))
    ram_buffers = para_positivo(linha.get("ram_buffers"))
    ram_cached = para_positivo(linha.get("ram_cached"))
    ram_slab = para_positivo(linha.get("ram_slab"))
    buff_cache = somar(ram_buffers, ram_cached, ram_slab)

    swap_total = para_positivo(linha.get("swap_total"))
    swap_used = para_positivo(linha.get("swap_used"))
    swap_free = para_positivo(linha.get("swap_free"))

    d_leituras = delta(linha, anterior, "disk_read_count")
    d_escritas = delta(linha, anterior, "disk_write_count")
    d_t_leitura = delta(linha, anterior, "disk_read_time")
    d_t_escrita = delta(linha, anterior, "disk_write_time")

    lat_leitura = arredondar(dividir(d_t_leitura, d_leituras), 3)
    lat_escrita = arredondar(dividir(d_t_escrita, d_escritas), 3)
    lat_total = arredondar(dividir(somar(d_t_leitura, d_t_escrita), somar(d_leituras, d_escritas)), 3)

    d_pac = somar(delta(linha, anterior, "network_packets_sent"), delta(linha, anterior, "network_packets_recv"))
    d_drop = somar(delta(linha, anterior, "network_dropin"), delta(linha, anterior, "network_dropout"))
    perda_pct = round(d_drop / (d_pac + d_drop) * 100, 3) if d_drop is not None and d_pac is not None and (d_pac + d_drop) > 0 else None

    return {
        # Empresa é obrigatoriamente mantida em cada registro
        "empresa_id": empresa["empresa_id"],
        "empresa_nome": empresa["empresa_nome"],
        "identifier": para_texto(linha.get("identifier")),
        "vm_nome": empresa.get("vm_nome"),
        "hostname": para_texto(linha.get("hostname")),
        "os": para_texto(linha.get("os")),
        "timestamp": ts.strftime(FORMATO_TIMESTAMP),
        "intervalo_segundos": arredondar(intervalo, 3),

        # CPU
        "cpu_percent": arredondar(para_percentual(linha.get("cpu_percent"))),
        "cpu_freq_mhz": arredondar(para_positivo(linha.get("cpu_freq_current"))),
        "cpu_nucleos_logicos": nucleos_logicos,
        "cpu_nucleos_fisicos": para_positivo(linha.get("cpu_count_physical")),
        "cpu_por_nucleo": colunas_nucleos(linha),
        "load_1m": arredondar(load_1m, 3),
        "load_5m": arredondar(para_positivo(linha.get("load_5m")), 3),
        "load_15m": arredondar(para_positivo(linha.get("load_15m")), 3),
        "load_por_nucleo": arredondar(dividir(load_1m, nucleos_logicos), 3),
        "cpu_user_pct": pct_modo["cpu_user"],
        "cpu_sys_pct": pct_modo["cpu_system"],
        "cpu_nice_pct": pct_modo["cpu_nice"],
        "cpu_idle_pct": pct_modo["cpu_idle"],
        "cpu_iowait_pct": pct_modo["cpu_iowait"],
        "qtd_processos": para_positivo(linha.get("process_count")),
        "qtd_threads": para_positivo(linha.get("thread_count")),

        # RAM
        "ram_percent": arredondar(para_percentual(linha.get("ram_percent"))),
        "ram_total_gb": para_gb(ram_total),
        "ram_disponivel_gb": para_gb(ram_avail),
        "ram_usada_gb": para_gb(ram_used),
        "ram_cached_gb": para_gb(ram_cached),
        "ram_buffers_gb": para_gb(ram_buffers),
        "ram_shared_gb": para_gb(para_positivo(linha.get("ram_shared"))),
        "ram_slab_gb": para_gb(ram_slab),
        "ram_buff_cache_usado_gb": para_gb(buff_cache),
        "ram_buff_cache_total_gb": para_gb(ram_total),

        # SWAP
        "swap_percent": arredondar(para_percentual(linha.get("swap_percent"))),
        "swap_total_gb": para_gb(swap_total),
        "swap_disponivel_gb": para_gb(swap_free),
        "swap_usada_gb": para_gb(swap_used),

        # DISCO 
        "disco_percent": arredondar(para_percentual(linha.get("disk_root_percent"))),
        "disco_total_gb": para_gb(para_positivo(linha.get("disk_root_total"))),
        "disco_usado_gb": para_gb(para_positivo(linha.get("disk_root_used"))),
        "disco_livre_gb": para_gb(para_positivo(linha.get("disk_root_free"))),
        "disco_leitura_bytes_s": taxa("disk_read_bytes"),
        "disco_escrita_bytes_s": taxa("disk_write_bytes"),
        "disco_iops_leitura": taxa("disk_read_count"),
        "disco_iops_escrita": taxa("disk_write_count"),
        "disco_iops_total": arredondar(dividir(somar(d_leituras, d_escritas), intervalo)),
        "disco_latencia_leitura_ms": lat_leitura,
        "disco_latencia_escrita_ms": lat_escrita,
        "disco_latencia_ms": lat_total,

        # REDE
        "rede_upload_bytes_s": taxa("network_bytes_sent"),
        "rede_download_bytes_s": taxa("network_bytes_recv"),
        "rede_perda_pacotes_pct": perda_pct,
        "rede_erros_s": arredondar(dividir(somar(delta(linha, anterior, "network_errin"),
                                                 delta(linha, anterior, "network_errout")), intervalo)),
        "rede_established": para_positivo(linha.get("net_established")),
        "rede_time_wait": para_positivo(linha.get("net_time_wait")),
        "rede_close_wait": para_positivo(linha.get("net_close_wait")),
        "rede_conn_total": para_positivo(linha.get("net_conn_total")),
    }


METRICAS_DESVIO = [
    "cpu_percent", "load_1m", "cpu_iowait_pct",
    "ram_percent", "swap_percent",
    "disco_percent", "disco_leitura_bytes_s", "disco_escrita_bytes_s",
    "disco_iops_total", "disco_latencia_ms",
    "rede_upload_bytes_s", "rede_download_bytes_s", "rede_perda_pacotes_pct",
]


def aplicar_desvio_movel(registros):
    janelas = {m: deque(maxlen=JANELA_DESVIO) for m in METRICAS_DESVIO}
    for reg in registros:
        for m in METRICAS_DESVIO:
            janelas[m].append(reg.get(m))
            reg[f"{m}_dp{JANELA_DESVIO}"] = desvio(janelas[m])
    return registros



# SILVER - CSV


def gravar_silver(pasta_empresa, identificador, registros):
    diretorio = os.path.join(DIR_SILVER, pasta_empresa)
    os.makedirs(diretorio, exist_ok=True)
    caminho = os.path.join(diretorio, f"{identificador}_sistema.csv")
    colunas = list(registros[0].keys())
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=colunas, delimiter=DELIMITADOR_SAIDA)
        escritor.writeheader()
        for reg in registros:
            linha = dict(reg)
            linha["cpu_por_nucleo"] = "|".join(str(v) for v in reg["cpu_por_nucleo"]) or None
            escritor.writerow({k: ("NA" if v is None else v) for k, v in linha.items()})
    return caminho


# GOLD - JSON


def historico(registros, campos):
    serie = []
    for r in registros:
        ponto = {"timestamp": r["timestamp"]}
        for c in campos:
            ponto[c] = r.get(c)
            chave_dp = f"{c}_dp{JANELA_DESVIO}"
            if chave_dp in r:
                ponto[f"{c}_dp{JANELA_DESVIO}"] = r[chave_dp]
        serie.append(ponto)
    return serie


def ultimo_valido(registros, campo):
    for r in reversed(registros):
        if r.get(campo) not in (None, []):
            return r[campo]
    return None


def montar_gold(registros):
    serie = lambda c: [r.get(c) for r in registros]
    ult = lambda c: ultimo_valido(registros, c)

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
            "primeiro_registro": registros[0]["timestamp"],
            "ultimo_registro": registros[-1]["timestamp"],
            "total_registros": len(registros),
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
                "modos_percent": {"user": ult("cpu_user_pct"), "sys": ult("cpu_sys_pct"),
                                  "nice": ult("cpu_nice_pct"), "idle": ult("cpu_idle_pct"),
                                  "iowait": ult("cpu_iowait_pct")},
            },
            "resumo": {"uso_percent": estatisticas(serie("cpu_percent")),
                       "load_1m": estatisticas(serie("load_1m"))},
            "historico": historico(registros, [
                "cpu_percent", "cpu_por_nucleo", "load_1m", "load_5m", "load_15m",
                "cpu_user_pct", "cpu_sys_pct", "cpu_nice_pct", "cpu_idle_pct",
                "cpu_iowait_pct", "qtd_processos"]),
        },
        "ram": {
            "atual": {
                "uso_percent": ult("ram_percent"),
                "total_gb": ult("ram_total_gb"),
                "disponivel_gb": ult("ram_disponivel_gb"),
                "usada_gb": ult("ram_usada_gb"),
                "modos_gb": {"cached": ult("ram_cached_gb"), "buffers": ult("ram_buffers_gb"),
                             "shared": ult("ram_shared_gb"), "slab": ult("ram_slab_gb"),
                             "available": ult("ram_disponivel_gb")},
                "buffers_cache": {"usado_gb": ult("ram_buff_cache_usado_gb"),
                                  "total_gb": ult("ram_buff_cache_total_gb")},
            },
            "resumo": {"uso_percent": estatisticas(serie("ram_percent"))},
            "historico": historico(registros, [
                "ram_percent", "ram_usada_gb", "ram_disponivel_gb", "ram_cached_gb",
                "ram_buffers_gb", "ram_shared_gb", "ram_slab_gb", "ram_buff_cache_usado_gb"]),
        },
        "disco": {
            "atual": {
                "uso_percent": ult("disco_percent"),
                "total_gb": ult("disco_total_gb"),
                "usado_gb": ult("disco_usado_gb"),
                "livre_gb": ult("disco_livre_gb"),
                "leitura_bytes_s": ult("disco_leitura_bytes_s"),
                "escrita_bytes_s": ult("disco_escrita_bytes_s"),
                "iops": {"leitura": ult("disco_iops_leitura"), "escrita": ult("disco_iops_escrita"),
                         "total": ult("disco_iops_total")},
                "latencia_ms": {"leitura": ult("disco_latencia_leitura_ms"),
                                "escrita": ult("disco_latencia_escrita_ms"),
                                "media": ult("disco_latencia_ms")},
            },
            "resumo": {"uso_percent": estatisticas(serie("disco_percent")),
                       "iops_total": estatisticas(serie("disco_iops_total")),
                       "latencia_ms": estatisticas(serie("disco_latencia_ms"))},
            "historico": historico(registros, [
                "disco_percent", "disco_leitura_bytes_s", "disco_escrita_bytes_s",
                "disco_iops_leitura", "disco_iops_escrita", "disco_iops_total",
                "disco_latencia_leitura_ms", "disco_latencia_escrita_ms", "disco_latencia_ms"]),
        },
        "swap": {
            "atual": {
                "uso_percent": ult("swap_percent"),
                "total_gb": ult("swap_total_gb"),
                "disponivel_gb": ult("swap_disponivel_gb"),
                "usada_gb": ult("swap_usada_gb"),
            },
            "resumo": {"uso_percent": estatisticas(serie("swap_percent"))},
            "historico": historico(registros, ["swap_percent", "swap_usada_gb"]),
        },
        "rede": {
            "atual": {
                "conexoes": {"established": ult("rede_established"),
                             "time_wait": ult("rede_time_wait"),
                             "close_wait": ult("rede_close_wait"),
                             "total": ult("rede_conn_total")},
                "perda_pacotes_pct": ult("rede_perda_pacotes_pct"),
                "upload_bytes_s": ult("rede_upload_bytes_s"),
                "download_bytes_s": ult("rede_download_bytes_s"),
            },
            "resumo": {"upload_bytes_s": estatisticas(serie("rede_upload_bytes_s")),
                       "download_bytes_s": estatisticas(serie("rede_download_bytes_s")),
                       "perda_pacotes_pct": estatisticas(serie("rede_perda_pacotes_pct"))},
            "historico": historico(registros, [
                "rede_upload_bytes_s", "rede_download_bytes_s", "rede_perda_pacotes_pct",
                "rede_established", "rede_time_wait", "rede_close_wait", "rede_conn_total"]),
        },
    }


def nome_seguro(texto):
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(texto))[:120]


def ultimo_valido_bronze(linhas, *colunas):
    for linha in reversed(linhas):
        for col in colunas:
            valor = para_texto(linha.get(col))
            if valor:
                return valor
    return None



# EXECUÇÃO


def main():
    # 1. Download do S3 se ativado
    baixar_bronze_s3()

    if not os.path.isdir(DIR_BRONZE):
        print(f"[ERRO] Diretório bronze não encontrado: {DIR_BRONZE}")
        return

    arquivos = sorted(glob.glob(os.path.join(DIR_BRONZE, "*.csv")))
    if not arquivos:
        print(f"[AVISO] Nenhum CSV encontrado em {DIR_BRONZE}")
        return

    os.makedirs(DIR_SILVER, exist_ok=True)
    os.makedirs(DIR_GOLD, exist_ok=True)

    mapa_bd = carregar_maquinas_bd()

    por_maquina = {}
    for caminho in arquivos:
        # Apenas processa arquivos de sistema principal
        if "_particoes.csv" in caminho or "_processos.csv" in caminho or "_conexoes_rede.csv" in caminho:
            continue
        print(f"Lendo {caminho}")
        for linha in ler_bronze(caminho):
            chave = normalizar_identificador(linha["identifier"])
            if chave:
                por_maquina.setdefault(chave, []).append(linha)

    if not por_maquina:
        print("[AVISO] Nenhuma linha válida encontrada nos arquivos bronze.")
        return

    indice = {}
    for identificador, linhas in por_maquina.items():
        linhas.sort(key=lambda l: l["_ts"])

        # Identificação da Empresa: BD > CSV Bronze > Fallback "sem_empresa"
        empresa_id = (mapa_bd.get(identificador, {}).get("empresa_id") or
                      ultimo_valido_bronze(linhas, "enterprise_identifier", "empresa_id") or "sem_empresa")
        empresa_nome = (mapa_bd.get(identificador, {}).get("empresa_nome") or
                        ultimo_valido_bronze(linhas, "enterprise_reason", "empresa_nome") or "Empresa Desconhecida")
        vm_nome = mapa_bd.get(identificador, {}).get("vm_nome")

        empresa = {
            "empresa_id": empresa_id,
            "empresa_nome": empresa_nome,
            "vm_nome": vm_nome
        }

        registros, anterior = [], None
        for linha in linhas:
            try:
                registros.append(transformar_linha(linha, anterior, empresa))
                anterior = linha
            except Exception as erro:
                print(f"  [AVISO] Linha ignorada ({identificador} {linha.get('timestamp')}): {erro}")

        if not registros:
            continue

        aplicar_desvio_movel(registros)
        nome = nome_seguro(identificador)
        pasta_empresa = nome_seguro(empresa_id)

        silver = gravar_silver(pasta_empresa, nome, registros)
        gold = montar_gold(registros)
        dir_gold_empresa = os.path.join(DIR_GOLD, pasta_empresa)
        os.makedirs(dir_gold_empresa, exist_ok=True)
        caminho_gold = os.path.join(dir_gold_empresa, f"{nome}_sistema.json")
        with open(caminho_gold, "w", encoding="utf-8") as f:
            json.dump(gold, f, ensure_ascii=False, indent=2)

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
        print(f"  OK {identificador} ({empresa_nome}): {len(registros)} registros -> {silver} | {caminho_gold}")

    for pasta_empresa, grupo in indice.items():
        with open(os.path.join(DIR_GOLD, pasta_empresa, "indice_maquinas.json"), "w", encoding="utf-8") as f:
            json.dump(grupo, f, ensure_ascii=False, indent=2)

    with open(os.path.join(DIR_GOLD, "indice_maquinas.json"), "w", encoding="utf-8") as f:
        json.dump({"empresas": list(indice.values())}, f, ensure_ascii=False, indent=2)

    total = sum(len(g["maquinas"]) for g in indice.values())
    print(f"Concluído: {total} máquina(s) de {len(indice)} empresa(s) processada(s).")


if __name__ == "__main__":
    main()