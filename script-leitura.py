"""
==========================================================================================
 TRACK COMMERCE - ETL DE MÉTRICAS (BRONZE -> SILVER / GOLD)
==========================================================================================

 NOTA:
 Versão: beta 2.0

 O script a seguir trata os dados capturados das máquinas de nossos clientes e os
 transforma nas métricas descritas abaixo. Os dados tratados no momento atual estão no
 diretório raiz local e, depois, deverão ser capturados pelo Bucket S3 da aplicação e
 tratados da mesma forma.

 Vale ressaltar que este arquivo NÃO está fazendo tratamentos no BD, porque outros
 valores para identificar a máquina ainda serão capturados e implementados depois.

 ------------------------------------------------------------------------------------------
 Entrada : ./track-commerce/bronze/*.csv
 Saída   :
   SILVER (para análise em R - valores numéricos crus, sem formatação de texto)
     ./track-commerce/silver/<empresa_id>/<identificador>_sistema.csv
   GOLD   (para o site - métricas organizadas por componente, com TODO o histórico)
     ./track-commerce/gold/<empresa_id>/<identificador>_sistema.json
     ./track-commerce/gold/<empresa_id>/indice_maquinas.json (máquinas da empresa)
     ./track-commerce/gold/indice_maquinas.json   (empresas -> máquinas + último status)

 Desvio padrão: para cada métrica principal é calculado o desvio padrão MÓVEL dos
 últimos 60 registros (JANELA_DESVIO) de cada máquina. Ele aparece:
   - em cada linha do silver (coluna <metrica>_dp60)
   - em cada ponto do histórico do gold (campo "dp60")
   - no resumo do gold (desvio dos 60 registros mais recentes)

 Usa apenas a biblioteca padrão do Python.
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

# ==========================================================================================
# CONFIGURAÇÕES
# ==========================================================================================
load_dotenv()


def carregar_maquinas_bd():
    """
    Busca no BD o vínculo máquina (identificador/UUID) -> empresa.
    Retorna {identificador: {"empresa_id", "empresa_nome", "vm_nome"}}.
    Se o BD estiver indisponível, devolve {} e o script usa os dados do próprio CSV bronze.
    """
    try:
        conexao = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
        )
    except mysql.connector.Error as erro:
        print(f"[AVISO] BD indisponível, usando dados do bronze: {erro}")
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

DIR_BASE = "./track-commerce"
DIR_BRONZE = os.path.join(DIR_BASE, "bronze")
DIR_SILVER = os.path.join(DIR_BASE, "silver")
DIR_GOLD = os.path.join(DIR_BASE, "gold")


DELIMITADOR_SAIDA = ";"
FORMATO_TIMESTAMP = "%Y-%m-%d %H:%M:%S"
JANELA_DESVIO = 60          # quantidade de registros usada no desvio padrão móvel
BYTES_POR_GB = 1024 ** 3

# Colunas mínimas para que um arquivo bronze seja considerado válido.
# O script de captura grava APENAS o UUID da máquina (coluna "identifier");
# a empresa é resolvida pelo BD a partir desse identificador.
COLUNAS_OBRIGATORIAS = ["identifier", "timestamp"]


def normalizar_identificador(valor):
    """
    O script de captura grava o UUID sem hífens e com '_' no nome do arquivo,
    mas dentro do CSV grava o UUID original (com hífens). O BD pode guardar
    qualquer uma das formas. Esta função padroniza para comparação:
    minúsculas, sem hífens e sem underlines.
    """
    return para_texto(valor).lower().replace("-", "").replace("_", "")


# ==========================================================================================
# UTILITÁRIOS DE VALIDAÇÃO / CONVERSÃO
# Os dados podem vir quebrados, vazios ou com texto no lugar de número.
# Todas as conversões devolvem None em vez de lançar erro.
# ==========================================================================================

def para_float(valor):
    """Converte texto em float. Vazio, 'None', 'nan', inf ou lixo -> None."""
    if valor is None:
        return None
    texto = str(valor).strip().replace(",", ".")  # aceita vírgula decimal
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
    """Float válido somente se estiver entre 0 e 100 (percentuais impossíveis viram None)."""
    numero = para_float(valor)
    if numero is None or numero < 0 or numero > 100:
        return None
    return numero


def para_positivo(valor):
    """Float válido somente se >= 0 (bytes, contadores, tempos)."""
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
    """Divisão segura: evita divisão por zero e operandos nulos."""
    if a is None or b is None or b == 0:
        return None
    return a / b


def somar(*valores):
    """Soma ignorando None; se todos forem None devolve None."""
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
    """Desvio padrão populacional ignorando None. Precisa de pelo menos 2 valores."""
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
    if inf == sup:
        return ordenados[int(pos)]
    return ordenados[inf] * (sup - pos) + ordenados[sup] * (pos - inf)


def estatisticas(valores):
    """Resumo de uma série: geral + desvio padrão dos últimos JANELA_DESVIO registros."""
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


# ==========================================================================================
# LEITURA DO BRONZE
# ==========================================================================================

def ler_bronze(caminho):
    """
    Lê um CSV de captura. Validações:
      - arquivo vazio ou ilegível -> lista vazia
      - detecta o delimitador automaticamente
      - descarta linhas totalmente vazias e linhas sem identifier/timestamp válidos
      - ordena por timestamp e remove timestamps duplicados
    """
    if not os.path.isfile(caminho) or os.path.getsize(caminho) == 0:
        print(f"  [AVISO] Arquivo vazio ou inexistente: {caminho}")
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

            # Cabeçalho precisa existir e conter as colunas obrigatórias
            cabecalho = [c.strip() for c in (leitor.fieldnames or [])]
            faltando = [c for c in COLUNAS_OBRIGATORIAS if c not in cabecalho]
            if faltando:
                print(f"  [ERRO] {caminho} sem colunas obrigatórias: {faltando}")
                return []

            linhas = []
            for bruta in leitor:
                # Normaliza chaves (remove espaços) e ignora colunas extras sem nome
                linha = {(k or "").strip(): v for k, v in bruta.items() if k}
                if not any((v or "").strip() for v in linha.values() if isinstance(v, str)):
                    continue
                ts = ler_timestamp(linha.get("timestamp"))
                if ts is None or not para_texto(linha.get("identifier")):
                    continue  # linha quebrada: sem data ou sem máquina
                linha["_ts"] = ts
                linhas.append(linha)
    except (OSError, csv.Error) as erro:
        print(f"  [ERRO] Falha ao ler {caminho}: {erro}")
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
    """
    Uso por núcleo: o script de captura ainda NÃO grava essa informação.
    Se no futuro forem gravadas colunas cpu_core_0, cpu_core_1, ... elas serão lidas aqui.
    """
    nucleos = {}
    for chave, valor in linha.items():
        if chave.startswith("cpu_core_"):
            sufixo = chave.replace("cpu_core_", "")
            if sufixo.isdigit():
                nucleos[int(sufixo)] = para_percentual(valor)
    return [nucleos[i] for i in sorted(nucleos)]


# ==========================================================================================
# TRANSFORMAÇÃO (uma linha bronze -> um registro tratado)
# ==========================================================================================

def delta(atual, anterior, coluna):
    """Diferença de um contador acumulado. Negativo (reboot/reset) -> None."""
    a = para_positivo(atual.get(coluna))
    b = para_positivo(anterior.get(coluna)) if anterior else None
    if a is None or b is None:
        return None
    d = a - b
    return d if d >= 0 else None


def transformar_linha(linha, anterior, empresa):
    ts = linha["_ts"]
    intervalo = None
    if anterior:
        intervalo = (ts - anterior["_ts"]).total_seconds()
        if intervalo <= 0:
            intervalo = None

    def taxa(coluna):
        """Contador acumulado -> valor por segundo."""
        return arredondar(dividir(delta(linha, anterior, coluna), intervalo))

    # ---------------- CPU --------------------------------------------------------------
    # cpu_user, cpu_system... são segundos ACUMULADOS. O % de cada modo é o delta do modo
    # dividido pelo delta da soma de todos os modos no intervalo.
    modos = ["cpu_user", "cpu_system", "cpu_nice", "cpu_idle", "cpu_iowait",
             "cpu_irq", "cpu_softirq", "cpu_steal"]
    deltas_modos = {m: delta(linha, anterior, m) for m in modos}
    total_modos = somar(*deltas_modos.values())
    pct_modo = {m: arredondar(dividir(d, total_modos) * 100) if dividir(d, total_modos) is not None else None
                for m, d in deltas_modos.items()}

    nucleos_logicos = para_positivo(linha.get("cpu_count_logical"))
    load_1m = para_positivo(linha.get("load_1m"))

    # Quantidade de processos: ainda não capturada; lida se a coluna existir no futuro
    qtd_processos = para_positivo(linha.get("process_count"))

    # ---------------- RAM --------------------------------------------------------------
    ram_total = para_positivo(linha.get("ram_total"))
    ram_used = para_positivo(linha.get("ram_used"))
    ram_avail = para_positivo(linha.get("ram_available"))
    ram_buffers = para_positivo(linha.get("ram_buffers"))
    ram_cached = para_positivo(linha.get("ram_cached"))
    ram_slab = para_positivo(linha.get("ram_slab"))
    buff_cache = somar(ram_buffers, ram_cached, ram_slab)  # mesmo critério do "free -h"

    # ---------------- SWAP -------------------------------------------------------------
    swap_total = para_positivo(linha.get("swap_total"))
    swap_used = para_positivo(linha.get("swap_used"))
    swap_free = para_positivo(linha.get("swap_free"))

    # ---------------- DISCO ------------------------------------------------------------
    d_leituras = delta(linha, anterior, "disk_read_count")
    d_escritas = delta(linha, anterior, "disk_write_count")
    d_t_leitura = delta(linha, anterior, "disk_read_time")    # ms
    d_t_escrita = delta(linha, anterior, "disk_write_time")   # ms
    # Latência média = tempo gasto em I/O / quantidade de operações (ms por operação)
    lat_leitura = arredondar(dividir(d_t_leitura, d_leituras), 3)
    lat_escrita = arredondar(dividir(d_t_escrita, d_escritas), 3)
    lat_total = arredondar(dividir(somar(d_t_leitura, d_t_escrita), somar(d_leituras, d_escritas)), 3)

    # ---------------- REDE -------------------------------------------------------------
    d_pac = somar(delta(linha, anterior, "network_packets_sent"),
                  delta(linha, anterior, "network_packets_recv"))
    d_drop = somar(delta(linha, anterior, "network_dropin"),
                   delta(linha, anterior, "network_dropout"))
    perda_pct = None
    if d_drop is not None and d_pac is not None and (d_pac + d_drop) > 0:
        perda_pct = round(d_drop / (d_pac + d_drop) * 100, 3)

    return {
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
        "qtd_processos": qtd_processos,

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
        # Estados TCP: ainda não capturados; lidos se as colunas existirem no futuro
        "rede_established": para_positivo(linha.get("net_established")),
        "rede_time_wait": para_positivo(linha.get("net_time_wait")),
        "rede_close_wait": para_positivo(linha.get("net_close_wait")),
    }


# Métricas que recebem desvio padrão móvel dos últimos JANELA_DESVIO registros
METRICAS_DESVIO = [
    "cpu_percent", "load_1m", "cpu_iowait_pct",
    "ram_percent", "swap_percent",
    "disco_percent", "disco_leitura_bytes_s", "disco_escrita_bytes_s",
    "disco_iops_total", "disco_latencia_ms",
    "rede_upload_bytes_s", "rede_download_bytes_s", "rede_perda_pacotes_pct",
]


def aplicar_desvio_movel(registros):
    """Adiciona <metrica>_dp60 em cada registro (janela deslizante por máquina)."""
    janelas = {m: deque(maxlen=JANELA_DESVIO) for m in METRICAS_DESVIO}
    for reg in registros:
        for m in METRICAS_DESVIO:
            janelas[m].append(reg.get(m))
            reg[f"{m}_dp{JANELA_DESVIO}"] = desvio(janelas[m])
    return registros


# ==========================================================================================
# SILVER - CSV linha a linha (numérico, pronto para read.csv2 no R)
# ==========================================================================================

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
            # Listas (uso por núcleo) viram texto "12.5|30.1|..." para caber em uma célula
            linha["cpu_por_nucleo"] = "|".join(str(v) for v in reg["cpu_por_nucleo"]) or None
            # None vira "NA", que o R reconhece como valor ausente
            escritor.writerow({k: ("NA" if v is None else v) for k, v in linha.items()})
    return caminho


# ==========================================================================================
# GOLD - JSON por máquina com TODO o histórico, organizado por componente
# ==========================================================================================

def historico(registros, campos):
    """Série temporal completa: um ponto por registro (não apenas 1 linha)."""
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
    """Último valor não nulo de um campo (o mais recente pode ter vindo quebrado)."""
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
                "qtd_processos": ult("qtd_processos"),
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
                             "close_wait": ult("rede_close_wait")},
                "perda_pacotes_pct": ult("rede_perda_pacotes_pct"),
                "upload_bytes_s": ult("rede_upload_bytes_s"),
                "download_bytes_s": ult("rede_download_bytes_s"),
            },
            "resumo": {"upload_bytes_s": estatisticas(serie("rede_upload_bytes_s")),
                       "download_bytes_s": estatisticas(serie("rede_download_bytes_s")),
                       "perda_pacotes_pct": estatisticas(serie("rede_perda_pacotes_pct"))},
            "historico": historico(registros, [
                "rede_upload_bytes_s", "rede_download_bytes_s", "rede_perda_pacotes_pct",
                "rede_established", "rede_time_wait", "rede_close_wait"]),
        },
    }


def nome_seguro(texto):
    """Evita caracteres inválidos no nome do arquivo."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in texto)[:120]


# ==========================================================================================
# EXECUÇÃO
# ==========================================================================================

def main():
    # Validação: diretório de entrada precisa existir
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

    # Agrupa por máquina (identifier), pois a mesma máquina pode ter mais de um arquivo
    por_maquina = {}
    for caminho in arquivos:
        print(f"Lendo {caminho}")
        for linha in ler_bronze(caminho):
            chave = normalizar_identificador(linha["identifier"])
            if chave:
                por_maquina.setdefault(chave, []).append(linha)

    if not por_maquina:
        print("[AVISO] Nenhuma linha válida encontrada nos arquivos bronze.")
        return

    # indice agrupado por empresa: {empresa_id: {"empresa_id", "empresa_nome", "maquinas": [...]}}
    indice = {}
    for identificador, linhas in por_maquina.items():
        linhas.sort(key=lambda l: l["_ts"])

        # Empresa: BD tem prioridade; senão usa as colunas do próprio bronze
        empresa = mapa_bd.get(identificador) or {
            "empresa_id": ultimo_valido_bronze(linhas, "enterprise_identifier"),
            "empresa_nome": ultimo_valido_bronze(linhas, "enterprise_reason"),
            "vm_nome": None,
        }
        if not empresa.get("empresa_id"):
            print(f"  [AVISO] Máquina {identificador} sem empresa vinculada; ignorada.")
            continue

        registros, anterior = [], None
        for linha in linhas:
            try:
                registros.append(transformar_linha(linha, anterior, empresa))
                anterior = linha
            except Exception as erro:  # uma linha quebrada não derruba a máquina inteira
                print(f"  [AVISO] Linha ignorada ({identificador} {linha.get('timestamp')}): {erro}")

        if not registros:
            print(f"  [AVISO] Máquina {identificador} sem registros válidos.")
            continue

        aplicar_desvio_movel(registros)
        nome = nome_seguro(identificador)
        pasta_empresa = nome_seguro(str(empresa["empresa_id"]))

        silver = gravar_silver(pasta_empresa, nome, registros)
        gold = montar_gold(registros)
        dir_gold_empresa = os.path.join(DIR_GOLD, pasta_empresa)
        os.makedirs(dir_gold_empresa, exist_ok=True)
        caminho_gold = os.path.join(dir_gold_empresa, f"{nome}_sistema.json")
        with open(caminho_gold, "w", encoding="utf-8") as f:
            json.dump(gold, f, ensure_ascii=False, indent=2)

        grupo = indice.setdefault(pasta_empresa, {
            "empresa_id": empresa["empresa_id"],
            "empresa_nome": empresa["empresa_nome"],
            "maquinas": [],
        })
        grupo["maquinas"].append({
            **gold["maquina"],
            # caminho relativo à pasta gold: <empresa_id>/<identifier>_sistema.json
            "arquivo": f"{pasta_empresa}/{os.path.basename(caminho_gold)}",
            "cpu_percent": gold["cpu"]["atual"]["uso_percent"],
            "ram_percent": gold["ram"]["atual"]["uso_percent"],
            "disco_percent": gold["disco"]["atual"]["uso_percent"],
        })
        print(f"  OK {identificador} ({empresa['empresa_nome']}): {len(registros)} registros -> {silver} | {caminho_gold}")

    # Índice por empresa (o site de cada empresa carrega só o seu)
    for pasta_empresa, grupo in indice.items():
        with open(os.path.join(DIR_GOLD, pasta_empresa, "indice_maquinas.json"), "w", encoding="utf-8") as f:
            json.dump(grupo, f, ensure_ascii=False, indent=2)

    # Índice geral: lista de empresas, cada uma com suas máquinas
    with open(os.path.join(DIR_GOLD, "indice_maquinas.json"), "w", encoding="utf-8") as f:
        json.dump({"empresas": list(indice.values())}, f, ensure_ascii=False, indent=2)
    total = sum(len(g["maquinas"]) for g in indice.values())
    print(f"Concluído: {total} máquina(s) de {len(indice)} empresa(s) processada(s).")


def ultimo_valido_bronze(linhas, coluna):
    for linha in reversed(linhas):
        valor = para_texto(linha.get(coluna))
        if valor:
            return valor
    return None


if __name__ == "__main__":
    main()