"""
==========================================================================================
 TRACK COMMERCE - CAPTURA DE MÉTRICAS
==========================================================================================
"""
import csv
import datetime
import os
import platform
import socket
import subprocess
import sys
import time
import uuid

# Bibliotecas externas
try:
    import psutil
except ImportError as erro:
    print(f"\n{'=' * 100}\nErro ao carregar a biblioteca psutil: {erro}\nInstale com: pip install psutil\n{'=' * 100}")
    sys.exit(1)

try:
    import boto3
except ImportError:
    boto3 = None



# configuração


INTERVALO_COLETA = 1       # segundos de espera entre ciclos
INTERVALO_CPU = 1          # segundos de medição do uso de CPU (bloqueante)
TOP_PROCESSOS = 50         # quantos processos gravar por ciclo (0 = todos)

# Partições ignoradas no monitoramento
FS_IGNORADOS = {"", "squashfs", "iso9660", "udf", "overlay", "devtmpfs", "tmpfs"} # Partições não especificadas no arquivo de Capturas de Métricas e no Figma

# Configurações AWS S3 (opcional) 
AWS_ENVIAR = os.getenv("AWS_ENVIAR", "False").lower() in ("true", "1", "t", "yes")
AWS_BUCKET = os.getenv("AWS_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")
AWS_PREFIXO = "track-commerce/bronze"
INTERVALO_UPLOAD_S = int(os.getenv("INTERVALO_UPLOAD_S", "300"))



#diretório padrão da aplicação


def diretorio_aplicacao():
    """Pasta onde a aplicação está (script .py ou executável empacotado)."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


DIR_APP = diretorio_aplicacao()
os.chdir(DIR_APP)
DIR_BRONZE = os.path.join(DIR_APP, "track-commerce", "bronze")
os.makedirs(DIR_BRONZE, exist_ok=True)




print(f'''
{'=' * 100}

█████ ████   ███   ███  █   █  ███   ███  █   █ █   █ █████ ████   ███  █████ 
  █   █   █ █   █ █     █  █  █     █   █ ██ ██ ██ ██ █     █   █ █     █     
  █   ████  █████ █     ███   █     █   █ █ █ █ █ █ █ ████  ████  █     ████  
  █   █  █  █   █ █     █  █  █     █   █ █   █ █   █ █     █  █  █     █     
  █   █   █ █   █  ███  █   █  ███   ███  █   █ █   █ █████ █   █  ███  █████ 

{'=' * 100}
''')

print(f"Sistema: {platform.system()}")


# UUID - identificador da máquina


def get_machineUUID():
    """Devolve um identificador estável da máquina"""
    try:
        if sys.platform == "win32":
            try:
                txt = subprocess.check_output("wmic csproduct get uuid", shell=True,
                                              stderr=subprocess.DEVNULL).decode(errors="ignore")
                linhas = [l.strip() for l in txt.splitlines() if l.strip() and l.strip().upper() != "UUID"]
                if linhas:
                    try:
                        return str(uuid.UUID(linhas[0]))
                    except ValueError:
                        return linhas[0]
            except Exception:
                pass
            return str(uuid.getnode())

        if sys.platform.startswith("linux"):
            try:
                with open("/etc/machine-id", "r", encoding="utf-8") as f:
                    machine_id = f.read().strip()
                return str(uuid.UUID(machine_id)) if len(machine_id) == 32 else machine_id
            except (FileNotFoundError, PermissionError):
                return str(uuid.getnode())

        return str(uuid.getnode())
    except Exception as e:
        print(f"Erro ao obter UUID da máquina: {e}")
        return str(uuid.getnode())


mach_uuid = get_machineUUID()
identificador_servidor = str(mach_uuid).replace("-", "_")
print(f"UUID da Máquina: {mach_uuid}")


# cabeçalhos do csv


HEADER_SISTEMA = [
    # Identificação
    "identifier", "timestamp", "hostname", "os", "platform", "boot_time",

    # CPU
    "cpu_percent", "cpu_por_nucleo", "cpu_freq_current", "cpu_freq_min", "cpu_freq_max",
    "cpu_count_logical", "cpu_count_physical", "process_count", "thread_count",

    # CPU Times
    "cpu_user", "cpu_system", "cpu_idle", "cpu_nice", "cpu_iowait",
    "cpu_irq", "cpu_softirq", "cpu_steal",

    # Load Average
    "load_1m", "load_5m", "load_15m",

    # RAM (bytes)
    "ram_total", "ram_available", "ram_used", "ram_free", "ram_percent",
    "ram_active", "ram_inactive", "ram_buffers", "ram_cached", "ram_shared", "ram_slab",

    # Swap (bytes)
    "swap_total", "swap_used", "swap_free", "swap_percent", "swap_sin", "swap_sout",

    # Disco Raiz (restaurado para cálculo correto no ETL)
    "disk_root_total", "disk_root_used", "disk_root_free", "disk_root_percent",

    # I/O global de disco (contadores acumulados)
    "disk_read_count", "disk_write_count", "disk_read_bytes", "disk_write_bytes",
    "disk_read_time", "disk_write_time",

    # Rede (contadores acumulados)
    "network_bytes_sent", "network_bytes_recv", "network_packets_sent", "network_packets_recv",
    "network_errin", "network_errout", "network_dropin", "network_dropout",

    # Conexões ativas agregadas por estado
    "net_conn_total", "net_established", "net_time_wait", "net_close_wait", "net_none", "net_outros",
]

HEADER_PARTICOES = [
    "identifier", "timestamp", "device", "mountpoint", "fstype",
    "total", "used", "free", "percent",
]

HEADER_PROCESSOS = [
    "identifier", "timestamp", "pid", "ppid", "name", "username", "status",
    "cpu_percent", "memory_percent", "memory_rss_mb", "memory_vms_mb",
    "num_threads", "read_bytes", "write_bytes"
]

HEADER_CONEXOES_REDE = [
    "identifier", "timestamp", "pid", "type", "family",
    "local_ip", "local_port", "remote_ip", "remote_port", "status"
]

arquivo_sistema = os.path.join(DIR_BRONZE, f"{identificador_servidor}_sistema.csv")
arquivo_particoes = os.path.join(DIR_BRONZE, f"{identificador_servidor}_particoes.csv")
arquivo_processos = os.path.join(DIR_BRONZE, f"{identificador_servidor}_processos.csv")
arquivo_conexoes_rede = os.path.join(DIR_BRONZE, f"{identificador_servidor}_conexoes_rede.csv")


def gravar_csv(caminho, cabecalho, linhas):
    """Acrescenta linhas ao CSV e escreve o cabeçalho se o arquivo for novo/vazio."""
    if not linhas:
        return
    arquivo_vazio = not os.path.exists(caminho) or os.path.getsize(caminho) == 0
    with open(caminho, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        if arquivo_vazio:
            writer.writerow(cabecalho)
        writer.writerows(linhas)



# ENVIO OPCIONAL AO S3 (boto3)


_cliente_s3 = None
_s3_desativado = False


def obter_cliente_s3():
    global _cliente_s3, _s3_desativado
    if _cliente_s3 is not None or _s3_desativado:
        return _cliente_s3
    if not AWS_ENVIAR:
        _s3_desativado = True
        return None
    if boto3 is None:
        print("[AVISO] AWS_ENVIAR=True, mas boto3 não está instalado. Envio ao S3 desativado.")
        _s3_desativado = True
        return None
    if not (AWS_BUCKET and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        print("[AVISO] Bucket ou credenciais AWS ausentes. Envio ao S3 desativado.")
        _s3_desativado = True
        return None
    try:
        _cliente_s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN or None,
            region_name=AWS_REGION,
        )
    except Exception as erro:
        print(f"[AVISO] Não foi possível criar o cliente S3: {erro}")
        _s3_desativado = True
    return _cliente_s3


def enviar_arquivos_s3(caminhos):
    cliente = obter_cliente_s3()
    if cliente is None:
        return
    for caminho in caminhos:
        if not os.path.isfile(caminho):
            continue
        chave = f"{AWS_PREFIXO}/{os.path.basename(caminho)}"
        try:
            cliente.upload_file(caminho, AWS_BUCKET, chave)
            print(f"[S3] Enviado: s3://{AWS_BUCKET}/{chave}")
        except Exception as erro:
            print(f"[AVISO] Falha ao enviar {caminho} ao S3: {erro}")



# coletas detalhadas 


def coletar_particoes(timestamp):
    linhas, vistos = [], set()
    for p in psutil.disk_partitions(all=False):
        if p.device in vistos or p.fstype.lower() in FS_IGNORADOS or "cdrom" in p.opts:
            continue
        try:
            uso = psutil.disk_usage(p.mountpoint)
        except (PermissionError, OSError):
            continue
        vistos.add(p.device)
        linhas.append([mach_uuid, timestamp, p.device, p.mountpoint, p.fstype,
                       uso.total, uso.used, uso.free, uso.percent])
    return linhas


def coletar_processos(timestamp):
    atributos = ["pid", "ppid", "name", "username", "status", "cpu_percent",
                 "memory_percent", "memory_info", "num_threads", "io_counters"]
    linhas = []
    for proc in psutil.process_iter(attrs=atributos, ad_value=None):
        try:
            info = proc.info
            mem = info.get("memory_info")
            io = info.get("io_counters")
            rss_mb = round(mem.rss / (1024 ** 2), 2) if mem and getattr(mem, "rss", None) else None
            vms_mb = round(mem.vms / (1024 ** 2), 2) if mem and getattr(mem, "vms", None) else None
            cpu = info.get("cpu_percent")
            ram = info.get("memory_percent")

            linhas.append([
                mach_uuid, timestamp, info.get("pid"), proc.ppid(), info.get("name"),
                info.get("username"), info.get("status"),
                round(cpu, 2) if cpu is not None else None,
                round(ram, 2) if ram is not None else None,
                rss_mb, vms_mb, info.get("num_threads"),
                getattr(io, "read_bytes", None) if io else None,
                getattr(io, "write_bytes", None) if io else None
            ])
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, OSError):
            continue

    linhas.sort(key=lambda l: -((l[7] or 0) + (l[8] or 0)))
    return linhas[:TOP_PROCESSOS] if TOP_PROCESSOS else linhas


def coletar_conexoes_detalhadas(timestamp):
    """Gera linhas com detalhes de cada conexão ativa na máquina."""
    linhas = []
    try:
        conexoes = psutil.net_connections(kind="inet")
        for conn in conexoes:
            local_ip = conn.laddr.ip if conn.laddr else None
            local_port = conn.laddr.port if conn.laddr else None
            remote_ip = conn.raddr.ip if conn.raddr else None
            remote_port = conn.raddr.port if conn.raddr else None

            fam = "IPv4" if conn.family == socket.AF_INET else ("IPv6" if conn.family == getattr(socket, "AF_INET6", -1) else str(conn.family))
            tp = "TCP" if conn.type == socket.SOCK_STREAM else ("UDP" if conn.type == socket.SOCK_DGRAM else str(conn.type))

            linhas.append([
                mach_uuid, timestamp, conn.pid, tp, fam,
                local_ip, local_port, remote_ip, remote_port, conn.status
            ])
    except (psutil.AccessDenied, OSError, NotImplementedError):
        pass
    return linhas


def contar_conexoes_resumo():
    vazio = {"total": None, "established": None, "time_wait": None,
             "close_wait": None, "none": None, "outros": None}
    try:
        conexoes = psutil.net_connections(kind="inet")
    except (psutil.AccessDenied, OSError, NotImplementedError):
        return vazio
    estados = {}
    for c in conexoes:
        estados[c.status] = estados.get(c.status, 0) + 1
    principais = {k: estados.get(v, 0) for k, v in
                  (("established", "ESTABLISHED"), ("time_wait", "TIME_WAIT"),
                   ("close_wait", "CLOSE_WAIT"), ("none", "NONE"))}
    total = len(conexoes)
    return {"total": total, **principais, "outros": total - sum(principais.values())}


def coletar_ciclo():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # CPU
    por_nucleo = psutil.cpu_percent(interval=INTERVALO_CPU, percpu=True)
    cpu_percent = round(sum(por_nucleo) / len(por_nucleo), 1) if por_nucleo else None
    freq = psutil.cpu_freq()
    tempos = psutil.cpu_times()

    # Memória e Swap
    memoria = psutil.virtual_memory()
    swap = psutil.swap_memory()

    # Disco Raiz
    try:
        root_disk = psutil.disk_usage(os.path.abspath(os.sep))
        d_root_total, d_root_used, d_root_free, d_root_pct = root_disk.total, root_disk.used, root_disk.free, root_disk.percent
    except Exception:
        d_root_total = d_root_used = d_root_free = d_root_pct = None

    disk_io = psutil.disk_io_counters()
    rede = psutil.net_io_counters()

    # Load Average
    try:
        load_1m, load_5m, load_15m = psutil.getloadavg()
    except (AttributeError, OSError):
        load_1m = load_5m = load_15m = None

    # Contagem total de Threads
    try:
        total_threads = sum(p.info["num_threads"] for p in psutil.process_iter(attrs=["num_threads"]) if p.info.get("num_threads"))
    except Exception:
        total_threads = None

    conexoes_resumo = contar_conexoes_resumo()
    conexoes_detalhadas = coletar_conexoes_detalhadas(timestamp)
    processos = coletar_processos(timestamp)
    particoes = coletar_particoes(timestamp)

    dados = {
        "identifier": mach_uuid,
        "timestamp": timestamp,
        "hostname": platform.node(),
        "os": platform.system(),
        "platform": platform.platform(),
        "boot_time": psutil.boot_time(),

        "cpu_percent": cpu_percent,
        "cpu_por_nucleo": "|".join(str(v) for v in por_nucleo),
        "cpu_freq_current": getattr(freq, "current", None),
        "cpu_freq_min": getattr(freq, "min", None),
        "cpu_freq_max": getattr(freq, "max", None),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "process_count": len(psutil.pids()),
        "thread_count": total_threads,

        "cpu_user": getattr(tempos, "user", None),
        "cpu_system": getattr(tempos, "system", None),
        "cpu_idle": getattr(tempos, "idle", None),
        "cpu_nice": getattr(tempos, "nice", None),
        "cpu_iowait": getattr(tempos, "iowait", None),
        "cpu_irq": getattr(tempos, "irq", None),
        "cpu_softirq": getattr(tempos, "softirq", None),
        "cpu_steal": getattr(tempos, "steal", None),

        "load_1m": load_1m, "load_5m": load_5m, "load_15m": load_15m,

        "ram_total": memoria.total,
        "ram_available": memoria.available,
        "ram_used": memoria.used,
        "ram_free": memoria.free,
        "ram_percent": memoria.percent,
        "ram_active": getattr(memoria, "active", None),
        "ram_inactive": getattr(memoria, "inactive", None),
        "ram_buffers": getattr(memoria, "buffers", None),
        "ram_cached": getattr(memoria, "cached", None),
        "ram_shared": getattr(memoria, "shared", None),
        "ram_slab": getattr(memoria, "slab", None),

        "swap_total": swap.total, "swap_used": swap.used, "swap_free": swap.free,
        "swap_percent": swap.percent, "swap_sin": swap.sin, "swap_sout": swap.sout,

        "disk_root_total": d_root_total,
        "disk_root_used": d_root_used,
        "disk_root_free": d_root_free,
        "disk_root_percent": d_root_pct,

        "disk_read_count": getattr(disk_io, "read_count", None),
        "disk_write_count": getattr(disk_io, "write_count", None),
        "disk_read_bytes": getattr(disk_io, "read_bytes", None),
        "disk_write_bytes": getattr(disk_io, "write_bytes", None),
        "disk_read_time": getattr(disk_io, "read_time", None),
        "disk_write_time": getattr(disk_io, "write_time", None),

        "network_bytes_sent": rede.bytes_sent, "network_bytes_recv": rede.bytes_recv,
        "network_packets_sent": rede.packets_sent, "network_packets_recv": rede.packets_recv,
        "network_errin": rede.errin, "network_errout": rede.errout,
        "network_dropin": rede.dropin, "network_dropout": rede.dropout,

        "net_conn_total": conexoes_resumo["total"], "net_established": conexoes_resumo["established"],
        "net_time_wait": conexoes_resumo["time_wait"], "net_close_wait": conexoes_resumo["close_wait"],
        "net_none": conexoes_resumo["none"], "net_outros": conexoes_resumo["outros"],
    }

    gravar_csv(arquivo_sistema, HEADER_SISTEMA, [[dados.get(c) for c in HEADER_SISTEMA]])
    gravar_csv(arquivo_particoes, HEADER_PARTICOES, particoes)
    gravar_csv(arquivo_processos, HEADER_PROCESSOS, processos)
    gravar_csv(arquivo_conexoes_rede, HEADER_CONEXOES_REDE, conexoes_detalhadas)

    print(f"[{timestamp}] CPU={cpu_percent}% | RAM={memoria.percent:.1f}% | "
          f"Disco Raiz={d_root_pct}% | Conexões={len(conexoes_detalhadas)} | Processos={len(processos)}")


# início captura

print(f'''
Iniciando Captura

Diretório bronze:
{DIR_BRONZE}

Intervalo: {INTERVALO_COLETA}s (+ {INTERVALO_CPU}s medição CPU)
Envio ao S3: {"ATIVADO" if AWS_ENVIAR else "desativado"}

{'=' * 100}
''')

try:
    for _ in psutil.process_iter(["cpu_percent"]):
        pass

    ultimo_upload = time.monotonic()

    while True:
        try:
            coletar_ciclo()
        except Exception as erro:
            print(f"[AVISO] Falha no ciclo de coleta: {erro}")

        if AWS_ENVIAR and time.monotonic() - ultimo_upload >= INTERVALO_UPLOAD_S:
            enviar_arquivos_s3([arquivo_sistema, arquivo_particoes, arquivo_processos, arquivo_conexoes_rede])
            ultimo_upload = time.monotonic()

        time.sleep(INTERVALO_COLETA)

except KeyboardInterrupt:
    if AWS_ENVIAR:
        enviar_arquivos_s3([arquivo_sistema, arquivo_particoes, arquivo_processos, arquivo_conexoes_rede])
    print(f"\nInterrompendo a captura...\n\n{'=' * 100}\n")

except Exception as e:
    print(f"\nErro inesperado na captura: [ CÓDIGO ERRO = {e.args} ]\n\n{'=' * 100}\n")