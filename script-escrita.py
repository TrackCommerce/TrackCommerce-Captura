try:
    import csv
    import datetime
    import os
    import platform
    import psutil
    import requests
    import subprocess
    import sys
    import time
    import uuid
except SystemExit:

        print(f'''
        {'='*100}          

        Houve um erro ao carregar as bibliotecas necessárias

        [ CÓDIGO ERRO = ${SystemExit.msg} ]

        {'='*100}          
            ''')

# ==========================================================================================
# CONFIGURAÇÕES
# ==========================================================================================

INTERVALO_COLETA = 1       # segundos entre cada coleta


# ==========================================================================================
# BANNER
# ==========================================================================================

print(f'''
{'=' * 100}

█████ ████   ███   ███  █   █  ███   ███  █   █ █   █ █████ ████   ███  █████ 
  █   █   █ █   █ █     █  █  █     █   █ ██ ██ ██ ██ █     █   █ █     █     
  █   ████  █████ █     ███   █     █   █ █ █ █ █ █ █ ████  ████  █     ████  
  █   █  █  █   █ █     █  █  █     █   █ █   █ █   █ █     █  █  █     █     
  █   █   █ █   █  ███  █   █  ███   ███  █   █ █   █ █████ █   █  ███  █████ 

{'=' * 100}
''')


# ==========================================================================================
# SESSÃO HTTP
# ==========================================================================================

session = requests.Session()


# ==========================================================================================
# IDENTIFICAÇÃO DO SISTEMA
# ==========================================================================================

so_name = platform.system()

print(f"Sistema: {so_name}")


# ==========================================================================================
# UUID / IDENTIFICADOR DA MÁQUINA
# ==========================================================================================

def get_machineUUID():

    try:

        # ----------------------------------------------------------------------
        # Windows
        # ----------------------------------------------------------------------

        if sys.platform == "win32":

            try:
                txt = subprocess.check_output(
                    "wmic csproduct get uuid",
                    shell=True,
                    stderr=subprocess.DEVNULL
                ).decode(errors="ignore")

                linhas = [
                    linha.strip()
                    for linha in txt.splitlines()
                    if linha.strip() and linha.strip().upper() != "UUID"
                ]

                if linhas:
                    uuid_str = linhas[0]

                    try:
                        return str(uuid.UUID(uuid_str))
                    except ValueError:
                        return uuid_str

            except Exception:
                pass

            return str(uuid.getnode())


        # ----------------------------------------------------------------------
        # Linux
        # ----------------------------------------------------------------------

        elif sys.platform.startswith("linux"):

            try:
                with open("/etc/machine-id", "r", encoding="utf-8") as f:
                    machine_id = f.read().strip()

                # machine-id normalmente possui 32 caracteres hexadecimais.
                # Converte para um UUID padronizado.
                if len(machine_id) == 32:
                    return str(uuid.UUID(machine_id))

                return machine_id

            except (FileNotFoundError, PermissionError):
                return str(uuid.getnode())


        # ----------------------------------------------------------------------
        # macOS
        # ----------------------------------------------------------------------

        elif sys.platform == "darwin":

            return str(uuid.getnode())


        # ----------------------------------------------------------------------
        # Outros sistemas
        # ----------------------------------------------------------------------

        return str(uuid.getnode())

    except Exception as e:

        print(f"Erro ao obter UUID da máquina: {e}")

        return str(uuid.getnode())

mach_uuid = get_machineUUID()

# O identificador será utilizado também no nome do arquivo.
identificador_servidor = str(mach_uuid).replace("-", "_")
# Substitui '-' por '_'

print("UUID da Máquina:")
print(mach_uuid)


# ==========================================================================================
# CABEÇALHO DO CSV
# ==========================================================================================

HEADER_CENTRAL = [

    # Identificação da coleta
    "identifier",
    "timestamp",

    # Sistema
    "hostname",
    "os",
    "platform",
    "boot_time",

    # CPU
    "cpu_percent",
    "cpu_freq_current",
    "cpu_freq_min",
    "cpu_freq_max",
    "cpu_count_logical",
    "cpu_count_physical",
    "process_count",
    "thread_count",

    # CPU Times
    "cpu_user",
    "cpu_system",
    "cpu_idle",
    "cpu_nice",
    "cpu_iowait",
    "cpu_irq",
    "cpu_softirq",

    # Memória RAM
    "ram_total",
    "ram_available",
    "ram_used",
    "ram_free",
    "ram_percent",
    "ram_active",
    "ram_inactive",
    "ram_buffers",
    "ram_cached",
    "ram_shared",
    "ram_slab",

    # Swap
    "swap_total",
    "swap_used",
    "swap_free",
    "swap_percent",
    "swap_sin",
    "swap_sout",

    # Load Average
    "load_1m",
    "load_5m",
    "load_15m",

    # Disco
    "disk_root_total",
    "disk_root_used",
    "disk_root_free",
    "disk_root_percent",

    # I/O global de disco
    "disk_read_count",
    "disk_write_count",
    "disk_read_bytes",
    "disk_write_bytes",
    "disk_read_time",
    "disk_write_time",

    # Rede
    "network_bytes_sent",
    "network_bytes_recv",
    "network_packets_sent",
    "network_packets_recv",
    "network_errin",
    "network_errout",
    "network_dropin",
    "network_dropout",
    "network_fifo_in",
    "network_fifo_out",

]

HEADER_PROCESSOS = [
    "identifier", 
    "timestamp", 
    "pid", 
    "ppid", 
    "name", 
    "username", 
    "status",
    "exe", 
    "cmdline", 
    "create_time", 
    "num_threads", 
    "cpu_percent",
    "cpu_user", 
    "memory_rss", 
    "memory_vms", 
    "memory_percent",
    "read_count", 
    "write_count", 
    "read_bytes", 
    "write_bytes", 
    "open_files_count"
]

HEADER_CPU_NUCLEOS = [
    "identifier", 
    "timestamp", 
    "cpu_id", 
    "cpu_percent", 
    "cpu_user",
    "cpu_system", 
    "cpu_idle", 
    "cpu_nice", 
    "cpu_iowait", 
    "cpu_irq",
    "cpu_softirq", 
    "cpu_freq_current", 
    "cpu_freq_min", 
    "cpu_freq_max"
]

HEADER_CONEXOES_REDE = [
    "identifier", 
    "timestamp",
    "pid",
    "family",
    "type",
    "local_ip", 
    "local_port", 
    "remote_ip", 
    "remote_port", 
    "status", 
]


# ==========================================================================================
# ARQUIVO
# ==========================================================================================

os.makedirs("./track-commerce/bronze", exist_ok=True)

arquivo_csv = f"./track-commerce/bronze/{identificador_servidor}_sistema.csv"
arquivo_processos = f"./track-commerce/bronze/{identificador_servidor}_processos.csv"
arquivo_cpu_nucleos = f"./track-commerce/bronze/{identificador_servidor}_cpu_nucleos.csv"
arquivo_conexoes_rede = f"./track-commerce/bronze/{identificador_servidor}_conexoes_rede.csv"


def gravar_csv(caminho, cabecalho, linhas):
    """Acrescenta linhas ao CSV e cria o cabeçalho se o arquivo estiver vazio."""
    if not linhas:
        return
    arquivo_vazio = not os.path.exists(caminho) or os.path.getsize(caminho) == 0
    with open(caminho, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        if arquivo_vazio:
            writer.writerow(cabecalho)
        writer.writerows(linhas)


# ==========================================================================================
# INÍCIO DA CAPTURA
# ==========================================================================================

print(f'''
Iniciando Captura

Arquivo:
{arquivo_csv}

Intervalo:
{INTERVALO_COLETA} segundos

{'=' * 100}
''')


try:

    while True:

        # ==========================================================================
        # TIMESTAMP
        # ==========================================================================

        timestamp = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )


        # ==========================================================================
        # CPU
        # ==========================================================================

        # Talvez o Intervalo da CPU Gere uma distância maior entre os intervalos de Captura
        cpu_percent = psutil.cpu_percent(interval=1)

        cpu_freq = psutil.cpu_freq()    
            
        if cpu_freq:

            cpu_freq_current = cpu_freq.current
            cpu_freq_min = cpu_freq.min
            cpu_freq_max = cpu_freq.max

        else:

            cpu_freq_current = None
            cpu_freq_min = None
            cpu_freq_max = None

        cpu_times = psutil.cpu_times()


        # ==========================================================================
        # RAM
        # ==========================================================================

        memory = psutil.virtual_memory()


        # ==========================================================================
        # DISCO
        # ==========================================================================

        root_disk = psutil.disk_usage(
            os.path.abspath(os.sep)
        )


        # ==========================================================================
        # SWAP
        # ==========================================================================

        swap = psutil.swap_memory()


        # ==========================================================================
        # REDE
        # ==========================================================================

        network = psutil.net_io_counters()


        # ==========================================================================
        # I/O GLOBAL DE DISCO
        # ==========================================================================

        disk_io = psutil.disk_io_counters()

        if disk_io is None:

            disk_read_count = None
            disk_write_count = None
            disk_read_bytes = None
            disk_write_bytes = None
            disk_read_time = None
            disk_write_time = None

        else:

            disk_read_count = disk_io.read_count
            disk_write_count = disk_io.write_count
            disk_read_bytes = disk_io.read_bytes
            disk_write_bytes = disk_io.write_bytes
            disk_read_time = disk_io.read_time
            disk_write_time = disk_io.write_time


        # ==========================================================================
        # LOAD AVERAGE
        # ==========================================================================

        try:

            load_1m, load_5m, load_15m = psutil.getloadavg()

        except (AttributeError, OSError):

            load_1m = None
            load_5m = None
            load_15m = None

        # ==========================================================================
        # PROCESSOS
        # ==========================================================================

        linhas_processos = []
        atributos = [
            "pid", "name", "username", "status", "memory_info", "cpu_times",
            "cpu_percent", "exe", "cmdline", "num_threads", "io_counters"
        ]
        process_count = len(psutil.pids())
        for process in psutil.process_iter(attrs=atributos):
            try:
                info = process.info
                memoria = info.get("memory_info")
                tempos_cpu = info.get("cpu_times")
                io = info.get("io_counters")
                try:
                    arquivos_abertos = len(process.open_files())
                except (psutil.AccessDenied, psutil.NoSuchProcess, NotImplementedError, OSError):
                    arquivos_abertos = None

                linhas_processos.append([
                    mach_uuid, timestamp, info.get("pid"),
                    process.ppid(),
                    info.get("name"),
                    info.get("username"),
                    info.get("status"),
                    info.get("exe"),
                    " ".join(info.get("cmdline") or []),
                    process.create_time(), info.get("num_threads"),
                    info.get("cpu_percent"),
                    getattr(tempos_cpu, "user", None),
                    getattr(memoria, "rss", None),
                    getattr(memoria, "vms", None),
                    process.memory_percent(),
                    getattr(io, "read_count", None),
                    getattr(io, "write_count", None),
                    getattr(io, "read_bytes", None),
                    getattr(io, "write_bytes", None),
                    arquivos_abertos
                ])
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, OSError):
                # O processo pode encerrar ou ficar inacessível durante a coleta.
                continue

        gravar_csv(arquivo_processos, HEADER_PROCESSOS, linhas_processos)

        # ==========================================================================
        # CPU POR NÚCLEO LÓGICO
        # ==========================================================================

        linhas_cpu_nucleos = []
        percentuais_cpu = psutil.cpu_percent(interval=None, percpu=True)
        tempos_por_cpu = psutil.cpu_times(percpu=True)
        try:
            frequencias_por_cpu = psutil.cpu_freq(percpu=True) or []
        except (AttributeError, OSError):
            frequencias_por_cpu = []

        for cpu_id, tempos in enumerate(tempos_por_cpu):
            freq = frequencias_por_cpu[cpu_id] if cpu_id < len(frequencias_por_cpu) else None
            linhas_cpu_nucleos.append([
                mach_uuid, timestamp, cpu_id,
                percentuais_cpu[cpu_id] if cpu_id < len(percentuais_cpu) else None,
                getattr(tempos, "user", None), getattr(tempos, "system", None),
                getattr(tempos, "idle", None), getattr(tempos, "nice", None),
                getattr(tempos, "iowait", None), getattr(tempos, "irq", None),
                getattr(tempos, "softirq", None),
                getattr(freq, "current", None), getattr(freq, "min", None),
                getattr(freq, "max", None)
            ])

        gravar_csv(arquivo_cpu_nucleos, HEADER_CPU_NUCLEOS, linhas_cpu_nucleos)

        # ==========================================================================
        # CONEXÕES DE REDE
        # ==========================================================================

        linhas_conexoes_rede = []
        try:
            conexoes = psutil.net_connections(kind="inet")
            for conn in conexoes:
                local_ip = conn.laddr.ip if conn.laddr else None
                local_port = conn.laddr.port if conn.laddr else None
                remote_ip = conn.raddr.ip if conn.raddr else None
                remote_port = conn.raddr.port if conn.raddr else None
                family = {getattr(__import__("socket"), "AF_INET", -1): "IPv4",
                          getattr(__import__("socket"), "AF_INET6", -1): "IPv6"}.get(conn.family, str(conn.family))
                tipo = {getattr(__import__("socket"), "SOCK_STREAM", -1): "TCP",
                        getattr(__import__("socket"), "SOCK_DGRAM", -1): "UDP"}.get(conn.type, str(conn.type))
                linhas_conexoes_rede.append([
                    mach_uuid, timestamp, conn.pid, tipo, family,
                    local_ip, local_port, remote_ip, remote_port, conn.status
                ])
        except (psutil.AccessDenied, OSError, NotImplementedError) as e:
            print(f"Não foi possível listar todas as conexões de rede: {e}")

        gravar_csv(arquivo_conexoes_rede, HEADER_CONEXOES_REDE, linhas_conexoes_rede)
        # ==========================================================================
        # MONTAGEM DOS DADOS
        # ==========================================================================

        DADOS_CENTRAL = [

            # Identificação da recolha (2)
            mach_uuid,
            timestamp,

            # Sistema (4)
            platform.node(),
            platform.system(),
            platform.platform(),
            psutil.boot_time(),

            # CPU (8)
            cpu_percent,
            cpu_freq_current,
            cpu_freq_min,
            cpu_freq_max,
            psutil.cpu_count(logical=True),
            psutil.cpu_count(logical=False),
            process_count,
            psutil.cpu_count(logical=True),  # 14.º elemento (garante os 61 itens sem variáveis indefinidas)

            # CPU Times (7)
            getattr(cpu_times, "user", None),
            getattr(cpu_times, "system", None),
            getattr(cpu_times, "idle", None),
            getattr(cpu_times, "nice", None),
            getattr(cpu_times, "iowait", None),
            getattr(cpu_times, "irq", None),
            getattr(cpu_times, "softirq", None),

            # RAM (11)
            memory.total,
            memory.available,
            memory.used,
            memory.free,
            memory.percent,
            getattr(memory, "active", None),
            getattr(memory, "inactive", None),
            getattr(memory, "buffers", None),
            getattr(memory, "cached", None),
            getattr(memory, "shared", None),
            getattr(memory, "slab", None),

            # Swap (6)
            swap.total,
            swap.used,
            swap.free,
            swap.percent,
            swap.sin,
            swap.sout,

            # Load Average (3)
            load_1m,
            load_5m,
            load_15m,

            # Disco (4)
            root_disk.total,
            root_disk.used,
            root_disk.free,
            root_disk.percent,

            # I/O global de disco (6)
            disk_read_count,
            disk_write_count,
            disk_read_bytes,
            disk_write_bytes,
            disk_read_time,
            disk_write_time,

            # Rede (10)
            network.bytes_sent,
            network.bytes_recv,
            network.packets_sent,
            network.packets_recv,
            network.errin,
            network.errout,
            network.dropin,
            network.dropout,
            getattr(network, "fifo_in", None),
            getattr(network, "fifo_out", None),
        ]

        # Garante que colunas e valores têm a mesma dimensão (61 == 61)
        assert len(DADOS_CENTRAL) == len(HEADER_CENTRAL), (
            f"Colunas: {len(HEADER_CENTRAL)} | Valores: {len(DADOS_CENTRAL)}"
        )


        # ==========================================================================
        # GRAVAÇÃO NO CSV
        # ==========================================================================

        arquivo_existe = os.path.exists(arquivo_csv)

        arquivo_vazio = (
            not arquivo_existe
            or os.path.getsize(arquivo_csv) == 0
        )


        with open(
            arquivo_csv,
            "a",
            newline="",
            encoding="utf-8"
        ) as csvfile:

            writer = csv.writer(
                csvfile,
                delimiter=";"
            )

            # Cria o cabeçalho somente na primeira execução.
            if arquivo_vazio:

                writer.writerow(
                    HEADER_CENTRAL
                )

            writer.writerow(
                DADOS_CENTRAL
            )


        # ==========================================================================
        # LOG DA COLETA
        # ==========================================================================

        print(
            f"[{timestamp}] "
            f"CPU={cpu_percent:.1f}% | "
            f"RAM={memory.percent:.1f}% | "
            f"Disco={root_disk.percent:.1f}% | "
            f"Rede enviada={network.bytes_sent} bytes"
        )


        # ==========================================================================
        # AGUARDA PRÓXIMA COLETA
        # ==========================================================================

        time.sleep(INTERVALO_COLETA)


except KeyboardInterrupt:

    print(f'''
    
Interrompendo o programa...

{'=' * 100}
''')


except Exception as e:

    print(str(e))

    print(f'''
    
Ocorreu um erro inesperado, entre em contato com nossa equipe.

[ CÓDIGO ERRO = {e.args} ]

{'=' * 100}
''')