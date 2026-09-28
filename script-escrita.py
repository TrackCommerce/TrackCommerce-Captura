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
except ImportError:

        print(f'''
        {'='*100}          

        Houve um erro ao carregar as bibliotecas necessárias

        [ CÓDIGO ERRO = ${ImportError.msg} ]

        {'='*100}          
            ''')

# ==========================================================================================
# CONFIGURAÇÕES
# ==========================================================================================

INTERVALO_COLETA = 1       # segundos entre cada coleta
TIMEOUT_API = 5             # timeout da API em segundos

# Coloque a URL da API entre aspas "" caso queira monitorá-la.
# Exemplo:
# URL_API = "https://chart.googleapis.com/chart?chs=150x150&cht=qr&chl=SEU_LINK_AQUI&choe=UTF-8"
URL_API = None


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

    # CPU Times
    "cpu_user",
    "cpu_system",
    "cpu_idle",
    "cpu_nice",
    "cpu_iowait",
    "cpu_irq",
    "cpu_softirq",
    "cpu_steal",
    "cpu_guest",
    "cpu_guest_nice",

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

    # API
    "api_url",
    "api_latency_seconds",
    "api_status_code",
    "api_ok",
    "api_error",
]


# ==========================================================================================
# ARQUIVO
# ==========================================================================================

os.makedirs("./track-commerce/bronze", exist_ok=True)

arquivo_csv = (
    f"./track-commerce/bronze/{identificador_servidor}_sistema.csv"
)


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
        # API
        # ==========================================================================

        api_latency_seconds = None
        api_status_code = None
        api_ok = None
        api_error = None

        if URL_API:

            request_start = time.perf_counter()

            try:

                response = session.get(
                    URL_API,
                    timeout=TIMEOUT_API
                )

                api_latency_seconds = (
                    time.perf_counter() - request_start
                )

                api_status_code = response.status_code

                api_ok = (
                    200 <= response.status_code < 400
                )

            except requests.exceptions.Timeout:

                api_latency_seconds = (
                    time.perf_counter() - request_start
                )

                api_ok = False
                api_error = "timeout"

            except requests.exceptions.RequestException as exc:

                api_latency_seconds = (
                    time.perf_counter() - request_start
                )

                api_ok = False
                api_error = type(exc).__name__


        # ==========================================================================
        # MONTAGEM DOS DADOS
        # ==========================================================================

        DADOS_CENTRAL = [

            mach_uuid,

            timestamp,


            # ----------------------------------------------------------------------
            # Sistema
            # ----------------------------------------------------------------------

            platform.node(),

            platform.system(),

            platform.platform(),

            psutil.boot_time(),


            # ----------------------------------------------------------------------
            # CPU
            # ----------------------------------------------------------------------

            cpu_percent,

            cpu_freq_current,

            cpu_freq_min,

            cpu_freq_max,

            psutil.cpu_count(logical=True),

            psutil.cpu_count(logical=False),


            # ----------------------------------------------------------------------
            # CPU Times
            # ----------------------------------------------------------------------

            getattr(cpu_times, "user", None),

            getattr(cpu_times, "system", None),

            getattr(cpu_times, "idle", None),

            getattr(cpu_times, "nice", None),

            getattr(cpu_times, "iowait", None),

            getattr(cpu_times, "irq", None),

            getattr(cpu_times, "softirq", None),

            getattr(cpu_times, "steal", None),

            getattr(cpu_times, "guest", None),

            getattr(cpu_times, "guest_nice", None),


            # ----------------------------------------------------------------------
            # RAM
            # ----------------------------------------------------------------------

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


            # ----------------------------------------------------------------------
            # Swap
            # ----------------------------------------------------------------------

            swap.total,

            swap.used,

            swap.free,

            swap.percent,

            swap.sin,

            swap.sout,


            # ----------------------------------------------------------------------
            # Load Average
            # ----------------------------------------------------------------------

            load_1m,

            load_5m,

            load_15m,


            # ----------------------------------------------------------------------
            # Disco
            # ----------------------------------------------------------------------

            root_disk.total,

            root_disk.used,

            root_disk.free,

            root_disk.percent,


            # ----------------------------------------------------------------------
            # I/O
            # ----------------------------------------------------------------------

            disk_read_count,

            disk_write_count,

            disk_read_bytes,

            disk_write_bytes,

            disk_read_time,

            disk_write_time,


            # ----------------------------------------------------------------------
            # Rede
            # ----------------------------------------------------------------------

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


            # ----------------------------------------------------------------------
            # API
            # ----------------------------------------------------------------------

            URL_API,

            api_latency_seconds,

            api_status_code,

            api_ok,

            api_error,
        ]


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