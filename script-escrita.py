import csv
import psutil
from datetime import datetime
import time
import requests
import os

identificador_servidor = input('Digite o identificador do seu servidor aqui: ')
url = input('Digite a url da sua API de pagamento, para verificar a sua latência: ') # API de exemplo: https://pokeapi.co/api/v2/pokemon/blaziken

while True:

    # Caso o cabeçalho arquivo não tenha cabeçalho, irei criar
    if not os.path.exists(f"./{identificador_servidor}.csv"):
        with open(f"./{identificador_servidor}.csv", 'w') as csvfile:
            # Escrevendo o cabeçalho
            csv.writer(csvfile, delimiter=';').writerow(['identifier', 'timestamp','frequency_cpu' , 'percentage_cpu', 'memory_ram_total', 'memory_ram_available', 'percentage_storage', 'storege_available', 'latency', 'download', 'upload'])
    else:
        # Descobrindo a data e hora atual
        data_hora_atual = datetime.now()
        data_hora_formatada = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')

        # Capturando a frequencia da cpu
        cpu_frequencia_atual = round((psutil.cpu_freq().current), 2)

        # Capturando a velocidade de download/upload antes de um segundo
        download_upload_antes = psutil.net_io_counters()

        # Capturando a porcentagem atual da cpu
        cpu_porcetagem = round((psutil.cpu_percent(interval=1)), 2)

        # Capturando a velocidade de download/upload depois de um segundo
        download_upload_depois = psutil.net_io_counters()

        # Capturando a quantidade total de memória ram
        memoria_ram_total = round((psutil.virtual_memory().total), 2)

        # Capturando a quantidade disponivél de memória ram
        memoria_ram_disponivel = round((psutil.virtual_memory().available), 2)

        # Capturando a porcetagem de armazenamento em disco
        disco_porcetagem = round((psutil.disk_usage("/").percent), 2)

        # Capturando a quantidade de armazenamento em disco livre
        disco_livre = round((psutil.disk_usage("/").free), 2)

        # Descobrindo a latência 
        resposta_api = requests.get(url).elapsed.total_seconds()

        # Calculando o tempo final de dowload/upload
        calculo_download = download_upload_depois.bytes_recv - download_upload_antes.bytes_recv
        calculo_upload = download_upload_depois.bytes_sent - download_upload_antes.bytes_sent

        # Escrevendo os dados em um CSV
        with open(f"./{identificador_servidor}.csv", 'a') as csvfile:
            csv.writer(csvfile, delimiter=';').writerow([identificador_servidor, data_hora_formatada, cpu_frequencia_atual, cpu_porcetagem, memoria_ram_total, memoria_ram_disponivel, disco_porcetagem, disco_livre, resposta_api, calculo_download, calculo_upload])

        # Mostrando no terminal que o dado foi capturado
        print(f'Dado Capturado: \n Identificador Servidor: {identificador_servidor} - Data e hora captura: {data_hora_formatada} \nFrequência da CPU: {cpu_frequencia_atual} - Porcentagem de uso da CPU: {cpu_porcetagem} \nQunatidade total de Memória RAM: {memoria_ram_total} - Quantidade disponível de Memória RAM {memoria_ram_disponivel} \nPorcentagem de uso de disco: {disco_porcetagem} - Quantidade de disco livre: {disco_livre} \nLatência da API: {resposta_api} - Velocidade de Dowload: {calculo_download} - Velocidade de Upload: {calculo_upload} \n ------------------------------------------------------------------------------------------------')