import csv
import psutil
from datetime import datetime
import os

identificador_servidor = "" # Insira identificador do seu Servidor Aqui!

while True:

    # Caso o cabeçalho arquivo não tenha cabeçalho, irei criar
    if not os.path.exists(f"./{identificador_servidor}.csv"):
        with open(f"./{identificador_servidor}.csv", 'w') as csvfile:
            # Escrevendo o cabeçalho
            csv.writer(csvfile, delimiter=';').writerow(['identificador', 'timestamp','frequency_cpu' , 'percentage_cpu', 'memory_ram_total', 'memory_ram_available', 'percentage_storage', 'storege_available'])
    else:
        # Descobrindo a data e hora atual
        data_hora_atual = datetime.now()
        data_hora_formatada = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')

        # Capturando a frequencia da cpu
        cpu_frequencia_atual = round((psutil.cpu_freq().current), 2)

        # Capturando a porcentagem atual da cpu
        cpu_porcetagem = round((psutil.cpu_percent(interval=1)), 2)

        # Capturando a quantidade total de memória ram
        memoria_ram_total = round((psutil.virtual_memory().total), 2)

        # Capturando a quantidade disponivél de memória ram
        memoria_ram_disponivel = round((psutil.virtual_memory().available), 2)

        # Capturando a porcetagem de armazenamento em disco
        disco_porcetagem = round((psutil.disk_usage("/").percent), 2)

        # Capturando a quantidade de armazenamento em disco livre
        disco_livre = round((psutil.disk_usage("/").free), 2)

        # Escrevendo os dados em um CSV
        with open(f"./{identificador_servidor}.csv", 'a') as csvfile:
            csv.writer(csvfile, delimiter=';').writerow([identificador_servidor, data_hora_formatada, cpu_frequencia_atual, cpu_porcetagem, memoria_ram_total, memoria_ram_disponivel, disco_porcetagem, disco_livre])

        # Mostrando no terminal que o dado foi capturado
        print(f'Dado Capturado: \n {identificador_servidor} - {data_hora_formatada} - {cpu_frequencia_atual} - {cpu_porcetagem} - {memoria_ram_total} - {memoria_ram_disponivel} - {disco_porcetagem} - {disco_livre} \n ------------------------------------------------------------------------------------------------')