import csv
import os

identificador_servidor = input('Digite o identificador do seu servidor aqui: ')

parametro_porcentagem_cpu = 0
parametro_freq = 0
parametro_porcentagem_ram = 0
parametro_quantidade_ram = 0
parametro_porcentagem_disco = 0
parametro_quantidade_disco = 0

if not os.path.exists(f"./{identificador_servidor}.csv"):
    print(f'Não existe o arquivo CSV: {identificador_servidor}. Garanta que você escreveu o identificador corretamente!')
else:
    with open(f'{identificador_servidor}.csv', 'r') as csvfile:
        leitura_total = csv.reader(csvfile, delimiter=';')

        # Armazenando o maior valor de cada componente
        maior_porcentagem_cpu = 0
        maior_consumo_freq = 0
        maior_porcentagem_ram = 0
        maior_uso_ram = 0
        maior_porcentagem_disco = 0
        maior_uso_disco = 0

        # Armazenando o maior horário de cada componente
        horario_maior_porcentagem_cpu = 0
        horario_maior_consumo_freq = 0
        horario_maior_porcentagem_ram = 0
        horario_maior_uso_ram = 0
        horario_maior_porcentagem_disco = 0
        horario_maior_uso_disco = 0


        # Quantidade de alertas em cada componente
        alerta_freq = 0
        alerta_porcentagem_cpu = 0
        alerta_porcentagem_ram = 0
        alerta_quantidade_ram = 0
        alerta_porcentagem_disco = 0
        alerta_quantidade_disco = 0

        # Pulando o cabeçalho
        next(leitura_total)

        for linha in leitura_total:
            # Transformação de unidades
            frequencia_cpu = float(linha[2]) / 1000 # Transformando a frequência para GHz
            porcentagem_memoria_ram = round(((float(linha[4]) - float(linha[5])) / float(linha[4]) * 100), 2) # Calculando a porcetagem de memória RAM
            transformando_ram = float(linha[5]) ** 3
            transformacao_disco = float(linha[7]) ** 3 # Transformando a quantidade de disco em GHz

            # Descobrindo qual é o horário de maior consumo de cada componente
            if(float(linha[2]) > maior_consumo_freq):
                maior_consumo_freq = float(linha[2])
                horario_maior_consumo_freq = linha[1]

            if(float(linha[3]) > maior_porcentagem_cpu):
                maior_porcentagem_cpu = float(linha[3])
                horario_maior_porcentagem_cpu = linha[1]

            if(float(linha[4]) > maior_uso_ram):
                maior_uso_ram = float(linha[4])
                horario_maior_uso_ram = linha[1]

            if(float(linha[5]) > maior_porcentagem_ram):
                maior_porcentagem_ram = float(linha[5])
                horario_maior_porcentagem_ram = linha[1]

            if(float(linha[6]) > maior_porcentagem_disco):
                maior_porcentagem_disco = float(linha[6])
                horario_maior_porcentagem_disco = linha[1]

            if(float(linha[7]) > maior_uso_disco):
                maior_uso_disco = float(linha[7])
                horario_maior_uso_disco = linha[1]

            # Capturando quantidade de alerta em cada componente
            if(parametro_freq <= frequencia_cpu):
                alerta_freq += 1

            if(parametro_porcentagem_cpu <= float(linha[3])):
                alerta_porcentagem_cpu += 1

            if(parametro_porcentagem_ram <= porcentagem_memoria_ram):
                alerta_porcentagem_ram += 1

            if(parametro_quantidade_ram <= transformando_ram):
                alerta_quantidade_ram += 1

            if(parametro_porcentagem_disco <= float(linha[6])):
                alerta_porcentagem_disco += 1

            if(parametro_quantidade_disco <= transformacao_disco):
                alerta_quantidade_disco += 1

    print('\n')

    print('======== Horários com Maior Consumo de Cada Componente ========')
    print(f'Porcetagem de CPU: {horario_maior_porcentagem_cpu}')
    print(f'Frequência (GHz) de CPU: {horario_maior_consumo_freq}')
    print(f'Porcetagem de Memória RAM: {horario_maior_porcentagem_ram}')
    print(f'Consumo (GB) de Memória RAM: {horario_maior_uso_ram}')
    print(f'Porcetagem de Armazenamento em Disco: {horario_maior_porcentagem_disco}')
    print(f'Consumo (GB) de Armazenamento em Disco: {horario_maior_porcentagem_disco}')

    print('\n')

    print('======== Quantidade de Alertas de Acordo com os Parâmetros ========')
    print(f'Porcetagem de CPU: {alerta_freq}')
    print(f'Frequência (GHz) de CPU: {alerta_porcentagem_cpu}')
    print(f'Porcetagem de Memória RAM: {alerta_porcentagem_ram}')
    print(f'Consumo (GB) de Memória RAM: {alerta_quantidade_ram}')
    print(f'Porcetagem de Armazenamento em Disco: {alerta_porcentagem_disco}')
    print(f'Consumo (GB) de Armazenamento em Disco: {alerta_quantidade_disco}')

    print('\n')