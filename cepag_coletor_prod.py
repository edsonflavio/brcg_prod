#!/usr/bin/python3
# -*- coding: utf-8 -*-
################################################################
#Author: Edson Flavio de Souza
#Contato: edson.flavio @ ufpr.br
#Cliente: CEPAG - Centro Pesquisas Aplicada em Geoinformacao - UFPR
#DATA: 06/05/2022
#Versao: 1.0 - Script efetuado em bash script
#Versao: 2.0 - Script efetuado em python3
#Ultima Revisão: 10/07/2022
#
#Objetivo:
#
#Sincroniza os arquivos do receptor GNSS emlid reach para o servidor de arquivos do CEPAG
#
#Rev 01 - Alterado em 15/02/2022 por Edson Flavio -
#       Modificado a forma de representacao dos arquivos de origem e destino conforme especificado pelo CEPAG
#       reach_raw_DATA_HOJE*_RINEX_3_03.zip copia para DATA_HOJE_CEPAG_RINEX_3_03.zip
#Rev 02 - Alterado em 25/03/2022 por Edson Flavio
#       Modificado o modelo de envio, o qual agora sera efetuado de hora em hora
#       Reach_raw_DATA_HOJE+HORA_ARQ_COPIAR*_RINEX_3_30.zip copia para $DATA_HOJE$HORA_ARQ_COPIAR_CEPAG_RINEX_3_03.zip
#Rev 03 - Alterado em 25/03/2022 por Edson Flavio
#       Modificado o modelo de envio, o qual agora sera efetuado de hora em hora
#       Reach_raw_DATA_HOJE+HORA_ARQ_COPIAR*_RINEX_3_30.zip copia para Reach_raw_$DATA_HOJE$HORA_ARQ$MIN_ARQ_CEPAG_RINEX_3_03.zip
#Rev 04 - Alterado em 05/05/2022 por Edson Flavio
#       Refatorado Integralmente o Código em Python, para melhor compreensão
#Rev 05 - Alterado em 10/07/2022 por Edson Flavio
#Rev 06 - Ajustado os controles sobre a data dos arquivos, incluído verificação dos intervalos máximo para as datas dos arquvivos
#	checa_intervalo(), intervalo_hora_valido(), data_valida()
##################################################################
import os
import subprocess as sp
import sys
from datetime import datetime, timedelta, date
from encodings import utf_8
# Variavies de ambiente necessarias para o script
# Defina aqui os intervalos de variação do horário aceito para o envio dos arquivos
intervalo_superior_hora  = ['00','01','02','03']
intervalo_inferior_hora = ['57','58','59']
# Insira aqui o diretório no servidor remoto para onde será transferido o arquivo coletados do GNSS
dir_remoto_dados = "/tmp"
# Insira aqui o diretório no servidor remoto para onde será transferido o arquivo de logs deste script
dir_remoto_logs = "/tmp"
# Insira aqui o diretório do arquivo de dados do GNSS
dir_dados = "/data/logs"
# Insira aqui o nome do arquivo de log do script
arq_logs = "logs.txt"
# Insira aqui o nome do arquivo de destino que será transferido o log do script
arq_logs_destino = "logs.txt"
# Para a transferência ocorrer de forma satisfatória, é necessário 
# que haja comunicação SSH entre o GNSS e o servidor de arquivos do 
# CEPAG utilizando chaves assimetricas
# Use os seguintes comandos para criar a chave e transferir para o servidor de arquivos do CEPAG
# ssh-keygen -t rsa -b 4096
# copie a chave para o usuário que irá receber os arquivos no servidor
# ssh-copy-id -p porta_ssh -i ~/ssh/id_rsa.pub usuario@servidor
servidor = "ip_do_servidor"
porta_ssh = "22"
usuario = "usuario"

def gera_log(mensagem: str):
    '''Função para gerar os logs do script e gravar no arquivo de log 
    constante na var'iavel arq_logs'''
    data = datetime.now()
    dia = data.strftime("%d")
    mes = data.strftime("%m")
    ano = data.strftime("%Y")
    hora = data.strftime("%H")
    minuto = data.strftime("%M")
    try:
        arquivo_de_logs = f'{arq_logs}'
        with open(arquivo_de_logs, 'a') as f:
            log = f'{dia}/{mes}/{ano} {hora}:{minuto} - {mensagem}'
            print(log, file=f)
    except:
        log = f'{dia}/{mes}/{ano} {hora}:{minuto} - {mensagem}'
        print(f'Erro ao inserir log no arquivo {arq_logs} - VERIFIQUE!!!')
        exit (3) #Não foi possível criar/abrir o arquivo de log

def data_e_valida(data:str )->bool:
    '''Verifica se a data que consta no arquivo de origem é válida'''

    data_valida = False
    ano = int(data[0:4])
    mes = int(data[4:6])
    dia = int(data[6:8])
    if(mes==1 or mes==3 or mes==5 or mes==7 or mes==8 or mes==10 or mes==12):
        if dia <=31 and dia > 0:
            data_valida=True
    elif mes != 2:
        if dia <=30  and dia > 0:
            data_valida = True
    else:
        if ano%4==0 or ano%400==0:
            if dia<=29 and dia > 0:
                data_valida = True
        else:
            if dia<=28 and dia > 0:
                data_valida = True
    return data_valida

def checa_intervalo (minutos_do_arquivo:str, intervalo_aceito:list)->bool:
    '''Verifica se o minuto do arquivo está dentro do intervalo de minutos aceitos
    descritos nas variáveis intervalo_superior_hora e intervalo_inferior_hora'''

    minutos = minutos_do_arquivo
    intervalo_aceito = intervalo_aceito
    if minutos in intervalo_aceito:
        intervalo_aceito = True
    else:
        intervalo_aceito = False
    return intervalo_aceito

def is_meia_noite(hora:str)->bool:
    '''Verifica se a hora de geração do arquivo foi meia noite'''
    
    meia_noite = False
    if hora == '00':
        meia_noite = True
    else:
        meia_noite = False
    return meia_noite

def is_23h(hora:str)->bool:
    '''Verifica se a hora de geração do arquivo foi 23h'''

    horas_23 = False
    if hora == '23':
        horas_23 = True
    else:
        horas_23 = False
    return horas_23

def is_deontem(arq_data_de_amanha:str, arq_data_de_hoje:str)->bool:
    '''Verifica se a data do arquivo é do dia anterior'''

    if arq_data_de_amanha == arq_data_de_hoje:
            e_de_ontem = True
    else:
            e_de_ontem = False
    return e_de_ontem

def data_arquivo_ok(data_arq:str)->bool:
    """Verifica se Data do Arquivo encontra-se no prazo determinado como 
    válido, sendo permitido a data do dia ou a data do dia anterior, 
    devido a transição de horário quando o arquivo for gerado entre 
    23:57H e 23:59H do dia seguinte. Verifica ainda se o minuto da 
    hora está dentro do intervalo de minutos aceitos, sendo permitido os 
    intervalos de minutos constantes nas variávies intervalo_superior_hora 
    e intervalo_inferior_hora"""

    if data_e_valida(data_arq) is True:
        #Data de hoje formatada para YYYYMMDD
        data_hoje = date.today().strftime('%Y%m%d')
        #Data de ontem formatada para YYYYMMDD
        data_ontem = (date.today() - timedelta(days=1)).strftime('%Y%m%d')
        #Extrai da variável data_arq YYYYMMDD
        data_arquivo = data_arq[0:8]
        #Verifica se a data do arquivo está no intervalo permitido, data_hoje ou data_ontem
        min_arq_origem = data_arq[10:12]
        int_inf_val = checa_intervalo(min_arq_origem, intervalo_inferior_hora)
        int_sup_val = checa_intervalo(min_arq_origem, intervalo_superior_hora)

        if (data_arquivo != data_hoje) and (data_arquivo != data_ontem):
            data_arquivo_ok = False
        elif (int_inf_val == False) and (int_sup_val == False):
            data_arquivo_ok = False
        else:
            data_arquivo_ok = True
    else:
        data_arquivo_ok = False        
    return data_arquivo_ok

def ajustar_data_arquivo_destino(data_arquivo_origem:str)->str:
    #Indica a data de hoje no formato YYYYMMDD - 0 pad
    data_hoje = date.today().strftime('%Y%m%d')
    #Usada para ajustar a data constante no nome do arquivo para o dia seguinte
    data_dia_seguinte = (date.today()+timedelta(days=1)).strftime('%Y%m%d')
    #Captura a data constante no nome do arquivo de origem
    data_arquivo_original = data_arquivo_origem[0:8]
    #Decompõe da data_arquivo_origem HHMM
    hora_arq_origem = data_arquivo_origem[8:10] 
    min_arq_origem = data_arquivo_origem[10:12]
    #Inicializa a data do arquivo ajustada com a data do arquivo de origem
    data_arq_ajustada = data_arquivo_original
    #Inicializa a hora e minuto ajustada com os valores do arquivo de origem
    hora_arq_ajustada = hora_arq_origem
    min_arq_ajustado = min_arq_origem
    #Verifica se o arquivo é da data de ontem
    arq_de_ontem = is_deontem(data_dia_seguinte, data_hoje)
    #Verifica se está no intervalo de INFERIOR a 00:57 - 00:59
    intervalo_inferior_aceito = checa_intervalo(min_arq_origem, intervalo_inferior_hora)
    #Verifica se está no intervalo de POSTERIOR a 00:00 - 00:03
    intervalo_superior_aceito = checa_intervalo(min_arq_origem, intervalo_superior_hora)
    #Verifica se e meia-noite
    e_meia_noite = is_meia_noite(hora_arq_origem) #True se for meia-noite
    #Verifica se sao 23h
    e_23_hora = is_23h(hora_arq_origem) #True se é 23h
    #Verifica se o arquivo e de ontem e e das 23h e a hora esta no intervalo de 23:57 a 23:59
    if (arq_de_ontem) and (e_23_hora) and (intervalo_inferior_aceito):
        data_arq_ajustada = data_dia_seguinte
        hora_arq_ajustada = '00'
        min_arq_ajustado  = '00'
        data_arq_ajustada = f'{data_arq_ajustada}{hora_arq_ajustada}{min_arq_ajustado}'
        gera_log(f'Data do Arquivo Original - {data_arquivo_origem} - Estou no horario 23h do dia Anterior - Intervalo 23:57 - 23:59')
    #Verifica se o arquivo e de hoje e o horario e MEIA-NOITE e a hora esta no intervalo de 00:57 a 00:59
    elif (data_arquivo_original == data_hoje) and (e_meia_noite) and (intervalo_inferior_aceito):
        hora_arq_ajustada = '01'
        min_arq_ajustado  = '00'
        data_arq_ajustada = f'{data_arq_ajustada}{hora_arq_ajustada}{min_arq_ajustado}'
        gera_log(f'Data do Arquivo Original - {data_arquivo_origem} - Estou no horario 00 - Intervalo 00:57 - 00:59')
    #Verifica se o arquivo e de hoje e o horario e 23h e a hora esta no intervalo de 23:57 a 23:59
    elif (data_arquivo_original == data_hoje) and (e_23_hora) and (intervalo_inferior_aceito):
        data_arq_ajustada = data_dia_seguinte
        hora_arq_ajustada = '00'
        min_arq_ajustado  = '00'
        data_arq_ajustada = f'{data_arq_ajustada}{hora_arq_ajustada}{min_arq_ajustado}'
        gera_log(f'Data do Arquivo Original - {data_arquivo_origem} - Data de Hoje 23H e estou no Intervalo 23:57 - 23:59')
    # Verifica se o arquivo e de hoje e 
    # se a hora esta no intervalo de XX:57 a XX:59 
    # Do período das 00H as 22H
    # ajusta para o horario cheio XX:00
    elif (data_arquivo_original == data_hoje) and (e_23_hora == False) and (intervalo_inferior_aceito):
        #Data do arquivo e data atual são iguais - estão no mesmo dia
        hora_1 = hora_arq_origem[0:1]
        hora_2 = hora_arq_origem[1:2]
        if int(hora_1) == 0:
            if int(hora_2) <= 8:
                #hora_2 é string, precisa fazer um cast para somar
                hora_arq_ajustada = int(hora_2) + 1
                hora_arq_ajustada = "0" + str(hora_arq_ajustada)
                min_arq_ajustado = '00'
        elif (int(hora_arq_origem) <= 22) and (int(hora_arq_origem) >= 9):
            hora_arq_ajustada = int(hora_arq_origem) + 1
            min_arq_ajustado = '00'
        data_arq_ajustada = f'{data_arq_ajustada}{hora_arq_ajustada}{min_arq_ajustado}'
        gera_log(f'Data do Arquivo Original - {data_arquivo_origem} - Data de Hoje entre 01H e 22H e estou no Intervalo XX:57 - XX:59')        
    #Verifica se o arquivo e de hoje e o horario esta no intervalo de XX:00 a XX:03
    elif (data_arquivo_original == data_hoje) and (intervalo_superior_aceito):
        #Data do arquivo e data atual são iguais - estão no mesmo dia
        hora_arq_ajustada = hora_arq_origem
        min_arq_ajustado  = '00'
        data_arq_ajustada = f'{data_arq_ajustada}{hora_arq_ajustada}{min_arq_ajustado}'
        gera_log(f'Data do Arquivo Original {data_arquivo_origem} - com data de hoje e Intervalo XX:00 - XX:03')
    #Caso as regras acima nao sejam satisfeitas, nao houve ajuste de datas e o arquivo e invalido
    else:
        data_arq_ajustada = None
        gera_log(f'Data do Arquivo Original - {data_arquivo_origem} - Não Está no Intervalo Aceito')
    
    return data_arq_ajustada

def seleciona_arquivo_origem()->str:
    """"
    Selecionar o arquivo mais recente no diretório de dados
    """
    #Muda o diretorio de trabalho para _DIR_DADOS
    os.chdir(dir_dados)
    #Seleciona o arquivo mais recente
    res = sp.run("ls -tr1 *_RINEX_3_03.zip 2>/dev/null | tail -1", 
		     shell=True, 
		     stdout=sp.PIPE, 
		     universal_newlines=True)
    arquivo_origem = res.stdout
    #Limpa o \n que é retornado pelo comando no nome do arquivo
    arquivo_origem = arquivo_origem[:-1]
    #Retorna o nome do arquivo
    if arquivo_origem == '':
        arquivo_origem = None
    return arquivo_origem

def seleciona_arquivo_destino(arquivo_origem:str)->str:
    arq_destino_parte01 = 'Reach_raw'
    arq_destino_parte_02 = 'RINEX_3_03.zip'
    data_arq_origem = arquivo_origem.split('_')[2]
    data_arq_ajustada = ajustar_data_arquivo_destino(data_arq_origem)
    if data_arq_ajustada == data_arq_origem:
        arquivo_destino =  arquivo_origem
    else:
        arquivo_destino = f'{arq_destino_parte01}_{data_arq_ajustada}_{arq_destino_parte_02}'
    return arquivo_destino

def transfere_arq(arq_origem:str, arq_destino:str)->bool:
    mensagem = f'Iniciando a Transferência do arquivo {arq_origem}  para {arq_destino} no servidor'
    gera_log(mensagem)
    #Tenta enviar os dados para o servidor
    start = datetime.now()
    cmd = f'scp -P {porta_ssh} \'{arq_origem}\' {usuario}@{servidor}:\'{arq_destino}\''
    copiou_arquivos = sp.run(
                            [cmd], 
                            timeout=60, 
                            check=True,
                            shell=True,
                            universal_newlines=True
                        )
    end = datetime.now()
    if copiou_arquivos.returncode != 0:
        raise Exception(f'Erro ao transferir arquivo {arq_origem} para {arq_destino} - VERIFIQUE!!!\n'
                        f'Retornou: {str(copiou_arquivos.stdout)}')
    else:
        tempo_copia = end - start
        gera_log(f'Arquivo {arq_origem} transferido com sucesso, em {tempo_copia} s, para {arq_destino} no servidor')
        executou_com_sucesso = True
    return executou_com_sucesso

##################################################################################
#Script principal
##################################################################################
arquivo_origem = seleciona_arquivo_origem()
if arquivo_origem is None:
    mensagem = f'Nao ha arquivos disponiveis para coletar no Diretório da base GNSS - {dir_dados} - VERIFIQUE !!!' 
    gera_log(mensagem)
    exit(2)
data_arq_origem = arquivo_origem.split('_')[2]
if data_arquivo_ok(data_arq_origem):
    arquivo_destino = seleciona_arquivo_destino(arquivo_origem)
    arquivo_destino = f'{dir_remoto_dados}/{arquivo_destino}'
    transferiu_arq = transfere_arq(arquivo_origem, arquivo_destino)
    if transferiu_arq:
        arquivo_origem = f'{arq_logs}'
        arquivo_destino = f'{dir_remoto_logs}/{arq_logs_destino}'
        transferiu_logs = transfere_arq(arquivo_origem, arquivo_destino)
        if transferiu_logs:
            exit(0) # Terminou com Sucesso
        else:
            mensagem = f'Erro ao transferir arquivo de LOGS {arquivo_origem} para {arquivo_destino} - INVESTIGUE!!!'
            gera_log(mensagem)
            exit(3) #Erro ao transferir arquivo de LOGS
    else:
        mensagem = f'Erro ao transferir arquivo de DADOS {arquivo_origem} para {arquivo_destino} - INVESTIGUE!!!'
        gera_log(mensagem) #Erro ao transferir arquivo de DADOS
        exit(2)
else:
    gera_log(f'Data do arquivo de origem {data_arq_origem} não tem uma data válida!!!')
    exit(2)
