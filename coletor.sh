#!/bin/sh
################################################################
#Author: Edson Flavio de Souza
#Contato: edson.flavio @ ufpr.br
#Cliente: CEPAG - Centro Pesquisas Aplicada em Geoinformacao - UFPR
#DATA: 06/05/2022
#Ultima Revisão: 15/05/2022
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
#       Refatorado Integralmente o Código, para melhor compreensão
##################################################################

#Variavies de ambiente necessarias para o script
IP_SERVIDOR="IP_DO_SERVIDOR";
PORTA_SSH="PORTA_SSH";
INT_MIN_POST_HORA="00 01 02 03";
INT_MIN_ANT_HORA="57 58 59";
USUARIO="usuario";
DIR_DADOS="/tmp";
DIR_REMOTO_DADOS="/tmp"
DIR_REMOTO_LOGS="/tmp"
ARQ_LOGS="logs.txt"
ARQ_LOGS_DESTINO="logs_.txt"

checa_intervalo () {
        MIN="${1}";
        INTERVALO="${2}";
        for MIN_INT in $(echo $INTERVALO); do
                if [ $MIN_INT = $MIN ]; then
                        AJUSTAR_INTERVALO=0;
                        return $AJUSTAR_INTERVALO;
                else
                        AJUSTAR_INTERVALO=1;
                fi
        done
        return $AJUSTAR_INTERVALO;
}
is_meia_noite (){
        local MEIA_NOITE=1;
        if [ $ARQ_ORIG_HORA = 00 ]; then
                MEIA_NOITE=0;
        else
                MEIA_NOITE=1;
        fi
        return $MEIA_NOITE;
}
is_23h (){
        local HORA_23=1;
        if [ $ARQ_ORIG_HORA = 23 ]; then
                HORA_23=0;
        else
                HORA_23=1;
        fi
        return $HORA_23;
}
is_deontem () {
        if [ $DATA_ARQ_M1 = $DATA_HOJE ]; then
                return 0;
        else
                return 1;
        fi
}
ajustar_data_arquivo_destino () {
        local AJUSTAR_DATA=1;
        local DATA_HOJE=$(date +%Y%m%d);
        local DATA_ONTEM=$(date --date="1 day ago" +%Y%m%d);
        local DATA_ARQ=$(echo $1 | cut -c1-8);
        local DATA_ARQ_M1=$(date --date="${DATA_ARQ} 1 day" +%Y%m%d);
        local INT_SUP=0;
        local INT_INF=0;
        DATA_ARQ_AJUSTADA=$DATA_ARQ;
        #Verifica se está no intervalo de INFERIOR a 00:57 - 00:59
        $(checa_intervalo $ARQ_ORIG_MIN "$INT_MIN_ANT_HORA");
        INT_INF=$?;
        #Verifica se está no intervalo de POSTERIOR a 00:00 - 00:03
        $(checa_intervalo $ARQ_ORIG_MIN "$INT_MIN_POST_HORA");
        INT_SUP=$?
        #Verifica se e meia-noite
        is_meia_noite;
        MEIA_NOITE=$?
        #Verifica se sao 23h
        is_23h;
        HORA_23=$?
        #Verifica se Data do Arquivo e DIFERENTE da Data de Hoje e
        #Se a Data do Arquivo e DIFERENTE da data de Ontem
        if [ "$DATA_ARQ" -ne "$DATA_HOJE" -a "$DATA_ARQ" -ne "$DATA_ONTEM" ]; then
                gera_log "Data do arquivo de origem $ARQ_ORIG_DATA não está no espaço aceito para envio";
                exit 1;
        fi
        is_deontem;
        ARQ_DEONTEM=$?
        #Verifica se o arquivo e de ontem e e das 23h e a hora esta no intervalo de 23:57 a 23:59
        if [ "$ARQ_DEONTEM" -eq 0 -a "$HORA_23" -eq 0 -a "$INT_INF" -eq 0 ]; then
                DATA_ARQ_AJUSTADA=$DATA_ARQ_M1;
                HORA_ARQ_AJUSTADA='00';
                MIN_ARQ_AJUSTADO='00';
                AJUSTADO_DATA=0;
                gera_log "Data do Arquivo Original - $ARQ_ORIG_DATA - Estou no horario 23h do dia Anterior - Intervalo 00:57 - 00:59";
                return $AJUSTADO_DATA;
        #Verifica se o arquivo e de hoje e o horario e MEIA-NOITE e a hora esta no intervalo de 00:57 a 00:59
        elif [ "$DATA_ARQ" -eq "$DATA_HOJE" -a "$MEIA_NOITE" -eq 0 -a "$INT_INF" -eq 0 ]; then
                HORA_ARQ_AJUSTADA='01';
                MIN_ARQ_AJUSTADO='00';
                AJUSTADO_DATA=0;
                gera_log "Data do Arquivo Original - $ARQ_ORIG_DATA - Estou no horario 00 - Intervalo 00:57 - 00:59";
                return $AJUSTADO_DATA;
        #Verifica se o arquivo e de hoje e o horario e 23h e a hora esta no intervalo de 23:57 a 23:59
        elif [ "$DATA_ARQ" -eq "$DATA_HOJE" -a "$HORA_23" -eq 0 -a "$INT_INF" -eq 0 ]; then
                DATA_ARQ_AJUSTADA=$DATA_ARQ_M1;
                HORA_ARQ_AJUSTADA='00';
                MIN_ARQ_AJUSTADO='00';
                AJUSTADO_DATA=0;
                gera_log "Data do Arquivo Original - $ARQ_ORIG_DATA - Estou no horario 23 - Intervalo 23:57 - 23:59";
                return $AJUSTADO_DATA;
        #Verifica se o arquivo e de hoje e o horario e 23h e a hora esta no intervalo de 23:00 a 23:03
        elif [ "$DATA_ARQ" -eq "$DATA_HOJE" -a "$HORA_23" -eq 0 -a "$INT_SUP" -eq 0 ]; then
                HORA_ARQ_AJUSTADA=$ARQ_ORIG_HORA;
                MIN_ARQ_AJUSTADO='00';
                AJUSTADO_DATA=0;
                gera_log "Data do Arquivo Original - $ARQ_ORIG_DATA - Estou no horario 23 - Intervalo 23:00 - 23:03";
                return $AJUSTADO_DATA;
        #Verifica se o arquivo e de hoje hora esta no intervalo de XX:57 a XX:59 - ajusta para o horario cheio XX:00
        elif [ "$DATA_ARQ" -eq "$DATA_HOJE" -a "$INT_INF" -eq 0 ]; then
                #Data do arquivo e data atual são iguais - estão no mesmo dia
                local HORA_1;
                local HORA_2;
                HORA_1=$(echo $ARQ_ORIG_HORA | cut -c1);
                if [ $HORA_1 -eq 0 ]; then
                        HORA_2=$(echo $ARQ_ORIG_HORA | cut -c2);
                        if [ $HORA_2 -le 8 ]; then
                                HORA_ARQ_AJUSTADA=`expr $HORA_2 + 1`;
                                HORA_ARQ_AJUSTADA="0$HORA_ARQ_AJUSTADA";
                        fi
                fi
                if [ $ARQ_ORIG_HORA -le 23 -a $ARQ_ORIG_HORA -ge 9 ]; then
                        HORA_ARQ_AJUSTADA=`expr $ARQ_ORIG_HORA + 1`;
                fi
                if [ $HORA_ARQ_AJUSTADA -eq 24 ]; then
                        HORA_ARQ_AJUSTADA='00';
                fi
                MIN_ARQ_AJUSTADO='00';
                AJUSTADO_DATA=0;
                gera_log "Arquivo com data de hoje - $ARQ_ORIG_DATA - Intervalo XX:57 - XX:59";
                return $AJUSTADO_DATA;
        #Verifica se o arquivo e de hoje e o horario esta no intervalo de XX:00 a XX:03
        elif [ "$DATA_ARQ" -eq "$DATA_HOJE" -a "$INT_SUP" -eq 0 ]; then
                #Data do arquivo e data atual são iguais - estão no mesmo dia
                HORA_ARQ_AJUSTADA=$ARQ_ORIG_HORA;
                MIN_ARQ_AJUSTADO='00';
                AJUSTADO_DATA=0;
                gera_log "Arquivo com data de hoje - $ARQ_ORIG_DATA - Intervalo XX:00 - XX:03";
                return $AJUSTADO_DATA;
        #Caso as regras acima nao sejam satisfeitas, nao houve ajuste de datas e o arquivo e invalido
        else
                AJUSTADO_DATA=1;
                return $AJUSTADO_DATA;
        fi
}
seleciona_arquivos_origem_destino () {
        cd $DIR_DADOS;
        ARQ_ORIGINAL=$(ls -tr1 *_RINEX_3_03.zip 2>/dev/null | tail -1);
        [ -z "$ARQ_ORIGINAL" ] && gera_log "Nao ha arquivos disponiveis para coletar no Diretório da base GNSS - $DIR_DADOS - VERIFIQUE !!!"&& exit 1
        ARQ_ORIG_PARTE01="Reach_raw";
        ARQ_ORIG_DATA=$(echo $ARQ_ORIGINAL | cut -d'_' -f3);
        ARQ_ORIG_PARTE02="RINEX_3_03.zip";
        ARQ_ORIG_ANO=$(echo $ARQ_ORIG_DATA | cut -c1-4);
        ARQ_ORIG_MES=$(echo $ARQ_ORIG_DATA | cut -c5-6);
        ARQ_ORIG_DIA=$(echo $ARQ_ORIG_DATA | cut -c7-8);
        ARQ_ORIG_HORA=$(echo $ARQ_ORIG_DATA | cut -d'_' -f3 | cut -b 9-10);
        ARQ_ORIG_MIN=$(echo $ARQ_ORIG_DATA | cut -d'_' -f3 | cut -b 11-12);
        HORA_ARQ_AJUSTADA=$ARQ_ORIG_HORA;
        MIN_ARQ_AJUSTADO=$ARQ_ORIG_MIN;
        ajustar_data_arquivo_destino $ARQ_ORIG_DATA;

        if [ $? = 0 ] ; then
                ARQ_ORIGEM=$ARQ_ORIGINAL;
                ARQ_DESTINO="$ARQ_ORIG_PARTE01""_""$DATA_ARQ_AJUSTADA$HORA_ARQ_AJUSTADA$MIN_ARQ_AJUSTADO"'_'"$ARQ_ORIG_PARTE02";
        else
                gera_log "A data do arquivo $ARQ_ORIGINAL possui variação superior ao aceitável, não foi efetuado o envio do arquivo.";
                exit 1;
        fi
}
gera_log () {
        echo "$(date '+%d/%m/%Y %H:%M') ${1}" >> $ARQ_LOGS;
        if [ $? != 0 ]; then
                echo "$(date '+%d/%m/%Y %H:%M') Erro ao inserir log no arquivo $ARQ_LOGS - VERIFIQUE!!!" >> /dev/tty;
                exit 1;
        fi
}
transfere_arq () {
        ARQ_ORIG="${1}";
        ARQ_DEST="${2}";
        #Envia os dados para o servidor
        gera_log  "Iniciando a Transferência do arquivo $ARQ_ORIG para $ARQ_DEST no servidor";
        scp -P $PORTA_SSH $ARQ_ORIG $USUARIO@$IP_SERVIDOR:$ARQ_DEST;
        if [ $? = 0 ]; then
                gera_log "Arquivo $ARQ_ORIG transferido com sucesso para $ARQ_DEST no servidor";
                return 0;
        else
                gera_log "Erro ao transferir arquivo $ARQ_ORIG para $ARQ_DEST - VERIFIQUE!!!";
                return 1;
        fi
}

##################################################################################
#Script principal
##################################################################################

seleciona_arquivos_origem_destino;
transfere_arq $ARQ_ORIGEM "$DIR_REMOTO_DADOS/$ARQ_DESTINO";
if [ $? = 0 ]; then
        transfere_arq $ARQ_LOGS $DIR_REMOTO_DADOS/$ARQ_LOGS_DESTINO;
else
        gera_log Erro ao transferir arquivo $ARQ_LOGS para $DIR_REMOTO_DADOS/$ARQ_LOGS_DESTINO - INVESTIGUE!!!;
fi