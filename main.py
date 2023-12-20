# main.py
import os
import time
import logging
import pandas as pd
from datetime import datetime
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import io
from dotenv import load_dotenv
from config_bd import SessionLocal

# Carrega variáveis de ambiente
load_dotenv()

# Configurações de log
logging.basicConfig(
    handlers=[logging.FileHandler('Sentinela-corte-falta.log', 'a', 'utf-8'), logging.StreamHandler()],
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

# Configuração do cliente SES da AWS
ses_client = boto3.client(
    'ses',
    region_name='us-east-1',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def executar_consulta_sql(arquivo_sql):
    with SessionLocal() as session:
        with open(arquivo_sql, 'r') as file:
            query = file.read()
        result = pd.read_sql(query, session.bind)
    return result


def gerar_excel_em_memoria(dados_diarios, dados_mes):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Dados diários
        nome_aba_diaria = datetime.now().strftime('%d %m %Y')
        dados_diarios.to_excel(writer, sheet_name=nome_aba_diaria, index=False)

        # Dados mensais
        nome_aba_mensal = datetime.now().strftime('%B %Y')
        dados_mes.to_excel(writer, sheet_name=nome_aba_mensal, index=False)
    
    output.seek(0)
    return output

def construir_corpo_email(dados):
    corpo_email = "<html><body><p>Prezados,</p><p>Segue o relatório referente às alterações de preços detectadas:</p>"
    corpo_email += "<h2>Empresa</h2><ul style='list-style-type:none;'>"
    for filial, quantidade in dados.groupby('codfilial').size().items():
        nome_empresa = "Farmaum PB" if filial == '1' else "Farmaum RN" if filial == '2' else "Brasil" if filial == '3' else "Outra"
        corpo_email += f"<li><strong>{nome_empresa}</strong>: {quantidade}</li>"
    corpo_email += "</ul><h2>Comprador</h2><ul style='list-style-type:none;'>"
    for comprador, alteracoes in dados.groupby('comprador')['codprod'].nunique().items():
        corpo_email += f"<li><strong>{comprador}</strong>: {alteracoes}</li>"
    corpo_email += "</ul></body></html>"
    return corpo_email


def enviar_email(assunto, corpo, excel_data):
    try:
        destinatarios = os.getenv("EMAIL_DESTINATARIOS").split(';')

        msg = MIMEMultipart()
        msg['From'] = 'sentinela_corte_falta@aws.grupobrf1.com'
        msg['To'] = ', '.join(destinatarios)
        msg['Subject'] = assunto

        msg.attach(MIMEText(corpo, 'html'))

        nome_arquivo_excel = f"Sentinela Preço {datetime.now().strftime('%d %m %Y')}.xlsx"
        part = MIMEApplication(excel_data.read())
        part.add_header('Content-Disposition', 'attachment', filename=nome_arquivo_excel)
        msg.attach(part)

        ses_client.send_raw_email(
            Source=msg['From'],
            Destinations=destinatarios,
            RawMessage={'Data': msg.as_string()}
        )
    except Exception as e:
        logging.error(f"Erro ao enviar e-mail: {e}")


def verificar_mudancas():
    if datetime.now().hour == 17 and datetime.now().minute == 50:
        logging.info("Iniciando a verificação de mudanças de preço.")
        try:
            dados_diarios = executar_consulta_sql('sintetico_corte_falta.sql')
            dados_mes = executar_consulta_sql('analitico_corte_falta.sql')

            total_alteracoes = dados_diarios['codprod'].nunique()
            logging.info(f"Total de alterações detectadas: {total_alteracoes}")

            if total_alteracoes > 0:
                excel_data = gerar_excel_em_memoria(dados_diarios, dados_mes)
                corpo_email = construir_corpo_email(dados_diarios)
                assunto_email = f"Sentinela de preços - Tivemos {total_alteracoes} alterações de preços {datetime.now().strftime('%d/%m/%Y')}"
                enviar_email(assunto_email, corpo_email, excel_data)
                logging.info("Email enviado com sucesso!")
            else:
                logging.info("Nenhuma alteração detectada, e-mail não enviado.")

            logging.info("Verificação de mudanças de preço concluída com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao processar os dados: {e}")


# Loop principal
logging.info("Script iniciado.")
while True:
    verificar_mudancas()
    time.sleep(60)  # Pausa de 60 segundos antes da próxima execução
