# main.py
import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import io
from dotenv import load_dotenv
from config_bd import SessionLocal,session_scope

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

def executar_consulta_sql(nome_arquivo_sql):
    # Construir o caminho completo do arquivo SQL
    caminho_arquivo_sql = os.path.join(os.getcwd(), "sql", nome_arquivo_sql)

    try:
        with session_scope() as session:
            with open(caminho_arquivo_sql, 'r') as file:
                query = file.read()
            result = pd.read_sql(query, session.bind)
        return result
    except Exception as e:
        logging.error(f"Erro ao executar consulta SQL: {e}")
        # Caso ocorra um erro não relacionado ao SQLAlchemy
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro




def auto_ajustar_colunas(worksheet):
    for col in worksheet.columns:
        max_length = 0
        column = col[0].column_letter  # Get the column name
        for cell in col:
            try:  # Necessary to avoid error on empty cells
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2)
        worksheet.column_dimensions[column].width = adjusted_width

def gerar_excel_em_memoria(dados_sintetico, dados_analitico, dados_sintetico_mes):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Obtendo a data de ontem e o mês atual
        data_ontem = (datetime.now() - pd.Timedelta(days=1)).strftime('%d %m %Y')
        mes_atual = datetime.now().strftime('%B')  # Mês atual em formato textual

        # Nomeando as abas
        nome_aba_sintetico = f"Sintético ({data_ontem})"
        nome_aba_analitico = f"Analítico ({data_ontem})"
        nome_aba_sintetico_mes = f"Sintético {mes_atual}"  # Nome da aba com mês atual

        # Dados sintéticos
        dados_sintetico.to_excel(writer, sheet_name=nome_aba_sintetico, index=False)
        wb = writer.book
        auto_ajustar_colunas(wb[nome_aba_sintetico])

        # Dados analíticos
        dados_analitico.to_excel(writer, sheet_name=nome_aba_analitico, index=False)
        auto_ajustar_colunas(wb[nome_aba_analitico])

        # Dados sintéticos do mês
        dados_sintetico_mes.to_excel(writer, sheet_name=nome_aba_sintetico_mes, index=False)
        auto_ajustar_colunas(wb[nome_aba_sintetico_mes])
    
    output.seek(0)
    return output



def formatar_moeda_br(valor):
    return f"R$ {valor:,.2f}".replace(',', 'v').replace('.', ',').replace('v', '.')

def construir_corpo_email(dados):
    # Obtendo a data de referência (dia anterior)
    data_referencia = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

    # Convertendo os nomes das colunas para minúsculas
    dados.columns = [col.lower() for col in dados.columns]

    # Certifique-se de que a coluna 'codfilial' esteja em formato string
    dados['codfilial'] = dados['codfilial'].astype(str)

    # Agrupando dados por 'codfilial'
    agrupamento_filial = dados.groupby('codfilial')

    tabelas_html = ""
    for filial, sumario in agrupamento_filial[['qt_falta', 'qt_corte', 'pvenda_falta', 'pvenda_corte']].sum().iterrows():
        nome_empresa = "Farmaum PB" if filial == '1' else "Farmaum RN" if filial == '2' else "Brasil" if filial == '3' else "Outra"
        tabelas_html += f'''
        <div class="table-container">
            <table>
                <tr><th colspan="2" style="background-color: #0056b3; color: white; text-align: center;">{nome_empresa}</th></tr>
                <tr class="zoomable"><td>Quantidade Total de Faltas</td><td><b>{int(sumario['qt_falta'])}</b></td></tr>
                <tr class="zoomable"><td>Quantidade Total de Cortes</td><td><b>{int(sumario['qt_corte'])}</b></td></tr>
                <tr class="zoomable"><td>Valor Total de Faltas</td><td><b>{formatar_moeda_br(sumario['pvenda_falta'])}</b></td></tr>
                <tr class="zoomable"><td>Valor Total de Cortes</td><td><b>{formatar_moeda_br(sumario['pvenda_corte'])}</b></td></tr>
            </table>
        </div>
        '''

    # Montagem do corpo do e-mail usando f-strings e estilo CSS
    corpo_email = f'''
    <html>
    <head>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                font-size: 14px;
                color: #333;
                padding: 20px;
                line-height: 1.6;
            }}
            h1 {{
                color: #0056b3;
                text-align: center;
                font-size: 24px;
                margin-bottom: 10px;
            }}
            .table-container {{
                perspective: 600px;
                margin-bottom: 10px;
            }}
            table {{
                width: 70%;
                margin: auto;
                border-collapse: collapse;
            }}
            .zoomable:hover {{
                transform: scale(1.05);
                transition: transform 0.2s ease;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 12px 15px;
                text-align: center;
                font-size: 14px;
                background-color: #ffffff;
                color: #333;
            }}
            th {{
                background-color: #007bff;
                color: #ffffff;
                font-weight: normal;
            }}
            tr:nth-child(odd) {{
                background-color: #f2f2f2;
            }}
            @media (prefers-color-scheme: dark) {{
                body {{
                    color: #f1f1f1;
                }}
                table, th, td {{
                    border-color: #555;
                }}
                th, tr:nth-child(odd) {{
                    background-color: #3a3a3a;
                }}
            }}
            footer {{
                text-align: center;
                font-size: 12px;
                margin-top: 30px;
                padding-top: 10px;
                border-top: 1px solid #ccc;
                color: #777;
            }}
        </style>
    </head>
    <body>
        <h1>Relatório de Corte e Falta de Itens Ref {data_referencia}</h1>
        {tabelas_html}
        <footer>
            Mensagem automática, não responda.<br>
            Desenvolvido pelo TI do Grupo BRF1.
        </footer>
    </body>
    </html>
    '''
    return corpo_email

def enviar_email(assunto, corpo, excel_data):
    try:
        destinatarios = os.getenv("EMAIL_DESTINATARIOS").split(';')

        msg = MIMEMultipart()
        msg['From'] = 'sentinela_corte_falta@aws.grupobrf1.com'
        msg['To'] = ', '.join(destinatarios)
        msg['Subject'] = assunto

        msg.attach(MIMEText(corpo, 'html'))

        nome_arquivo_excel = f"Relatório de Corte e Falta {datetime.now().strftime('%d %m %Y')}.xlsx"
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
    if datetime.now().hour == 8 and datetime.now().minute == 51:
        logging.info("Iniciando a verificação de corte e falta de itens.")
        try:
            dados_diarios = executar_consulta_sql('sintetico_corte_falta.sql')
            dados_mes = executar_consulta_sql('analitico_corte_falta.sql')
            dados_sintetico_mes = executar_consulta_sql('sintetico_corte_falta_mes.sql')

            total_alteracoes = dados_diarios['codprod'].nunique()
            logging.info(f"Total de itens com corte e falta detectados: {total_alteracoes}")

            if total_alteracoes > 0:
                # Passando os novos dados para a função
                excel_data = gerar_excel_em_memoria(dados_diarios, dados_mes, dados_sintetico_mes)
                corpo_email = construir_corpo_email(dados_diarios)

                # Ajuste para obter a data de ontem
                data_ontem = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
                assunto_email = f"Relatório de Corte e Falta de Itens - {data_ontem}"

                enviar_email(assunto_email, corpo_email, excel_data)
                logging.info("Email enviado com sucesso!")
            else:
                logging.info("Nenhum item com corte ou falta detectado, e-mail não enviado.")

            logging.info("Verificação de corte e falta de itens concluída com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao processar os dados: {e}")



# Loop principal
logging.info("Script iniciado.")
while True:
    verificar_mudancas()
    time.sleep(60)  # Pausa de 60 segundos antes da próxima execução
