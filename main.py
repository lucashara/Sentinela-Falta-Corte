import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
import locale
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
import io
from dotenv import load_dotenv
from config_bd import SessionLocal, session_scope, text
import argparse

# Definir o locale para português do Brasil
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except locale.Error:
    # Se não for possível definir o locale, utilizar o locale padrão do sistema
    locale.setlocale(locale.LC_TIME, "")

# Carrega variáveis de ambiente
load_dotenv()

# Configurações de log
logging.basicConfig(
    handlers=[
        logging.FileHandler("Sentinela-Corte-Falta.log", "a", encoding="utf-8"),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

# Configuração do cliente SES da AWS
ses_client = boto3.client(
    "ses",
    region_name="us-east-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# Agendamento de execuções
# Dias: 0=Segunda, 6=Domingo
agenda = [
    {"dias": [0, 1, 2, 3, 4], "horario": dt_time(8, 0)},  # Dias de semana às 08:00
]


def executar_consulta_sql(nome_arquivo_sql):
    """
    Executa a consulta SQL a partir de um arquivo e retorna os resultados em um DataFrame.
    """
    caminho_arquivo_sql = os.path.join(os.getcwd(), "sql", nome_arquivo_sql)

    try:
        with session_scope() as session:
            with open(caminho_arquivo_sql, "r", encoding="utf-8") as file:
                raw_sql = file.read()
            query = text(raw_sql)
            result = session.execute(query)

            # Verifica se há resultados na consulta
            rows = result.fetchall()
            if rows:
                df = pd.DataFrame(rows)
                df.columns = result.keys()
            else:
                df = pd.DataFrame(columns=result.keys())

        return df
    except Exception as e:
        logging.error(f"Erro ao executar consulta SQL ({nome_arquivo_sql}): {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro


def normalize_columns(df):
    df.columns = [col.strip().upper() for col in df.columns]
    return df


def auto_ajustar_colunas(worksheet):
    """
    Ajusta automaticamente a largura das colunas no Excel.
    """
    for col in worksheet.columns:
        max_length = 0
        column = col[0].column_letter  # Nome da coluna
        for cell in col:
            try:  # Necessário para evitar erro em células vazias
                if cell.value and len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = max_length + 2
        worksheet.column_dimensions[column].width = adjusted_width


def formatar_colunas_moeda(worksheet, colunas):
    """
    Aplica o formato de moeda contábil às colunas especificadas no worksheet.
    """
    from openpyxl.styles import numbers

    colunas = [col.strip().upper() for col in colunas]
    # Obter os nomes das colunas no worksheet
    header = [
        cell.value.strip().upper() if isinstance(cell.value, str) else ""
        for cell in next(worksheet.iter_rows(min_row=1, max_row=1))
    ]
    for idx, col_name in enumerate(header):
        if col_name in colunas:
            # Aplicar formatação na coluna inteira (exceto cabeçalho)
            for cell in worksheet.iter_cols(
                min_col=idx + 1, max_col=idx + 1, min_row=2
            ):
                for c in cell:
                    c.number_format = "R$ #,##0.00"


def gerar_excel_em_memoria(
    dados_sintetico, dados_sintetico_mes, dados_analitico_corte, dados_analitico_falta
):
    """
    Gera um arquivo Excel em memória com múltiplas abas.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Obtendo a data de ontem e o mês atual
        data_ontem = (datetime.now() - timedelta(days=1)).strftime("%d %m %Y")
        mes_atual = datetime.now().strftime("%B").capitalize()  # Mês atual em português

        # Nomeando as abas em português
        nome_aba_sintetico = f"Sintético ({data_ontem})"
        nome_aba_sintetico_mes = f"Sintético {mes_atual}"
        nome_aba_analitico_corte = "Analítico Corte Mês"
        nome_aba_analitico_falta = "Analítico Falta Mês"

        # Dados sintéticos de ontem
        dados_sintetico.to_excel(writer, sheet_name=nome_aba_sintetico, index=False)
        wb = writer.book
        ws_sintetico = writer.sheets[nome_aba_sintetico]
        auto_ajustar_colunas(ws_sintetico)
        formatar_colunas_moeda(ws_sintetico, ["PVENDA_FALTA", "PVENDA_CORTE"])

        # Dados sintéticos do mês
        dados_sintetico_mes.to_excel(
            writer, sheet_name=nome_aba_sintetico_mes, index=False
        )
        ws_sintetico_mes = writer.sheets[nome_aba_sintetico_mes]
        auto_ajustar_colunas(ws_sintetico_mes)
        formatar_colunas_moeda(ws_sintetico_mes, ["PVENDA_FALTA", "PVENDA_CORTE"])

        # Dados analíticos de corte
        dados_analitico_corte.to_excel(
            writer, sheet_name=nome_aba_analitico_corte, index=False
        )
        ws_analitico_corte = writer.sheets[nome_aba_analitico_corte]
        auto_ajustar_colunas(ws_analitico_corte)
        formatar_colunas_moeda(ws_analitico_corte, ["PVENDA_CORTE"])

        # Dados analíticos de falta
        dados_analitico_falta.to_excel(
            writer, sheet_name=nome_aba_analitico_falta, index=False
        )
        ws_analitico_falta = writer.sheets[nome_aba_analitico_falta]
        auto_ajustar_colunas(ws_analitico_falta)
        formatar_colunas_moeda(ws_analitico_falta, ["PVENDA_FALTA"])

    output.seek(0)
    return output


def formatar_como_moeda(valor):
    """
    Formata um valor numérico como moeda em Real Brasileiro.
    """
    try:
        return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    except:
        return "R$ 0,00"


def nome_filial(codigo):
    """
    Mapeia o código da filial para o nome abreviado.
    """
    mapeamento = {"1": "F1 PB", "2": "F1 RN", "3": "BR"}
    return mapeamento.get(str(codigo).strip(), "Outra")


def construir_corpo_email(dados_ontem, dados_mes, data_hora_atual):
    """
    Constrói o corpo do e-mail em HTML, incluindo estilos e tabelas formatadas.
    """
    estilo_corpo = """
    <style>
        body {
            font-family: Arial, sans-serif;
            font-size: 16px;
        }
        h1, h2, h3 {
            text-align: center;
        }
        p {
            text-align: center;
        }
        .resumo {
            font-weight: bold;
        }
        .footer {
            margin-top: 20px;
            text-align: center;
        }
        table {
            width: 60%;
            margin: 20px auto;
            border-collapse: collapse;
            border-radius: 10px;
            overflow: hidden;
        }
        th, td {
            padding: 8px;
            text-align: center;
            border: 1px solid #ddd;
        }
        th {
            background-color: #282248;
            color: white;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        tr:hover {
            background-color: #ddd;
        }
        .quantidade, .valor {
            font-weight: bold;
            color: red;
        }
        .mensagem-positiva {
            text-align: center;
            font-size: 16px;
            color: green;
            font-weight: bold;
        }
    </style>
    """

    # Gerar tabelas
    tabela_ontem_html = gerar_tabela_html(
        dados_ontem, "Resumo de Corte e Falta de Ontem"
    )
    tabela_mes_html = gerar_tabela_html(dados_mes, "Resumo Acumulado do Mês Atual")

    corpo_email = f"""
    {estilo_corpo}
    <div>
        <h1>Relatório de Corte e Falta de Itens - {data_hora_atual}</h1>
        {tabela_ontem_html}
        {tabela_mes_html}
        <div class='footer'>
            <p><em>Este é um e-mail automático. Por favor, não responda.<br>Atenciosamente, Equipe de TI - Grupo BRF1.</em></p>
        </div>
    </div>
    """
    return corpo_email


def gerar_tabela_html(dados, titulo):
    """
    Gera o código HTML para a tabela de resumo, incluindo total de quantidade e valor.
    """
    if dados.empty:
        return "<p class='mensagem-positiva'>Nenhum dado encontrado para esta consulta. Tudo está em ordem!</p>"

    dados = normalize_columns(dados)
    dados["CODFILIAL"] = dados["CODFILIAL"].astype(str)

    # Agrupar dados por filial
    agrupados = (
        dados.groupby("CODFILIAL")
        .agg(
            QT_CORTE=("QT_CORTE", "sum"),
            PVENDA_CORTE=("PVENDA_CORTE", "sum"),
            QT_FALTA=("QT_FALTA", "sum"),
            PVENDA_FALTA=("PVENDA_FALTA", "sum"),
        )
        .reset_index()
    )

    # Calcular totais gerais
    total_qt_corte = agrupados["QT_CORTE"].sum()
    total_pvenda_corte = agrupados["PVENDA_CORTE"].sum()
    total_qt_falta = agrupados["QT_FALTA"].sum()
    total_pvenda_falta = agrupados["PVENDA_FALTA"].sum()

    # Construir HTML da tabela
    colunas_html = "<th>Filial</th><th>Tipo</th><th>Quantidade</th><th>Valor (R$)</th>"
    linhas_html = ""

    for _, row in agrupados.iterrows():
        filial_nome = nome_filial(row["CODFILIAL"])
        linhas_html += f"""
        <tr>
            <td rowspan="2">{filial_nome}</td>
            <td>Corte</td>
            <td class='quantidade'>{int(row['QT_CORTE'])}</td>
            <td class='valor'>{formatar_como_moeda(row['PVENDA_CORTE'])}</td>
        </tr>
        <tr>
            <td>Falta</td>
            <td class='quantidade'>{int(row['QT_FALTA'])}</td>
            <td class='valor'>{formatar_como_moeda(row['PVENDA_FALTA'])}</td>
        </tr>
        """

    # Adicionar total geral
    linhas_html += f"""
    <tr style='font-weight:bold;'>
        <td rowspan="2">Total Geral</td>
        <td>Corte</td>
        <td class='quantidade'>{int(total_qt_corte)}</td>
        <td class='valor'>{formatar_como_moeda(total_pvenda_corte)}</td>
    </tr>
    <tr style='font-weight:bold;'>
        <td>Falta</td>
        <td class='quantidade'>{int(total_qt_falta)}</td>
        <td class='valor'>{formatar_como_moeda(total_pvenda_falta)}</td>
    </tr>
    """

    tabela_html = f"""
    <h2>{titulo}</h2>
    <table>
        <tr>{colunas_html}</tr>
        {linhas_html}
    </table>
    """
    return tabela_html


def enviar_email(assunto, corpo, excel_data):
    """
    Envia um e-mail com o assunto, corpo e anexo fornecidos.
    """
    try:
        destinatarios = os.getenv("EMAIL_DESTINATARIOS").split(";")

        msg = MIMEMultipart()
        msg["From"] = "sentinela_corte_falta@aws.grupobrf1.com"
        msg["To"] = ", ".join(destinatarios)
        msg["Subject"] = assunto
        msg["X-Priority"] = "1"  # Alta prioridade

        msg.attach(MIMEText(corpo, "html", "utf-8"))

        nome_arquivo_excel = (
            f"Relatório de Corte e Falta {datetime.now().strftime('%d %m %Y')}.xlsx"
        )
        part = MIMEApplication(excel_data.read(), _subtype="xlsx")
        part.add_header(
            "Content-Disposition", "attachment", filename=nome_arquivo_excel
        )
        encoders.encode_base64(part)  # Certifica que o anexo é enviado corretamente
        msg.attach(part)

        ses_client.send_raw_email(
            Source=msg["From"],
            Destinations=destinatarios,
            RawMessage={"Data": msg.as_string()},
        )
    except Exception as e:
        logging.error(f"Erro ao enviar e-mail: {e}")


def verificar_corte_falta():
    """
    Realiza a verificação de corte e falta de itens e envia o relatório por e-mail se houver dados.
    """
    logging.info("Iniciando a verificação de corte e falta de itens.")
    try:
        # Executando as consultas SQL
        dados_sintetico = executar_consulta_sql("sintetico_corte_falta.sql")
        dados_sintetico_mes = executar_consulta_sql("sintetico_corte_falta_mes.sql")
        dados_analitico_corte = executar_consulta_sql("analitico_corte_mes.sql")
        dados_analitico_falta = executar_consulta_sql("analitico_falta_mes.sql")

        # Normalizar nomes de colunas
        dados_sintetico = normalize_columns(dados_sintetico)
        dados_sintetico_mes = normalize_columns(dados_sintetico_mes)
        dados_analitico_corte = normalize_columns(dados_analitico_corte)
        dados_analitico_falta = normalize_columns(dados_analitico_falta)

        # Logs com análise de ontem
        if not dados_sintetico.empty:
            qt_corte_ontem = int(dados_sintetico["QT_CORTE"].sum())
            valor_corte_ontem = dados_sintetico["PVENDA_CORTE"].sum()
            qt_falta_ontem = int(dados_sintetico["QT_FALTA"].sum())
            valor_falta_ontem = dados_sintetico["PVENDA_FALTA"].sum()
            logging.info(
                f"Análise de Ontem - Corte: {qt_corte_ontem} itens, Valor: {formatar_como_moeda(valor_corte_ontem)}"
            )
            logging.info(
                f"Análise de Ontem - Falta: {qt_falta_ontem} itens, Valor: {formatar_como_moeda(valor_falta_ontem)}"
            )
        else:
            logging.info("Nenhum dado de corte ou falta encontrado para ontem.")

        # Logs com análise acumulada do mês
        if not dados_sintetico_mes.empty:
            qt_corte_mes = int(dados_sintetico_mes["QT_CORTE"].sum())
            valor_corte_mes = dados_sintetico_mes["PVENDA_CORTE"].sum()
            qt_falta_mes = int(dados_sintetico_mes["QT_FALTA"].sum())
            valor_falta_mes = dados_sintetico_mes["PVENDA_FALTA"].sum()
            logging.info(
                f"Análise Acumulada do Mês - Corte: {qt_corte_mes} itens, Valor: {formatar_como_moeda(valor_corte_mes)}"
            )
            logging.info(
                f"Análise Acumulada do Mês - Falta: {qt_falta_mes} itens, Valor: {formatar_como_moeda(valor_falta_mes)}"
            )
        else:
            logging.info("Nenhum dado de corte ou falta encontrado para o mês atual.")

        # Verifica se há dados para enviar
        if not dados_sintetico.empty or not dados_sintetico_mes.empty:
            # Gerando o arquivo Excel em memória
            excel_data = gerar_excel_em_memoria(
                dados_sintetico,
                dados_sintetico_mes,
                dados_analitico_corte,
                dados_analitico_falta,
            )

            data_hora_atual = datetime.now().strftime("%d/%m/%Y %H:%M")
            corpo_email = construir_corpo_email(
                dados_sintetico, dados_sintetico_mes, data_hora_atual
            )

            # Definindo o assunto do e-mail
            assunto_email = f"Relatório de Corte e Falta de Itens - {datetime.now().strftime('%d/%m/%Y')}"

            # Enviando o e-mail
            enviar_email(assunto_email, corpo_email, excel_data)
            logging.info("E-mail enviado com sucesso!")
        else:
            logging.info("Nenhum dado encontrado, e-mail não enviado.")

        logging.info("Verificação de corte e falta de itens concluída com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao processar os dados: {e}")


def obter_proximo_horario_agendado(agenda):
    """
    Calcula o próximo horário agendado para execução baseado na agenda fornecida.
    """
    agora = datetime.now()
    proxima_execucao = None

    for entrada in agenda:
        dias = entrada["dias"]
        horario = entrada["horario"]

        for dia in dias:
            dias_ahead = (dia - agora.weekday()) % 7
            data_agendada = agora + timedelta(days=dias_ahead)
            datetime_agendado = datetime.combine(data_agendada.date(), horario)

            if datetime_agendado <= agora:
                datetime_agendado += timedelta(days=7)  # Move para a próxima semana

            if proxima_execucao is None or datetime_agendado < proxima_execucao:
                proxima_execucao = datetime_agendado

    return proxima_execucao


def verificar_mudancas_diarias():
    """
    Verifica periodicamente conforme a agenda definida.
    """
    while True:
        proxima_execucao = obter_proximo_horario_agendado(agenda)
        tempo_espera = (proxima_execucao - datetime.now()).total_seconds()
        logging.info(
            f"Próxima execução agendada para: {proxima_execucao.strftime('%d/%m/%Y %H:%M:%S')}"
        )
        if tempo_espera > 0:
            time.sleep(tempo_espera)
        verificar_corte_falta()


def main():
    """
    Função principal para gerenciar a execução do script.
    """
    # Parser de argumentos
    parser = argparse.ArgumentParser(description="Script de Sentinela de Corte e Falta")
    parser.add_argument(
        "--modo",
        choices=["manual", "diario"],
        required=True,
        help="Modo de execução: manual ou diario",
    )

    args = parser.parse_args()

    # Lógica principal
    if args.modo == "manual":
        logging.info("Executando em modo manual.")
        verificar_corte_falta()
    elif args.modo == "diario":
        logging.info("Executando em modo diário.")
        verificar_mudancas_diarias()
    else:
        logging.error("Modo inválido. Utilize --modo manual ou --modo diario.")
        exit(1)


if __name__ == "__main__":
    logging.info("Script iniciado.")
    main()
