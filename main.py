#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinela-Corte-Falta  ·  Grupo BRF1
-----------------------------------------------------------
Gera e envia:
• Indicadores Corte/Falta (% e desvio vs. média trimestral) – Ontem & Mês
• Ranking Top-5 produtos por filial
• Anexo XLSX com formatação contábil
"""

# —————————————————— IMPORTS —————————————————— #
import os, io, time, locale, argparse, logging
from datetime import datetime, timedelta, time as dt_time
from string import Template

import pandas as pd
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
from dotenv import load_dotenv
from openpyxl.styles import numbers

from config_bd import session_scope, text  # helper para Oracle

# —————————————————— CONFIG —————————————————— #
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except locale.Error:
    locale.setlocale(locale.LC_TIME, "")

load_dotenv()

logging.basicConfig(
    handlers=[
        logging.FileHandler("Sentinela-Corte-Falta.log", "a", "utf-8"),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

FROM_ADDR = "sentinela_corte_falta@aws.grupobrf1.com"
DESTS = [d for d in os.getenv("EMAIL_DESTINATARIOS", "").split(";") if d]

ses_client = boto3.client(
    "ses",
    region_name="us-east-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

AGENDA = [{"dias": [0, 1, 2, 3, 4], "horario": dt_time(8, 0)}]  # dias úteis 08h


# —————————————————— FUNÇÕES BÁSICAS —————————————————— #
def carregar_template() -> Template:
    return Template(open("email_template.html", encoding="utf-8").read())


def executar_sql_param(file: str, di: datetime, df: datetime) -> pd.DataFrame:
    sql = (
        open(os.path.join("sql", file), encoding="utf-8")
        .read()
        .replace(":DATAI", f"TO_DATE('{di:%Y-%m-%d}','YYYY-MM-DD')")
        .replace(":DATAF", f"TO_DATE('{df:%Y-%m-%d}','YYYY-MM-DD')")
    )
    with session_scope() as s:
        r = s.execute(text(sql))
        rows, cols = r.fetchall(), [c.upper() for c in r.keys()]
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


def executar_sql(file: str) -> pd.DataFrame:
    with session_scope() as s:
        txt = open(os.path.join("sql", file), encoding="utf-8").read()
        r = s.execute(text(txt))
        rows, cols = r.fetchall(), [c.upper() for c in r.keys()]
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


normalize = lambda df: df.rename(columns=str.upper)


def moeda(v: float) -> str:
    try:
        return f"R$ {v:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    except:
        return "R$ 0,00"


label_filial = lambda c: {"1": "F1 PB", "2": "F1 RN", "3": "BR"}.get(
    str(c).strip(), "Outra"
)


# —————————————————— INDICADORES (Benchmarks) —————————————————— #
def tabelas_benchmark(df_ontem: pd.DataFrame, df_mes: pd.DataFrame, tipo: str) -> str:
    """Gera HTML p/ indicadores (CORTE | FALTA) usando linha TOTAL devolvida pelo SQL."""
    tipo = tipo.upper()
    col_v = f"PVENDA_{tipo}"
    col_p = f"PCT_PERIODO_{tipo}"
    col_d = f"DESVIO_{tipo}"
    col_mt = f"MEDIA_TRIM_{tipo}"

    def tabela(df: pd.DataFrame, titulo: str) -> str:
        if df.empty:
            return "<p class='mensagem-positiva'>Sem dados.</p>"

        linhas = ""
        for r in df.itertuples(index=False):
            css = "total" if str(r.CODFILIAL) == "TOTAL" else ""
            linhas += (
                f"<tr class='{css}'>"
                f"<td>{'TOTAL' if css else label_filial(r.CODFILIAL)}</td>"
                f"<td class='valor'>{moeda(getattr(r, col_v))}</td>"
                f"<td class='qt'>{getattr(r, col_p)}</td>"
                f"<td class='qt'>{getattr(r, col_d)}</td></tr>"
            )
        return (
            f"<h3>{titulo}</h3>"
            "<table><tr><th>Filial</th><th>Valor</th><th>% Período</th><th>Desvio vs. Trim.</th></tr>"
            f"{linhas}</table>"
        )

    # legenda – ignora linha TOTAL
    leg = ", ".join(
        f"{label_filial(r.CODFILIAL)}: {getattr(r, col_mt)}"
        for r in df_mes.itertuples(index=False)
        if str(r.CODFILIAL) != "TOTAL"
    )
    legenda_html = (
        f"<p class='legenda'><em>Média Trimestral ({tipo.title()}): {leg}</em></p>"
        if leg
        else ""
    )

    return tabela(df_ontem, "Ontem") + legenda_html + tabela(df_mes, "Mês Atual")


# —————————————————— RANK TOP-5 —————————————————— #
def rank_por_filial(df: pd.DataFrame, periodo: str) -> str:
    """
    Gera HTML para o Top-5 de produtos por filial, excluindo
    produtos com quantidade ou valor zero.
    """
    if df.empty:
        return "<p class='mensagem-positiva'>Sem ranking.</p>"

    # Escolher campos de Corte ou Falta
    if "QT_CORTE" in df.columns:
        qt_field, cnt_field, val_field = "QT_CORTE", "COUNT_PED_CORTE", "PVENDA_CORTE"
    else:
        qt_field, cnt_field, val_field = "QT_FALTA", "COUNT_PED_FALTA", "PVENDA_FALTA"

    html = ""
    for cod in sorted(df["CODFILIAL"].astype(str).unique()):
        subset = df[df["CODFILIAL"] == cod]

        # Filtra já aqui: QT>0 e VAL>0
        sel = subset[(subset[qt_field] > 0) & (subset[val_field] > 0)]
        if sel.empty:
            continue

        # Agrega e soma
        top = (
            sel.groupby(["CODPROD", "DESCRICAO"])
            .agg(
                QT_UND=(qt_field, "sum"),
                QT_PED=(cnt_field, "sum"),
                VAL=(val_field, "sum"),
            )
            .reset_index()
        )
        if top.empty:
            continue

        # Ordena e pega Top 5
        top = top.sort_values("VAL", ascending=False).head(5)

        # Monta tabela HTML
        linhas = ""
        for r in top.itertuples(index=False):
            linhas += (
                f"<tr>"
                f"<td>{r.CODPROD}</td>"
                f"<td>{r.DESCRICAO}</td>"
                f"<td class='qt'>{int(r.QT_UND)}</td>"
                f"<td class='qt'>{int(r.QT_PED)}</td>"
                f"<td class='valor'>{moeda(r.VAL)}</td>"
                f"</tr>"
            )

        html += (
            f"<div class='filial-block'><h3>Top 5 {periodo} – {label_filial(cod)}</h3>"
            "<table>"
            "<tr><th>Código</th><th>Descrição</th><th>Qt Und</th><th>Qt Ped</th><th>Valor</th></tr>"
            f"{linhas}"
            "</table></div>"
        )

    return html or "<p class='mensagem-positiva'>Sem ranking.</p>"


# —————————————————— CORPO DO E-MAIL —————————————————— #
def corpo_email(bmk_ontem, bmk_mes, s_ontem, s_mes) -> str:
    tpl = carregar_template()
    ontem = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
    mes_nome = datetime.now().strftime("%B").capitalize()
    return tpl.substitute(
        data_ontem=ontem,
        mes_atual_nome=mes_nome,
        section_corte=tabelas_benchmark(bmk_ontem, bmk_mes, "CORTE"),
        section_falta=tabelas_benchmark(bmk_ontem, bmk_mes, "FALTA"),
        rank_ontem_por_filial=rank_por_filial(s_ontem, "Ontem"),
        rank_mes_por_filial=rank_por_filial(s_mes, f"Mês de {mes_nome}"),
    )


# —————————————————— EXCEL (formatação contábil) —————————————————— #
def _auto(ws):
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = (
            max((len(str(c.value)) for c in col if c.value), default=0) + 2
        )


def _fmt(ws, campos: list[str]):
    hdr = [(c.value or "").strip().upper() for c in next(ws.iter_rows(max_row=1))]
    for idx, n in enumerate(hdr, 1):
        if n in [c.upper() for c in campos]:
            for col in ws.iter_cols(min_col=idx, max_col=idx, min_row=2):
                for c in col:
                    if isinstance(c.value, (int, float)):
                        c.number_format = "R$ #,##0.00"


def gerar_xlsx(s_ontem, s_mes, a_c, a_f) -> io.BytesIO:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        ontem_str = (datetime.now() - timedelta(days=1)).strftime("%d %m %Y")
        mes_str = datetime.now().strftime("%B").capitalize()
        s_ontem.to_excel(w, sheet_name=f"Sintético ({ontem_str})", index=False)
        ws = w.sheets[f"Sintético ({ontem_str})"]
        _auto(ws)
        _fmt(ws, ["PVENDA_CORTE", "PVENDA_FALTA"])
        s_mes.to_excel(w, sheet_name=f"Sintético {mes_str}", index=False)
        ws = w.sheets[f"Sintético {mes_str}"]
        _auto(ws)
        _fmt(ws, ["PVENDA_CORTE", "PVENDA_FALTA"])
        a_c.to_excel(w, sheet_name="Analítico Corte Mês", index=False)
        ws = w.sheets["Analítico Corte Mês"]
        _auto(ws)
        _fmt(ws, ["PVENDA_CORTE"])
        a_f.to_excel(w, sheet_name="Analítico Falta Mês", index=False)
        ws = w.sheets["Analítico Falta Mês"]
        _auto(ws)
        _fmt(ws, ["PVENDA_FALTA"])
    buf.seek(0)
    return buf


# —————————————————— ENVIO —————————————————— #
def enviar_email(html, xlsx) -> bool:
    msg = MIMEMultipart()
    msg["From"] = FROM_ADDR
    msg["To"] = ",".join(DESTS)
    msg["Subject"] = f"Relatório Corte/Falta – {datetime.now():%d/%m/%Y}"
    msg["X-Priority"] = "1"
    msg.attach(MIMEText(html, "html", "utf-8"))
    part = MIMEApplication(xlsx.read(), _subtype="xlsx")
    part.add_header(
        "Content-Disposition",
        "attachment",
        filename=f"Relatório_Corte_Falta_{datetime.now():%d_%m_%Y}.xlsx",
    )
    encoders.encode_base64(part)
    msg.attach(part)
    try:
        ses_client.send_raw_email(
            Source=FROM_ADDR, Destinations=DESTS, RawMessage={"Data": msg.as_string()}
        )
        return True
    except Exception as e:
        logging.error("SES: %s", e)
        return False


# —————————————————— ROTINA PRINCIPAL —————————————————— #
def verificar():
    logging.info("=== Início verificação ===")

    ontem = datetime.now() - timedelta(days=1)
    mes_ini = datetime.now().replace(day=1)

    bmk_ontem = normalize(
        executar_sql_param("relatorio_corte_falta_benchmark.sql", ontem, ontem)
    )
    bmk_mes = normalize(
        executar_sql_param(
            "relatorio_corte_falta_benchmark.sql", mes_ini, datetime.now()
        )
    )

    s_ontem = normalize(executar_sql("sintetico_corte_falta.sql"))
    s_mes = normalize(executar_sql("sintetico_corte_falta_mes.sql"))
    a_corte = normalize(executar_sql("analitico_corte_mes.sql"))
    a_falta = normalize(executar_sql("analitico_falta_mes.sql"))

    # ---------- Resumo numérico no log ----------
    total = lambda df, col: int(df[col].sum()) if col in df.columns else 0
    valor = lambda df, col: moeda(df[col].sum()) if col in df.columns else "R$ 0,00"

    logging.info(
        "Ontem – Corte: %d und / %d ped / %s | Falta: %d und / %d ped / %s",
        total(s_ontem, "QT_CORTE"),
        total(s_ontem, "COUNT_PED_CORTE"),
        valor(s_ontem, "PVENDA_CORTE"),
        total(s_ontem, "QT_FALTA"),
        total(s_ontem, "COUNT_PED_FALTA"),
        valor(s_ontem, "PVENDA_FALTA"),
    )
    logging.info(
        "Mês – Corte: %d und / %d ped / %s | Falta: %d und / %d ped / %s",
        total(s_mes, "QT_CORTE"),
        total(s_mes, "COUNT_PED_CORTE"),
        valor(s_mes, "PVENDA_CORTE"),
        total(s_mes, "QT_FALTA"),
        total(s_mes, "COUNT_PED_FALTA"),
        valor(s_mes, "PVENDA_FALTA"),
    )
    # ---------------------------------------------

    if s_ontem.empty and s_mes.empty:
        logging.info("Nenhum dado → e-mail não enviado.")
        return

    corpo = corpo_email(bmk_ontem, bmk_mes, s_ontem, s_mes)
    xlsx = gerar_xlsx(s_ontem, s_mes, a_corte, a_falta)
    ok = enviar_email(corpo, xlsx)

    logging.info("Enviado para: %s", ", ".join(DESTS))
    logging.info("E-mail enviado: %s", "OK" if ok else "ERRO")
    logging.info("=== Fim verificação ===")


# —————————————————— SCHEDULER —————————————————— #
def _proximo():
    agora = datetime.now()
    prox = None
    for cfg in AGENDA:
        for d in cfg["dias"]:
            dt = datetime.combine(
                (agora + timedelta(days=(d - agora.weekday()) % 7)).date(),
                cfg["horario"],
            )
            if dt <= agora:
                dt += timedelta(days=7)
            prox = dt if prox is None or dt < prox else prox
    return prox


def _loop():
    while True:
        nxt = _proximo()
        logging.info("Próxima execução: %s", nxt.strftime("%d/%m/%Y %H:%M:%S"))
        time.sleep(max(0, (nxt - datetime.now()).total_seconds()))
        verificar()


# —————————————————— CLI —————————————————— #
def main():
    ap = argparse.ArgumentParser(description="Sentinela Corte/Falta")
    ap.add_argument("--modo", choices=["manual", "diario"], required=True)
    if ap.parse_args().modo == "manual":
        logging.info("Modo MANUAL")
        verificar()
    else:
        logging.info("Modo DIÁRIO")
        _loop()


if __name__ == "__main__":
    logging.info("Script iniciado.")
    main()
