#!/usr/bin/env python3
# Sentinela · Corte e Falta

import argparse
import io
import logging
import time
import os
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.sql import text
from config_bd import session_scope

from sentinela_core import (
    setup_logging,
    load_sql,
    render_email,
    read_template,
    moeda_br,
    label_filial,
    compute_next_run,
    read_env_emails,
)

# ------------------------------------------------------------
# Setup
# ------------------------------------------------------------
load_dotenv()
setup_logging("Sentinela-Corte-Falta.log")

BASE_DIR = Path(__file__).resolve().parent
AGENDA = [{"dias": [0, 1, 2, 3, 4], "horario": dt_time(8, 0)}]  # dias uteis, 08:00


# ------------------------------------------------------------
# Utils locais
# ------------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def send_email_with_attachments(
    subject: str,
    html: str,
    to: List[str],
    cc: List[str] = None,
    bcc: List[str] = None,
    attachments: List[Tuple[str, bytes]] = None,
    priority_high: bool = True,
) -> bool:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication

    user = os.getenv("EMAIL_USER")
    pwd = os.getenv("EMAIL_PASSWORD")
    host = os.getenv("OFFICE365_SMTP_SERVER", "smtp.office365.com")
    port = int(os.getenv("OFFICE365_SMTP_PORT", "587"))

    if not to:
        logging.error("Sem destinatarios (EMAIL_PARA). Cancelando envio.")
        return False

    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    if subject:
        msg["Subject"] = subject
    if priority_high:
        msg["X-Priority"] = "1"
    msg.attach(MIMEText(html, "html", "utf-8"))

    for nome, conteudo in attachments or []:
        part = MIMEApplication(
            conteudo, _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        part.add_header("Content-Disposition", "attachment", filename=nome)
        msg.attach(part)

    recipients = to + (cc or []) + (bcc or [])

    try:
        with smtplib.SMTP(host, port) as smtp:
            smtp.starttls()
            smtp.login(user, pwd)
            smtp.sendmail(user, recipients, msg.as_string())
        logging.info(
            "Email enviado -> To: %s; Cc: %s; Bcc: %s",
            "; ".join(to),
            "; ".join(cc) if cc else "-",
            "; ".join(bcc) if bcc else "-",
        )
        return True
    except Exception as exc:
        logging.error("Falha no envio: %s", exc)
        return False


def to_xlsx_bytes_multiplas_abas(
    s_ontem: pd.DataFrame,
    s_mes: pd.DataFrame,
    a_corte: pd.DataFrame,
    a_falta: pd.DataFrame,
) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        ontem_str = (datetime.now() - timedelta(days=1)).strftime("%d %m %Y")
        mes_str = datetime.now().strftime("%B").capitalize()
        s_ontem.to_excel(w, sheet_name=f"Sintetico ({ontem_str})", index=False)
        s_mes.to_excel(w, sheet_name=f"Sintetico {mes_str}", index=False)
        a_corte.to_excel(w, sheet_name="Analitico Corte Mes", index=False)
        a_falta.to_excel(w, sheet_name="Analitico Falta Mes", index=False)
    buf.seek(0)
    return buf.getvalue()


# ------------------------------------------------------------
# SQL
# ------------------------------------------------------------
def executar_sql_param(arquivo_sql: str, di: datetime, df: datetime) -> pd.DataFrame:
    sql = (
        load_sql(arquivo_sql)
        .replace(":DATAI", f"TO_DATE('{di:%Y-%m-%d}','YYYY-MM-DD')")
        .replace(":DATAF", f"TO_DATE('{df:%Y-%m-%d}','YYYY-MM-DD')")
    )
    with session_scope() as s:
        r = s.execute(text(sql))
        rows, cols = r.fetchall(), [c.upper() for c in r.keys()]
        return (
            normalize_columns(pd.DataFrame(rows, columns=cols))
            if rows
            else pd.DataFrame(columns=cols)
        )


def executar_sql(arquivo_sql: str) -> pd.DataFrame:
    with session_scope() as s:
        r = s.execute(text(load_sql(arquivo_sql)))
        rows, cols = r.fetchall(), [c.upper() for c in r.keys()]
        return (
            normalize_columns(pd.DataFrame(rows, columns=cols))
            if rows
            else pd.DataFrame(columns=cols)
        )


# ------------------------------------------------------------
# Blocos HTML
# ------------------------------------------------------------
def _tabela_indicador(df: pd.DataFrame, tipo: str, titulo_bloco: str) -> str:
    tipo = tipo.upper()
    col_v = f"PVENDA_{tipo}"
    col_p = f"PCT_PERIODO_{tipo}"
    col_d = f"DESVIO_{tipo}"

    if tipo == "CORTE":
        valor_hdr, pct_hdr, desv_hdr = (
            "Valor Cortado (R$)",
            "Corte no periodo (%)",
            "Desvio vs. Meta",
        )
    else:
        valor_hdr, pct_hdr, desv_hdr = (
            "Valor em Falta (R$)",
            "Falta no periodo (%)",
            "Desvio vs. Trimestre",
        )

    if df.empty:
        return (
            f"<h3>{titulo_bloco}</h3>"
            "<div class='tblWrap'><table class='data'>"
            f"<tr><th>Filial</th><th>{valor_hdr}</th><th>{pct_hdr}</th><th>{desv_hdr}</th></tr>"
            "<tr><td colspan='4'><strong>Sem dados.</strong></td></tr>"
            "</table></div>"
        )

    html = [f"<h3>{titulo_bloco}</h3><div class='tblWrap'><table class='data'>"]
    html.append(
        f"<tr><th>Filial</th><th>{valor_hdr}</th><th>{pct_hdr}</th><th>{desv_hdr}</th></tr>"
    )
    for _, r in df.iterrows():
        cod = r["CODFILIAL"]
        filial = "TOTAL" if str(cod) == "TOTAL" else label_filial(cod)
        val = moeda_br(r.get(col_v, 0))
        pct = r.get(col_p, "0,00%") or "0,00%"
        des = r.get(col_d, "0%") or "0%"
        cls = " class='bad'" if "ACIMA" in str(des).upper() else ""
        html.append(
            f"<tr{cls}><td>{filial}</td><td>{val}</td><td>{pct}</td><td>{des}</td></tr>"
        )
    html.append("</table></div>")
    return "".join(html)


def tabelas_benchmark(df_ontem: pd.DataFrame, df_mes: pd.DataFrame, tipo: str) -> str:
    bloco_ontem = _tabela_indicador(df_ontem, tipo, "Ontem")
    bloco_mes = _tabela_indicador(df_mes, tipo, "Mes Atual")
    if tipo.upper() == "CORTE":
        legenda = "<p class='legend'><em>Meta de Corte: 0,03%</em></p>"
    else:
        if not df_mes.empty and "MEDIA_TRIM_FALTA" in df_mes.columns:
            pares = []
            for _, rr in df_mes.iterrows():
                if str(rr["CODFILIAL"]) != "TOTAL":
                    pares.append(
                        f"{label_filial(rr['CODFILIAL'])}: {rr['MEDIA_TRIM_FALTA']}"
                    )
            legenda = (
                f"<p class='legend'><em>Media Trimestral por Filial (Falta): {', '.join(pares)}</em></p>"
                if pares
                else ""
            )
        else:
            legenda = ""
    return bloco_ontem + legenda + bloco_mes


def rank_por_filial(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p class='ok'>Sem ranking.</p>"
    if "QT_CORTE" in df.columns:
        qt_field, cnt_field, val_field = "QT_CORTE", "COUNT_PED_CORTE", "PVENDA_CORTE"
    else:
        qt_field, cnt_field, val_field = "QT_FALTA", "COUNT_PED_FALTA", "PVENDA_FALTA"

    html = []
    for cod in sorted(df["CODFILIAL"].astype(str).unique()):
        grp = df[(df["CODFILIAL"] == cod) & (df[qt_field] > 0) & (df[val_field] > 0)]
        if grp.empty:
            continue
        top = (
            grp.groupby(["CODPROD", "DESCRICAO"])
            .agg(
                QT_UND=(qt_field, "sum"),
                QT_PED=(cnt_field, "sum"),
                VAL=(val_field, "sum"),
            )
            .reset_index()
            .sort_values("VAL", ascending=False)
            .head(5)
        )
        html.append(
            f"<h3>{label_filial(cod)}</h3><div class='tblWrap'><table class='data'>"
        )
        html.append(
            "<tr><th>Codigo</th><th>Descricao</th><th>Qt Und</th><th>Qt Ped</th><th>Valor</th></tr>"
        )
        for r in top.itertuples(index=False):
            html.append(
                f"<tr><td>{r.CODPROD}</td><td>{r.DESCRICAO}</td>"
                f"<td>{int(r.QT_UND)}</td><td>{int(r.QT_PED)}</td><td>{moeda_br(r.VAL)}</td></tr>"
            )
        html.append("</table></div>")
    return "".join(html) or "<p class='ok'>Sem ranking.</p>"


def corpo_email(assunto: str, bmk_ontem, bmk_mes, s_ontem, s_mes) -> str:
    mes_nome = datetime.now().strftime("%B").capitalize()
    partes = []
    partes.append("<h3>Indicadores de Corte (Meta fixa 0,03%)</h3>")
    partes.append(
        f"<div class='tblWrap'>{tabelas_benchmark(bmk_ontem, bmk_mes, 'CORTE')}</div>"
    )
    partes.append("<h3>Indicadores de Falta</h3>")
    partes.append(
        f"<div class='tblWrap'>{tabelas_benchmark(bmk_ontem, bmk_mes, 'FALTA')}</div>"
    )
    partes.append("<h3>Top 5 por Filial - Ontem</h3>")
    partes.append(f"<div class='tblWrap'>{rank_por_filial(s_ontem)}</div>")
    partes.append(f"<h3>Top 5 por Filial - Mes {mes_nome}</h3>")
    partes.append(f"<div class='tblWrap'>{rank_por_filial(s_mes)}</div>")

    tpl = read_template("email_base.html")
    footer = "Este e um e-mail automatico - nao responda."
    return render_email(tpl, assunto, "".join(partes), footer, extra_css=None)


# ------------------------------------------------------------
# Regras / Execucao
# ------------------------------------------------------------
def _tem_movimento(df: pd.DataFrame, qty_col: str, cnt_col: str, val_col: str) -> bool:
    tem_qt = (
        qty_col in df.columns
        and pd.to_numeric(df[qty_col], errors="coerce").fillna(0).sum() > 0
    )
    tem_ped = (
        cnt_col in df.columns
        and pd.to_numeric(df[cnt_col], errors="coerce").fillna(0).sum() > 0
    )
    tem_val = (
        val_col in df.columns
        and pd.to_numeric(df[val_col], errors="coerce").fillna(0).sum() > 0
    )
    return tem_qt or tem_ped or tem_val


def verificar():
    logging.info("Inicio verificacao")

    ontem = datetime.now() - timedelta(days=1)
    mes_ini = datetime.now().replace(day=1)

    bmk_ontem = executar_sql_param("relatorio_corte_falta_benchmark.sql", ontem, ontem)
    bmk_mes = executar_sql_param(
        "relatorio_corte_falta_benchmark.sql", mes_ini, datetime.now()
    )

    s_ontem = executar_sql("sintetico_corte_falta.sql")
    s_mes = executar_sql("sintetico_corte_falta_mes.sql")
    a_corte = executar_sql("analitico_corte_mes.sql")
    a_falta = executar_sql("analitico_falta_mes.sql")

    corte_ok = _tem_movimento(s_ontem, "QT_CORTE", "COUNT_PED_CORTE", "PVENDA_CORTE")
    falta_ok = _tem_movimento(s_ontem, "QT_FALTA", "COUNT_PED_FALTA", "PVENDA_FALTA")

    if not (corte_ok and falta_ok):
        motivo = []
        if not corte_ok:
            motivo.append("sem CORTE")
        if not falta_ok:
            motivo.append("sem FALTA")
        logging.info(
            "Criterio de envio nao atendido (ontem %s) - e-mail nao enviado.",
            " e ".join(motivo) or "sem dados",
        )
        logging.info("Fim verificacao")
        return

    if s_ontem.empty and s_mes.empty:
        logging.info("Nenhum dado - e-mail nao enviado.")
        logging.info("Fim verificacao")
        return

    assunto = f"Sentinela · Corte e Falta - {datetime.now():%d/%m/%Y %H:%M}"
    html = corpo_email(assunto, bmk_ontem, bmk_mes, s_ontem, s_mes)
    anexo = to_xlsx_bytes_multiplas_abas(s_ontem, s_mes, a_corte, a_falta)

    dest = read_env_emails()
    ok = send_email_with_attachments(
        subject=assunto,
        html=html,
        to=dest["to"],
        cc=dest["cc"],
        bcc=dest["bcc"],
        attachments=[(f"Sentinela_Corte_e_Falta_{datetime.now():%Y%m%d}.xlsx", anexo)],
        priority_high=True,
    )

    logging.info("E-mail enviado: %s", "OK" if ok else "ERRO")
    logging.info("Fim verificacao")


def _loop():
    while True:
        nxt = compute_next_run(AGENDA)
        logging.info("Proxima execucao: %s", nxt.strftime("%d/%m/%Y %H:%M:%S"))
        time.sleep(max(0, (nxt - datetime.now()).total_seconds()))
        verificar()


def main():
    ap = argparse.ArgumentParser(description="Sentinela · Corte e Falta")
    ap.add_argument("--modo", choices=["manual", "diario"], required=True)
    modo = ap.parse_args().modo
    if modo == "manual":
        logging.info("Modo MANUAL")
        verificar()
    else:
        logging.info("Modo DIARIO")
        _loop()


if __name__ == "__main__":
    main()
