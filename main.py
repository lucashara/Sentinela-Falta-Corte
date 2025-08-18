#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinela – Corte e Falta  ·  Grupo BRF1
------------------------------------------------------------------
Gera e envia:
• Indicadores de CORTE (meta fixa 0,03%) e FALTA (% e desvio vs. média trimestral) – Ontem & Mês
• Ranking Top-5 produtos por filial
• Anexo XLSX com formatação contábil
------------------------------------------------------------------
"""

# ------------------------------ IMPORTS ------------------------------ #
import os, io, time, locale, argparse, logging, smtplib
from datetime import datetime, timedelta, time as dt_time
from string import Template

import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
from dotenv import load_dotenv
from openpyxl.styles import numbers  # noqa: F401

from config_bd import session_scope, text  # helper p/ Oracle

# ------------------------------ CONFIG ------------------------------ #
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

# SMTP Office 365
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASSWORD")
SMTP_HOST = os.getenv("OFFICE365_SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("OFFICE365_SMTP_PORT", "587"))

FROM_ADDR = EMAIL_USER
DESTS = [d for d in os.getenv("EMAIL_DESTINATARIOS", "").split(";") if d]

# Agenda: dias úteis, 08:00
AGENDA = [{"dias": [0, 1, 2, 3, 4], "horario": dt_time(8, 0)}]


# ------------------------------ DB UTILS ------------------------------ #
def carregar_template() -> Template:
    """Carrega o template HTML do e-mail."""
    return Template(open("email_template.html", encoding="utf-8").read())


def executar_sql_param(file: str, di: datetime, df: datetime) -> pd.DataFrame:
    """Executa SQL (./sql) substituindo :DATAI e :DATAF; retorna DataFrame."""
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
    """Executa SQL estático; retorna DataFrame."""
    with session_scope() as s:
        txt = open(os.path.join("sql", file), encoding="utf-8").read()
        r = s.execute(text(txt))
        rows, cols = r.fetchall(), [c.upper() for c in r.keys()]
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


normalize = lambda df: df.rename(columns=str.upper)


# ------------------------------ FORMATOS ------------------------------ #
def moeda(v: float) -> str:
    """Formata número como moeda BR (R$)."""
    try:
        return f"R$ {v:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    except Exception:
        return "R$ 0,00"


# Mantém os rótulos do script original
label_filial = lambda c: {"1": "F1 PB", "2": "F1 RN", "3": "BR"}.get(
    str(c).strip(), "Outra"
)


# ------------------------- TABELAS (ESTRUTURA SENTINELA) ------------------------- #
def _tabela_indicador(df: pd.DataFrame, tipo: str, titulo_bloco: str) -> str:
    """
    Constrói uma <table> para um indicador (CORTE/FALTA):
    - Cabeçalho e dados centralizados
    - Hover leve (quando suportado)
    - “ACIMA” em vermelho (apenas a fonte)
    """
    tipo = tipo.upper()
    col_v = f"PVENDA_{tipo}"  # valor monetário
    col_p = f"PCT_PERIODO_{tipo}"  # percentual do período
    col_d = f"DESVIO_{tipo}"  # ex: “+X,XX% ACIMA” | “-X,XX% ABAIXO”

    if tipo == "CORTE":
        valor_hdr = "Valor Cortado (R$)"
        pct_hdr = "Corte no período (%)"
        desv_hdr = "Desvio vs. Meta"
    else:
        valor_hdr = "Valor em Falta (R$)"
        pct_hdr = "Falta no período (%)"
        desv_hdr = "Desvio vs. Trimestre"

    if df.empty:
        return (
            f"<h3>{titulo_bloco}</h3>"
            "<div class='tblWrap'><table>"
            f"<tr><th>Filial</th><th>{valor_hdr}</th><th>{pct_hdr}</th><th>{desv_hdr}</th></tr>"
            "<tr><td colspan='4'><strong>Sem dados.</strong></td></tr>"
            "</table></div>"
        )

    html = [f"<h3>{titulo_bloco}</h3><div class='tblWrap'><table>"]
    html.append(
        f"<tr><th>Filial</th><th>{valor_hdr}</th><th>{pct_hdr}</th><th>{desv_hdr}</th></tr>"
    )

    for _, r in df.iterrows():
        cod = r["CODFILIAL"]
        filial = "TOTAL" if str(cod) == "TOTAL" else label_filial(cod)
        val = moeda(r[col_v]) if col_v in r else "R$ 0,00"
        pct = r[col_p] if col_p in r and r[col_p] is not None else "0,00%"
        des = r[col_d] if col_d in r and r[col_d] is not None else "0%"

        # “ACIMA” => ruim → cor da fonte vermelha
        cls = "ruim" if "ACIMA" in str(des).upper() else ""
        tr_open = f"<tr class='{cls}'>" if cls else "<tr>"
        html.append(
            tr_open
            + f"<td>{filial}</td>"
            + f"<td>{val}</td>"
            + f"<td>{pct}</td>"
            + f"<td>{des}</td>"
            + "</tr>"
        )

    html.append("</table></div>")
    return "".join(html)


def tabelas_benchmark(df_ontem: pd.DataFrame, df_mes: pd.DataFrame, tipo: str) -> str:
    """Produz blocos (Ontem / Mês Atual)."""
    bloco_ontem = _tabela_indicador(df_ontem, tipo, "Ontem")
    bloco_mes = _tabela_indicador(df_mes, tipo, "Mês Atual")
    # Legenda específica
    if tipo.upper() == "CORTE":
        legenda = "<p class='legenda'><em>Meta de Corte: 0,03%</em></p>"
    else:
        if not df_mes.empty and "MEDIA_TRIM_FALTA" in df_mes.columns:
            pares = []
            for _, rr in df_mes.iterrows():
                if str(rr["CODFILIAL"]) != "TOTAL":
                    pares.append(
                        f"{label_filial(rr['CODFILIAL'])}: {rr['MEDIA_TRIM_FALTA']}"
                    )
            legenda = (
                f"<p class='legenda'><em>Média Trimestral por Filial (Falta): {', '.join(pares)}</em></p>"
                if pares
                else ""
            )
        else:
            legenda = ""
    return bloco_ontem + legenda + bloco_mes


# ------------------------------ RANKING ------------------------------ #
def rank_por_filial(df: pd.DataFrame, periodo: str) -> str:
    """Top-5 por filial (centralizado; valores formatados)."""
    if df.empty:
        return "<p class='mensagem-positiva'>Sem ranking.</p>"

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

        html.append(f"<h3>{label_filial(cod)}</h3><div class='tblWrap'><table>")
        html.append(
            "<tr><th>Código</th><th>Descrição</th><th>Qt Und</th><th>Qt Ped</th><th>Valor</th></tr>"
        )
        for r in top.itertuples(index=False):
            html.append(
                "<tr>"
                f"<td>{r.CODPROD}</td>"
                f"<td>{r.DESCRICAO}</td>"
                f"<td>{int(r.QT_UND)}</td>"
                f"<td>{int(r.QT_PED)}</td>"
                f"<td>{moeda(r.VAL)}</td>"
                "</tr>"
            )
        html.append("</table></div>")
    return "".join(html) or "<p class='mensagem-positiva'>Sem ranking.</p>"


# ------------------------------ CORPO DO E-MAIL ------------------------------ #
def corpo_email(
    bmk_ontem: pd.DataFrame,
    bmk_mes: pd.DataFrame,
    s_ontem: pd.DataFrame,
    s_mes: pd.DataFrame,
) -> str:
    """Monta o HTML final do e-mail."""
    tpl = carregar_template()
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    mes_nome = datetime.now().strftime("%B").capitalize()

    return tpl.substitute(
        datahora_atual=agora,
        mes_atual_nome=mes_nome,
        section_corte=tabelas_benchmark(bmk_ontem, bmk_mes, "CORTE"),
        section_falta=tabelas_benchmark(bmk_ontem, bmk_mes, "FALTA"),
        rank_ontem_por_filial=rank_por_filial(s_ontem, "Ontem"),
        rank_mes_por_filial=rank_por_filial(s_mes, f"Mês de {mes_nome}"),
    )


# ------------------------------ EXCEL ------------------------------ #
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


# ------------------------------ ENVIO ------------------------------ #
def enviar_email(html: str, xlsx: io.BytesIO) -> bool:
    """Envia e-mail via SMTP Office 365 com o HTML e anexo XLSX."""
    msg = MIMEMultipart()
    msg["From"] = FROM_ADDR
    msg["To"] = ",".join(DESTS)
    # Assunto conforme solicitado
    msg["Subject"] = f"Sentinela · Corte e Falta - {datetime.now():%d/%m/%Y}"
    msg["X-Priority"] = "1"
    msg.attach(MIMEText(html, "html", "utf-8"))

    part = MIMEApplication(xlsx.read(), _subtype="xlsx")
    part.add_header(
        "Content-Disposition",
        "attachment",
        filename=f"Sentinela_Corte_e_Falta_{datetime.now():%Y%m%d}.xlsx",
    )
    encoders.encode_base64(part)
    msg.attach(part)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.starttls()
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.sendmail(FROM_ADDR, DESTS, msg.as_string())
        return True
    except Exception as exc:
        logging.error("Falha no envio SMTP: %s", exc)
        return False


# ------------------------------ REGRAS DE ENVIO ------------------------------ #
def _tem_movimento(df: pd.DataFrame, qty_col: str, cnt_col: str, val_col: str) -> bool:
    """Retorna True se houver qualquer movimento (>0) em quantidade, pedidos ou valor."""
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


# ------------------------------ ROTINA PRINCIPAL ------------------------------ #
def verificar():
    logging.info("=== Início verificação ===")

    ontem = datetime.now() - timedelta(days=1)
    mes_ini = datetime.now().replace(day=1)

    # Benchmarks (SQL: CORTE com meta fixa 0,03%; FALTA com média trimestral)
    bmk_ontem = normalize(
        executar_sql_param("relatorio_corte_falta_benchmark.sql", ontem, ontem)
    )
    bmk_mes = normalize(
        executar_sql_param(
            "relatorio_corte_falta_benchmark.sql", mes_ini, datetime.now()
        )
    )

    # Sintético & Analítico
    s_ontem = normalize(executar_sql("sintetico_corte_falta.sql"))
    s_mes = normalize(executar_sql("sintetico_corte_falta_mes.sql"))
    a_corte = normalize(executar_sql("analitico_corte_mes.sql"))
    a_falta = normalize(executar_sql("analitico_falta_mes.sql"))

    # Resumo no log (ontem e mês)
    tot = lambda df, c: (
        int(pd.to_numeric(df[c], errors="coerce").fillna(0).sum())
        if c in df.columns
        else 0
    )
    val = lambda df, c: (
        moeda(pd.to_numeric(df[c], errors="coerce").fillna(0).sum())
        if c in df.columns
        else "R$ 0,00"
    )

    logging.info(
        "Ontem – Corte: %d und / %d ped / %s | Falta: %d und / %d ped / %s",
        tot(s_ontem, "QT_CORTE"),
        tot(s_ontem, "COUNT_PED_CORTE"),
        val(s_ontem, "PVENDA_CORTE"),
        tot(s_ontem, "QT_FALTA"),
        tot(s_ontem, "COUNT_PED_FALTA"),
        val(s_ontem, "PVENDA_FALTA"),
    )
    logging.info(
        "Mês – Corte: %d und / %d ped / %s | Falta: %d und / %d ped / %s",
        tot(s_mes, "QT_CORTE"),
        tot(s_mes, "COUNT_PED_CORTE"),
        val(s_mes, "PVENDA_CORTE"),
        tot(s_mes, "QT_FALTA"),
        tot(s_mes, "COUNT_PED_FALTA"),
        val(s_mes, "PVENDA_FALTA"),
    )

    # >>> NOVA REGRA: enviar somente se HOUVER movimento ontem em CORTE E em FALTA.
    corte_ontem_ok = _tem_movimento(
        s_ontem, "QT_CORTE", "COUNT_PED_CORTE", "PVENDA_CORTE"
    )
    falta_ontem_ok = _tem_movimento(
        s_ontem, "QT_FALTA", "COUNT_PED_FALTA", "PVENDA_FALTA"
    )

    if not (corte_ontem_ok and falta_ontem_ok):
        motivo = []
        if not corte_ontem_ok:
            motivo.append("sem CORTE")
        if not falta_ontem_ok:
            motivo.append("sem FALTA")
        logging.info(
            "Critério de envio não atendido (ontem %s) → e-mail não enviado.",
            " e ".join(motivo) or "sem dados",
        )
        logging.info("=== Fim verificação ===")
        return

    if s_ontem.empty and s_mes.empty:
        logging.info("Nenhum dado → e-mail não enviado.")
        logging.info("=== Fim verificação ===")
        return

    corpo = corpo_email(bmk_ontem, bmk_mes, s_ontem, s_mes)
    xlsx = gerar_xlsx(s_ontem, s_mes, a_corte, a_falta)
    ok = enviar_email(corpo, xlsx)

    logging.info("Enviado para: %s", ", ".join(DESTS))
    logging.info("E-mail enviado: %s", "OK" if ok else "ERRO")
    logging.info("=== Fim verificação ===")


# ------------------------------ SCHEDULER / CLI ------------------------------ #
def _proximo() -> datetime:
    """Calcula próxima execução com base na AGENDA (dias úteis 08:00)."""
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


def main():
    ap = argparse.ArgumentParser(description="Sentinela – Corte e Falta")
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
