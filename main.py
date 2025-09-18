#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==============================================================================
# Sentinela · Corte e Falta
# ------------------------------------------------------------------------------
# Objetivo:
#   - Executar consultas SQL (benchmark diário e mensal, sintéticos de ontem e do mês,
#     analíticos de corte/falta).
#   - Montar e-mail com:
#       • Sumário de KPIs (Corte/Falta de Ontem e do Mês)
#       • Indicadores de Corte (Ontem + Mês) — meta fixa 0,03%
#       • Indicadores de Falta (Ontem + Mês) — com legenda da média trimestral por filial
#       • Top 5 por Filial — Ontem
#       • Top 5 por Filial — Mês corrente
#   - Anexar XLSX detalhado com 4 abas.
#   - Rodar manualmente via CLI (--modo manual) ou no loop diário (--modo diario).
#
# Pré-requisitos:
#   - Arquivos SQL em ./sql
#       relatorio_corte_falta_benchmark.sql
#       sintetico_corte_falta.sql
#       sintetico_corte_falta_mes.sql
#       analitico_corte_mes.sql
#       analitico_falta_mes.sql
#   - Template HTML email_base.html no diretório raiz (ao lado deste arquivo).
#   - .env com as variáveis de e-mail e banco (ver sentinela_core.py e config_bd.py).
# ==============================================================================

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.sql import text

# Conexão e sessão com o banco (Oracle via SQLAlchemy)
from config_bd import session_scope

# Núcleo compartilhado (logging, e-mail, template, XLSX, helpers)
from sentinela_core import (
    setup_logging,
    load_sql,
    read_template,
    render_email,
    moeda_br,
    label_filial,
    build_subject,
    compute_next_run,
    read_env_emails,
    smtp_client,
    to_xlsx_bytes_multiplas_abas,
)

# ------------------------------------------------------------------------------
# Setup básico
# ------------------------------------------------------------------------------
load_dotenv()
setup_logging("Sentinela-Corte-Falta.log")

BASE_DIR = Path(__file__).resolve().parent
TITLE_BASE = "Corte e Falta"  # fica "Sentinela · Corte e Falta" no assunto
AGENDA = [{"dias": [0, 1, 2, 3, 4], "horario": dt_time(8, 0)}]  # seg-sex, 08:00


# ------------------------------------------------------------------------------
# Utilitários locais
# ------------------------------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas para UPPERCASE sem espaços.”"""
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def executar_sql(arquivo_sql: str) -> pd.DataFrame:
    """Executa SQL sem parâmetros (carrega de ./sql/<arquivo_sql>)."""
    sql = load_sql(arquivo_sql)
    with session_scope() as s:
        r = s.execute(text(sql))
        rows = r.fetchall()
        cols = [c.upper() for c in r.keys()]
    return (
        normalize_columns(pd.DataFrame(rows, columns=cols))
        if rows
        else pd.DataFrame(columns=cols)
    )


def executar_sql_param(arquivo_sql: str, di: datetime, df: datetime) -> pd.DataFrame:
    """
    Executa SQL com parâmetros de data (:DATAI e :DATAF).
    Substitui por TO_DATE('yyyy-mm-dd','YYYY-MM-DD') no SQL.
    """
    sql = (
        load_sql(arquivo_sql)
        .replace(":DATAI", f"TO_DATE('{di:%Y-%m-%d}','YYYY-MM-DD')")
        .replace(":DATAF", f"TO_DATE('{df:%Y-%m-%d}','YYYY-MM-DD')")
    )
    with session_scope() as s:
        r = s.execute(text(sql))
        rows = r.fetchall()
        cols = [c.upper() for c in r.keys()]
    return (
        normalize_columns(pd.DataFrame(rows, columns=cols))
        if rows
        else pd.DataFrame(columns=cols)
    )


# ------------------------------------------------------------------------------
# Blocos HTML (componentes do corpo do e-mail)
# ------------------------------------------------------------------------------
def _tabela_indicador(df: pd.DataFrame, tipo: str, titulo_bloco: str) -> str:
    """
    Monta tabela de indicadores para 'CORTE' ou 'FALTA' em 'Ontem' ou 'Mês Atual'.
    Requer no df:
      CODFILIAL, PVENDA_<TIPO>, PCT_PERIODO_<TIPO>, DESVIO_<TIPO>
    """
    tipo = tipo.upper()
    col_v = f"PVENDA_{tipo}"
    col_p = f"PCT_PERIODO_{tipo}"
    col_d = f"DESVIO_{tipo}"

    if tipo == "CORTE":
        valor_hdr, pct_hdr, desv_hdr = (
            "Valor Cortado (R$)",
            "Corte no período (%)",
            "Desvio vs. Meta",
        )
    else:
        valor_hdr, pct_hdr, desv_hdr = (
            "Valor em Falta (R$)",
            "Falta no período (%)",
            "Desvio vs. Trimestre",
        )

    if df.empty:
        return (
            f"<h3 class='subtitle'>{titulo_bloco}</h3>"
            "<div class='tblWrap'><table class='data'>"
            f"<tr><th>Filial</th><th>{valor_hdr}</th><th>{pct_hdr}</th><th>{desv_hdr}</th></tr>"
            "<tr><td colspan='4'><strong>Sem dados.</strong></td></tr>"
            "</table></div>"
        )

    linhas: List[str] = []
    for _, r in df.iterrows():
        cod = r["CODFILIAL"]
        filial = "TOTAL" if str(cod) == "TOTAL" else label_filial(cod)
        val = moeda_br(r.get(col_v, 0))
        pct = str(r.get(col_p, "0,00%") or "0,00%")
        des = str(r.get(col_d, "0%") or "0%")
        cls = " class='bad'" if "ACIMA" in des.upper() else ""
        linhas.append(
            f"<tr{cls}><td>{filial}</td><td>{val}</td><td>{pct}</td><td>{des}</td></tr>"
        )

    return (
        f"<h3 class='subtitle'>{titulo_bloco}</h3>"
        f"<div class='tblWrap'><table class='data'>"
        f"<tr><th>Filial</th><th>{valor_hdr}</th><th>{pct_hdr}</th><th>{desv_hdr}</th></tr>"
        f"{''.join(linhas)}</table></div>"
    )


def _legenda_media_trim_falta(df_mes: pd.DataFrame) -> str:
    """
    Gera legenda da média trimestral de falta por filial, se a coluna
    MEDIA_TRIM_FALTA existir no df do mês.
    """
    if df_mes.empty or "MEDIA_TRIM_FALTA" not in df_mes.columns:
        return ""
    pares = []
    for _, rr in df_mes.iterrows():
        if str(rr.get("CODFILIAL")) != "TOTAL":
            pares.append(f"{label_filial(rr['CODFILIAL'])}: {rr['MEDIA_TRIM_FALTA']}")
    return (
        f"<p class='legend'><em>Média Trimestral por Filial (Falta): {', '.join(pares)}</em></p>"
        if pares
        else ""
    )


def tabelas_benchmark(bmk_ontem: pd.DataFrame, bmk_mes: pd.DataFrame, tipo: str) -> str:
    """Junta as duas tabelas (Ontem + Mês Atual) para CORTE ou FALTA."""
    bloco_ontem = _tabela_indicador(bmk_ontem, tipo, "Ontem")
    bloco_mes = _tabela_indicador(bmk_mes, tipo, "Mês Atual")
    if tipo.upper() == "FALTA":
        return bloco_ontem + _legenda_media_trim_falta(bmk_mes) + bloco_mes
    return bloco_ontem + bloco_mes


def rank_por_filial(df: pd.DataFrame) -> str:
    """
    Exibe Top 5 produtos por filial, priorizando CORTE se as colunas existirem,
    senão usa FALTA. Filtra linhas com quantidade e valor > 0.
    """
    if df.empty:
        return "<p class='ok'>Sem ranking.</p>"

    if "QT_CORTE" in df.columns:
        qt_field, cnt_field, val_field = "QT_CORTE", "COUNT_PED_CORTE", "PVENDA_CORTE"
    else:
        qt_field, cnt_field, val_field = "QT_FALTA", "COUNT_PED_FALTA", "PVENDA_FALTA"

    blocos: List[str] = []
    for cod in sorted(df["CODFILIAL"].astype(str).unique()):
        grp = df[
            (df["CODFILIAL"].astype(str) == cod)
            & (df[qt_field] > 0)
            & (df[val_field] > 0)
        ]
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

        linhas = [
            "<tr><th>Código</th><th>Descrição</th><th>Qt Und</th><th>Qt Ped</th><th>Valor</th></tr>"
        ]
        for r in top.itertuples(index=False):
            linhas.append(
                f"<tr><td>{r.CODPROD}</td><td>{r.DESCRICAO}</td>"
                f"<td>{int(r.QT_UND)}</td><td>{int(r.QT_PED)}</td><td>{moeda_br(r.VAL)}</td></tr>"
            )

        blocos.append(
            f"<h3 class='subtitle'>{label_filial(cod)}</h3>"
            f"<div class='tblWrap'><table class='data'>{''.join(linhas)}</table></div>"
        )

    return "".join(blocos) or "<p class='ok'>Sem ranking.</p>"


def _sumario_kpis(df_ontem: pd.DataFrame, df_mes: pd.DataFrame) -> str:
    """
    Sumário compacto de KPIs (valores totais de Corte e Falta) para Ontem e Mês.
    """
    parts: List[str] = []
    if not df_ontem.empty:
        corte_o = df_ontem.get("PVENDA_CORTE", pd.Series(dtype=float)).sum()
        falta_o = df_ontem.get("PVENDA_FALTA", pd.Series(dtype=float)).sum()
        parts.append(
            f"<p><strong>Ontem</strong> → Corte: {moeda_br(corte_o)}, Falta: {moeda_br(falta_o)}</p>"
        )
    if not df_mes.empty:
        corte_m = df_mes.get("PVENDA_CORTE", pd.Series(dtype=float)).sum()
        falta_m = df_mes.get("PVENDA_FALTA", pd.Series(dtype=float)).sum()
        parts.append(
            f"<p><strong>Mês Atual</strong> → Corte: {moeda_br(corte_m)}, Falta: {moeda_br(falta_m)}</p>"
        )
    return (
        "".join(parts) if parts else "<p class='ok'>Sem movimentação para exibir.</p>"
    )


def corpo_email_completo(
    assunto: str,
    bmk_ontem: pd.DataFrame,
    bmk_mes: pd.DataFrame,
    s_ontem: pd.DataFrame,
    s_mes: pd.DataFrame,
) -> str:
    """
    Monta o corpo HTML completo (com tabelas e rankings) usando email_base.html.
    """
    mes_nome = datetime.now().strftime("%B").capitalize()

    partes: List[str] = []
    partes.append("<h3 class='subtitle'>Relatório de Indicadores</h3>")
    partes.append(_sumario_kpis(s_ontem, s_mes))

    partes.append("<h3>Indicadores de Corte (Meta fixa 0,03%)</h3>")
    partes.append(tabelas_benchmark(bmk_ontem, bmk_mes, "CORTE"))

    partes.append("<h3>Indicadores de Falta</h3>")
    partes.append(tabelas_benchmark(bmk_ontem, bmk_mes, "FALTA"))

    partes.append("<h3>Top 5 por Filial – Ontem</h3>")
    partes.append(rank_por_filial(s_ontem))

    partes.append(f"<h3>Top 5 por Filial – Mês {mes_nome}</h3>")
    partes.append(rank_por_filial(s_mes))

    tpl = read_template("email_base.html")
    footer = "Este é um e-mail automático. Não responda."
    return render_email(tpl, assunto, "".join(partes), footer, extra_css=None)


# ------------------------------------------------------------------------------
# Fluxo principal
# ------------------------------------------------------------------------------
def verificar() -> None:
    """
    Orquestra:
      1) Executa SQLs
      2) Aplica critérios de envio
      3) Monta HTML e gera anexo XLSX
      4) Envia e-mail
    """
    logging.info("Início da verificação")

    ontem = datetime.now() - timedelta(days=1)
    mes_ini = datetime.now().replace(day=1)

    # Benchmarks (parametrizados por data)
    bmk_ontem = executar_sql_param("relatorio_corte_falta_benchmark.sql", ontem, ontem)
    bmk_mes = executar_sql_param(
        "relatorio_corte_falta_benchmark.sql", mes_ini, datetime.now()
    )

    # Sintéticos (ontem e mês) e analíticos (mês)
    s_ontem = executar_sql("sintetico_corte_falta.sql")
    s_mes = executar_sql("sintetico_corte_falta_mes.sql")
    a_corte = executar_sql("analitico_corte_mes.sql")
    a_falta = executar_sql("analitico_falta_mes.sql")

    # Critério de envio:
    #   Envia se houver CORTE OU FALTA no dia anterior (evita e-mail vazio)
    corte_ok = (
        (not s_ontem.empty)
        and ("PVENDA_CORTE" in s_ontem.columns)
        and (s_ontem["PVENDA_CORTE"].sum() > 0)
    )
    falta_ok = (
        (not s_ontem.empty)
        and ("PVENDA_FALTA" in s_ontem.columns)
        and (s_ontem["PVENDA_FALTA"].sum() > 0)
    )
    if not (corte_ok or falta_ok):
        logging.info(
            "Critério de envio não atendido (sem corte e sem falta ontem). E-mail não enviado."
        )
        logging.info("Fim da verificação")
        return

    # Assunto padronizado (usa helper do core)
    assunto = build_subject(TITLE_BASE)

    # Corpo do e-mail (completo, seguindo estrutura padronizada)
    html = corpo_email_completo(assunto, bmk_ontem, bmk_mes, s_ontem, s_mes)

    # Anexo XLSX — nomes “humanos”; sanitização automática no core
    dfs = {
        f"Sintético ({ontem:%d/%m/%Y})": s_ontem,
        f"Sintético Mês {datetime.now():%B}": s_mes,
        "Analítico Corte Mês": a_corte,
        "Analítico Falta Mês": a_falta,
    }
    anexo = to_xlsx_bytes_multiplas_abas(dfs)

    # Envio
    dest = read_env_emails()
    smtp = smtp_client()
    smtp.send_html(
        subject=assunto,
        html=html,
        to=dest["to"],
        cc=dest["cc"],
        bcc=dest["bcc"],
        attachments=[
            (f"Sentinela_Corte_Falta_{datetime.now():%Y%m%d_%H%M}.xlsx", anexo)
        ],
        priority_high=True,
    )

    logging.info("E-mail enviado com sucesso.")
    logging.info("Fim da verificação")


def _loop() -> None:
    """Loop do modo 'diario' (executa nos dias/horários definidos em AGENDA)."""
    import time as _t

    while True:
        nxt = compute_next_run(AGENDA)
        logging.info("Próxima execução: %s", nxt.strftime("%d/%m/%Y %H:%M:%S"))
        _t.sleep(max(0, (nxt - datetime.now()).total_seconds()))
        verificar()


def main() -> None:
    """CLI: --modo manual | --modo diario"""
    ap = argparse.ArgumentParser(description="Sentinela · Corte e Falta")
    ap.add_argument("--modo", choices=["manual", "diario"], required=True)
    modo = ap.parse_args().modo
    if modo == "manual":
        logging.info("Modo manual")
        verificar()
    else:
        logging.info("Modo diário")
        _loop()


if __name__ == "__main__":
    main()
