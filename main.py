# -*- coding: utf-8 -*-
"""
Sentinela · Corte
=================
Orquestra a execução dos relatórios de Corte:
- Executa 4 SQLs (todos parametrizados com :DATAI e :DATAF):
    • relatorio_corte_benchmark.sql   (indicadores por filial)    -> Ontem + Mês/Fechamento
    • sintetico_corte_ontem.sql       (Top 5 ontem por filial)
    • sintetico_corte_mes.sql         (Top 5 mês por filial)
    • analitico_corte_mes.sql         (Analítico mês)
- Monta e-mail (HTML) a partir do template `email_base.html`
- Envia e-mail via SMTP (Office 365)
- Gera anexo XLSX com múltiplas abas

Dependências do projeto:
- sentinela_core.py  -> logging, SMTP, renderização, XLSX, helpers
- config_bd.session_scope -> contexto de sessão SQLAlchemy para Oracle
- ./sql/*.sql        -> arquivos SQL parametrizados com :DATAI e :DATAF
- ./email_base.html  -> template do email com placeholders {{TITLE}}, {{CONTENT}}, {{FOOTER}}

Como usar:
    python main.py --modo manual
    python main.py --modo diario
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy.sql import text

from config_bd import session_scope
from sentinela_core import (
    setup_logging,
    load_sql,
    read_template,
    render_email,
    moeda_br,
    label_filial,
    smtp_client,
    read_env_emails,
    to_xlsx_bytes_multiplas_abas,
    build_subject_corte,
    build_attachment_name,
)

# ------------------------------------------------------------------------------
# Constantes e caminhos
# ------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = "Sentinela-Corte.log"
TITLE_BASE = "Corte"

# Arquivos SQL (pasta ./sql)
SQL_BMK = "relatorio_corte_benchmark.sql"
SQL_SINT_ONTEM = "sintetico_corte_ontem.sql"
SQL_SINT_MES = "sintetico_corte_mes.sql"
SQL_ANL_MES = "analitico_corte_mes.sql"


# ------------------------------------------------------------------------------
# Utilitários de datas e período
# ------------------------------------------------------------------------------
def _nome_mes_pt(dt: date) -> str:
    """Retorna 'Mês/AAAA' em PT-BR (ex.: 'Setembro/2025')."""
    meses = [
        "Janeiro",
        "Fevereiro",
        "Março",
        "Abril",
        "Maio",
        "Junho",
        "Julho",
        "Agosto",
        "Setembro",
        "Outubro",
        "Novembro",
        "Dezembro",
    ]
    return f"{meses[dt.month - 1]}/{dt.year}"


def _periodo_mes_para(dt_hoje: datetime) -> Tuple[datetime, datetime, bool, str]:
    """
    Determina o período do bloco 'Mês':
    - Se for dia 1 -> 'Fechamento' do mês anterior (1º..último dia do mês anterior)
    - Senão        -> Mês atual (1º..hoje)
    Retorna: (DATAI, DATAF, is_fechamento, label_bloco)
    """
    if dt_hoje.day == 1:
        # Fechamento (mês anterior completo)
        primeiro_destemes = dt_hoje.replace(day=1)
        ultimo_anterior = primeiro_destemes - timedelta(days=1)
        datai = ultimo_anterior.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        dataf = ultimo_anterior.replace(hour=23, minute=59, second=59, microsecond=0)
        label = f"Fechamento - {_nome_mes_pt(ultimo_anterior.date())}"
        return datai, dataf, True, label

    # Mês corrente até hoje
    datai = dt_hoje.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    dataf = dt_hoje.replace(hour=23, minute=59, second=59, microsecond=0)
    label = f"Mês Atual - {_nome_mes_pt(dt_hoje.date())}"
    return datai, dataf, False, label


# ------------------------------------------------------------------------------
# Execução SQL
# ------------------------------------------------------------------------------
def _normalize_upper(df: pd.DataFrame) -> pd.DataFrame:
    """Garante colunas em UPPERCASE para facilitar o consumo a jusante."""
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _executar_sql_binds(nome_arquivo_sql: str, params: Dict) -> pd.DataFrame:
    """
    Lê o SQL (com binds :DATAI e :DATAF) e executa via SQLAlchemy, retornando
    DataFrame com colunas normalizadas para UPPERCASE.
    """
    sql = load_sql(nome_arquivo_sql)
    with session_scope() as s:
        res = s.execute(text(sql), params)
        rows = res.fetchall()
        cols = [c for c in res.keys()]
    return _normalize_upper(pd.DataFrame(rows, columns=cols))


# ------------------------------------------------------------------------------
# Construção de blocos HTML
# ------------------------------------------------------------------------------
def _fmt_pct_str(pct_str: str) -> str:
    """
    Apenas garante retorno string; o SQL já vem formatado como '0,12%'.
    """
    return "0,00%" if not pct_str else str(pct_str)


def _build_tabela_indicadores(df: pd.DataFrame, titulo_bloco: str) -> str:
    """
    Monta a tabela de indicadores (Corte por Filial), já com:
    - 2 casas decimais em valores monetários (moeda_br)
    - Linha TOTAL destacada (.total-row no CSS)
    - Linhas com 'ACIMA' (desvio > meta) em vermelho (.bad)
    - Tabela centralizada (via .tblWrap + inline-table no template)

    Colunas esperadas no DF (UPPER):
      CODFILIAL | PVENDA_CORTE | PCT_PERIODO_CORTE | DESVIO_CORTE | FATURAMENTO
    """
    headers = (
        "<tr>"
        "<th>Filial</th>"
        "<th>Valor Cortado (R$)</th>"
        "<th>Corte no período (%)</th>"
        "<th>Desvio vs. Meta</th>"
        "<th>Faturado (R$)</th>"
        "</tr>"
    )

    linhas: List[str] = []
    for _, r in df.iterrows():
        cod = str(r.get("CODFILIAL"))
        filial = "TOTAL" if cod == "TOTAL" else label_filial(cod)

        corte_val = moeda_br(round(float(r.get("PVENDA_CORTE") or 0), 2))
        pct = _fmt_pct_str(r.get("PCT_PERIODO_CORTE", "0,00%"))
        desvio = str(r.get("DESVIO_CORTE") or "0%")
        faturado_val = moeda_br(round(float(r.get("FATURAMENTO") or 0), 2))

        # vermelho somente para ACIMA da meta
        tr_classes = []
        if desvio.upper().find("ACIMA") >= 0:
            tr_classes.append("bad")
        if cod == "TOTAL":
            tr_classes.append("total-row")

        cls_attr = f" class=\"{' '.join(tr_classes)}\"" if tr_classes else ""
        linhas.append(
            f"<tr{cls_attr}>"
            f"<td>{filial}</td>"
            f"<td>{corte_val}</td>"
            f"<td>{pct}</td>"
            f"<td>{desvio}</td>"
            f"<td>{faturado_val}</td>"
            f"</tr>"
        )

    return (
        f"<h3 class='subtitle subtitle-small sectionHeader'>{titulo_bloco}</h3>"
        "<div class='tblWrap'>"
        "<table class='data'>"
        f"{headers}{''.join(linhas)}"
        "</table>"
        "</div>"
    )


def _rank_por_filial(df: pd.DataFrame, titulo: str) -> str:
    """
    Constrói as tabelas Top 5 por filial (base PVENDA_CORTE).
    Espera colunas (UPPER): CODFILIAL, CODPROD, DESCRICAO, QT_CORTE, COUNT_PED_CORTE, PVENDA_CORTE
    """
    if df is None or df.empty:
        return f"<h3 class='subtitle subtitle-small sectionHeader'>{titulo}</h3><p class='muted' style='text-align:center'>Sem dados.</p>"

    blocos = [f"<h3 class='subtitle subtitle-small sectionHeader'>{titulo}</h3>"]
    for cod in sorted(df["CODFILIAL"].astype(str).unique()):
        grp = df[df["CODFILIAL"].astype(str) == cod].copy()
        if grp.empty:
            continue

        # agrega por produto dentro da filial
        top = (
            grp.groupby(["CODPROD", "DESCRICAO"], as_index=False)
            .agg(
                QT_UND=("QT_CORTE", "sum"),
                QT_PED=("COUNT_PED_CORTE", "sum"),
                VAL=("PVENDA_CORTE", "sum"),
            )
            .sort_values("VAL", ascending=False)
            .head(5)
        )

        linhas = [
            "<tr><th>Código</th><th>Descrição</th><th>Qt Und</th><th>Qt Ped</th><th>Valor</th></tr>"
        ]
        for r in top.itertuples(index=False):
            linhas.append(
                "<tr>"
                f"<td>{r.CODPROD}</td>"
                f"<td>{r.DESCRICAO}</td>"
                f"<td>{int(round(float(r.QT_UND or 0)))}</td>"
                f"<td>{int(round(float(r.QT_PED or 0)))}</td>"
                f"<td>{moeda_br(round(float(r.VAL or 0), 2))}</td>"
                "</tr>"
            )

        blocos.append(
            f"<h4 class='subtitle subtitle-mini' style='margin-top:6px'>{label_filial(cod)}</h4>"
            "<div class='tblWrap'><table class='data'>"
            + "".join(linhas)
            + "</table></div>"
        )

    return "".join(blocos)


def _montar_html_email(
    bmk_ontem: pd.DataFrame,
    bmk_mes: pd.DataFrame,
    s_ontem: pd.DataFrame,
    s_mes: pd.DataFrame,
    assunto: str,
    titulo_mes_bloco: str,
) -> str:
    """
    Monta o HTML final usando o template `email_base.html`:
    - Título H2 central
    - Subtítulo 'Indicadores de Corte (Meta fixa 0,03%)' central e menor
    - Tabela de indicadores (Ontem e Mês/Fechamento)
    - Top 5 por filial (Ontem e Mês)
    """
    partes: List[str] = []
    partes.append(
        "<h3 class='subtitle subtitle-small sectionHeader' style='margin-top:2px'>Indicadores de Corte (Meta fixa 0,03%)</h3>"
    )
    partes.append(_build_tabela_indicadores(bmk_ontem, "Ontem"))
    partes.append(_build_tabela_indicadores(bmk_mes, titulo_mes_bloco))
    partes.append(_rank_por_filial(s_ontem, "Top 5 por Filial - Ontem"))
    partes.append(
        _rank_por_filial(
            s_mes,
            "Top 5 por Filial - "
            + titulo_mes_bloco.replace("Fechamento - ", "").replace("Mês Atual - ", ""),
        )
    )

    # Renderiza no template
    tpl = read_template("email_base.html")
    footer = "Este e-mail é gerado automaticamente. Não responda."
    html = render_email(
        template=tpl, title=assunto, content="".join(partes), footer=footer
    )
    return html


# ------------------------------------------------------------------------------
# Orquestração principal
# ------------------------------------------------------------------------------
def montar_corpo_e_anexo(
    hoje: datetime,
) -> Tuple[str, bytes, Dict[str, pd.DataFrame], str, str]:
    """
    Executa os SQLs, monta o HTML e gera o XLSX.
    Retorna:
        html, anexo_bytes, abas_dict, nome_anexo, assunto
    """
    logging.info("Início do ciclo Sentinela · Corte")

    # Períodos
    ontem_i = (hoje - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    ontem_f = ontem_i.replace(hour=23, minute=59, second=59, microsecond=0)
    di_mes, df_mes, is_fechamento, label_mes_bloco = _periodo_mes_para(hoje)

    # Benchmarks (por filial)
    bmk_ontem = _executar_sql_binds(SQL_BMK, {"DATAI": ontem_i, "DATAF": ontem_f})
    bmk_mes = _executar_sql_binds(SQL_BMK, {"DATAI": di_mes, "DATAF": df_mes})

    # Sintéticos e Analítico
    s_ontem = _executar_sql_binds(SQL_SINT_ONTEM, {"DATAI": ontem_i, "DATAF": ontem_f})
    s_mes = _executar_sql_binds(SQL_SINT_MES, {"DATAI": di_mes, "DATAF": df_mes})
    a_mes = _executar_sql_binds(SQL_ANL_MES, {"DATAI": di_mes, "DATAF": df_mes})

    # Assunto e anexo
    assunto = build_subject_corte(hoje=hoje, is_fechamento=is_fechamento)
    nome_anexo = build_attachment_name(hoje=hoje, is_fechamento=is_fechamento)

    # HTML final
    html = _montar_html_email(
        bmk_ontem=bmk_ontem,
        bmk_mes=bmk_mes,
        s_ontem=s_ontem,
        s_mes=s_mes,
        assunto=assunto,
        titulo_mes_bloco=label_mes_bloco,
    )

    # XLSX com 3 abas
    abas = {
        f"Sintético (Ontem {ontem_i:%d/%m/%Y})": s_ontem,
        f"Sintético {label_mes_bloco}": s_mes,
        f"Analítico Corte {label_mes_bloco}": a_mes,
    }
    anexo = to_xlsx_bytes_multiplas_abas(abas)

    return html, anexo, abas, nome_anexo, assunto


def _enviar_email(hoje: datetime) -> None:
    """Chama a orquestração e dispara o envio via SMTP (Office 365)."""
    html, anexo, abas, nome_anexo, assunto = montar_corpo_e_anexo(hoje)
    destinatarios = read_env_emails()
    smtp = smtp_client()
    smtp.send_html(
        subject=assunto,
        html=html,
        to=destinatarios["to"],
        cc=destinatarios["cc"],
        bcc=destinatarios["bcc"],
        attachments=[(nome_anexo, anexo)],
        priority_high=True,
    )
    logging.info("E-mail enviado com sucesso.")


def _loop_diario() -> None:
    """
    Loop diário simples:
    - Executa apenas em dias úteis às 08:00.
    - Não usa libs externas de scheduling para manter o script autocontido.
    """
    import time as _time

    def proximo_disparo() -> datetime:
        agora = datetime.now()
        alvo = agora.replace(hour=8, minute=0, second=0, microsecond=0)
        if agora > alvo or agora.weekday() >= 5:
            d = agora
            # próximo dia útil
            while True:
                d = d + timedelta(days=1)
                if d.weekday() < 5:
                    break
            return d.replace(hour=8, minute=0, second=0, microsecond=0)
        return alvo

    while True:
        prox = proximo_disparo()
        logging.info("Próxima execução: %s", prox.strftime("%d/%m/%Y %H:%M:%S"))
        _time.sleep(max(0, int((prox - datetime.now()).total_seconds())))
        try:
            _enviar_email(datetime.now())
        except Exception as e:
            logging.exception("Falha no envio diário: %s", e)


def main() -> None:
    """Ponto de entrada do script."""
    setup_logging(LOG_FILE)

    ap = argparse.ArgumentParser(description="Sentinela · Corte")
    ap.add_argument("--modo", choices=["manual", "diario"], required=True)
    args = ap.parse_args()

    logging.info("Sentinela · Corte iniciado | Modo=%s", args.modo)

    if args.modo == "manual":
        _enviar_email(datetime.now())
    else:
        _loop_diario()


if __name__ == "__main__":
    main()
