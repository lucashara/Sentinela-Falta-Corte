# -*- coding: utf-8 -*-
"""
Sentinela · Corte
=================
- Fechamento: envia SEMPRE no dia 1 (mês anterior completo), mesmo se fim de semana/feriado.
- Diário: envia apenas se houve faturamento no dia anterior.
- Resiliente: checa a cada POLL_SECONDS (env var opcional), envia uma única vez por dia.

Modos:
    python main.py --modo manual   # envia agora
    python main.py --modo diario   # monitora e envia conforme regras

Requer:
- sentinela_core.py (SMTP, render, XLSX etc.)
- config_bd.session_scope (Oracle via SQLAlchemy)
- ./sql/relatorio_corte_benchmark.sql (usa :DATAI/:DATAF; coluna FATURAMENTO)
- ./sql/sintetico_corte_ontem.sql
- ./sql/sintetico_corte_mes.sql
- ./sql/analitico_corte_mes.sql
- ./email_base.html
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta, date, time as dt_time
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

# ---------------------------------------------------------------------------
# Constantes e caminhos
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = "Sentinela-Corte.log"

# SQLs
SQL_BMK = "relatorio_corte_benchmark.sql"
SQL_SINT_ONTEM = "sintetico_corte_ontem.sql"
SQL_SINT_MES = "sintetico_corte_mes.sql"
SQL_ANL_MES = "analitico_corte_mes.sql"

# Estado
STATE_DIR = BASE_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "sentinela_corte_state.json"

# Agendamento
HORA_ALVO = dt_time(8, 0)  # 08:00
POLL_SECONDS = int(os.getenv("CORTE_POLL_SECONDS", "60"))


# ---------------------------------------------------------------------------
# Estado
# ---------------------------------------------------------------------------
def _load_state() -> Dict:
    if STATE_PATH.exists():
        try:
            import json

            with open(STATE_PATH, "r", encoding="utf-8") as f:
                st = json.load(f)
            if not isinstance(st, dict):
                return {}
            return st
        except Exception:
            logging.warning("State corrompido; recriando.")
    return {}


def _save_state(st: Dict) -> None:
    try:
        import json

        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("Falha ao salvar state: %s", e)


# ---------------------------------------------------------------------------
# Datas / Períodos
# ---------------------------------------------------------------------------
def _nome_mes_pt(dt: date) -> str:
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
    Se for DIA 1 (calendário) -> Fechamento do mês ANTERIOR (1º..último).
    Senão                      -> Mês atual (1º..hoje).
    Retorna: (DATAI, DATAF, is_fechamento, label_bloco)
    """
    if dt_hoje.day == 1:
        # mês anterior completo
        primeiro_destemes = dt_hoje.replace(day=1)
        ultimo_anterior = primeiro_destemes - timedelta(days=1)
        datai = datetime(ultimo_anterior.year, ultimo_anterior.month, 1, 0, 0, 0)
        dataf = datetime(
            ultimo_anterior.year, ultimo_anterior.month, ultimo_anterior.day, 23, 59, 59
        )
        label = f"Fechamento - {_nome_mes_pt(ultimo_anterior.date())}"
        return datai, dataf, True, label

    # mês corrente
    datai = dt_hoje.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    dataf = dt_hoje.replace(hour=23, minute=59, second=59, microsecond=0)
    label = f"Mês Atual - {_nome_mes_pt(dt_hoje.date())}"
    return datai, dataf, False, label


# ---------------------------------------------------------------------------
# Execução SQL
# ---------------------------------------------------------------------------
def _normalize_upper(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _executar_sql_binds(nome_arquivo_sql: str, params: Dict) -> pd.DataFrame:
    sql = load_sql(nome_arquivo_sql)
    with session_scope() as s:
        res = s.execute(text(sql), params)
        rows = res.fetchall()
        cols = [c for c in res.keys()]
    return _normalize_upper(pd.DataFrame(rows, columns=cols))


# ---------------------------------------------------------------------------
# Checagem de faturamento
# ---------------------------------------------------------------------------
def _teve_faturamento_ontem(hoje: datetime) -> bool:
    """
    Usa o benchmark por filial (SQL_BMK) do período de 'ontem' e
    verifica se a soma da coluna FATURAMENTO é > 0.
    """
    ontem_i = (hoje - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    ontem_f = ontem_i.replace(hour=23, minute=59, second=59, microsecond=0)
    df = _executar_sql_binds(SQL_BMK, {"DATAI": ontem_i, "DATAF": ontem_f})
    if df.empty:
        return False
    col = "FATURAMENTO"
    if col not in df.columns:
        # Se o SQL não trouxer a coluna esperada, assume "sem faturamento"
        logging.warning("Coluna FATURAMENTO ausente no resultado do benchmark.")
        return False
    try:
        total = pd.to_numeric(df[col], errors="coerce").fillna(0).sum()
    except Exception:
        total = 0.0
    return float(total) > 0.0


# ---------------------------------------------------------------------------
# Construção HTML
# ---------------------------------------------------------------------------
def _fmt_pct_str(pct_str: str) -> str:
    return "0,00%" if not pct_str else str(pct_str)


def _build_tabela_indicadores(df: pd.DataFrame, titulo_bloco: str) -> str:
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
    if df is None or df.empty:
        return f"<h3 class='subtitle subtitle-small sectionHeader'>{titulo}</h3><p class='muted' style='text-align:center'>Sem dados.</p>"

    blocos = [f"<h3 class='subtitle subtitle-small sectionHeader'>{titulo}</h3>"]
    for cod in sorted(df["CODFILIAL"].astype(str).unique()):
        grp = df[df["CODFILIAL"].astype(str) == cod].copy()
        if grp.empty:
            continue

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

    tpl = read_template("email_base.html")
    footer = "Este e-mail é gerado automaticamente. Não responda."
    html = render_email(
        template=tpl, title=assunto, content="".join(partes), footer=footer
    )
    return html


# ---------------------------------------------------------------------------
# Orquestração (executa SQLs, monta HTML/XLSX e envia)
# ---------------------------------------------------------------------------
def montar_corpo_e_anexo(hoje: datetime):
    logging.info("Início do ciclo Sentinela · Corte")

    ontem_i = (hoje - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    ontem_f = ontem_i.replace(hour=23, minute=59, second=59, microsecond=0)
    di_mes, df_mes, is_fechamento, label_mes_bloco = _periodo_mes_para(hoje)

    bmk_ontem = _executar_sql_binds(SQL_BMK, {"DATAI": ontem_i, "DATAF": ontem_f})
    bmk_mes = _executar_sql_binds(SQL_BMK, {"DATAI": di_mes, "DATAF": df_mes})
    s_ontem = _executar_sql_binds(SQL_SINT_ONTEM, {"DATAI": ontem_i, "DATAF": ontem_f})
    s_mes = _executar_sql_binds(SQL_SINT_MES, {"DATAI": di_mes, "DATAF": df_mes})
    a_mes = _executar_sql_binds(SQL_ANL_MES, {"DATAI": di_mes, "DATAF": df_mes})

    assunto = build_subject_corte(hoje=hoje, is_fechamento=is_fechamento)
    nome_anexo = build_attachment_name(hoje=hoje, is_fechamento=is_fechamento)

    html = _montar_html_email(
        bmk_ontem=bmk_ontem,
        bmk_mes=bmk_mes,
        s_ontem=s_ontem,
        s_mes=s_mes,
        assunto=assunto,
        titulo_mes_bloco=label_mes_bloco,
    )

    abas = {
        f"Sintético (Ontem {ontem_i:%d/%m/%Y})": s_ontem,
        f"Sintético {label_mes_bloco}": s_mes,
        f"Analítico Corte {label_mes_bloco}": a_mes,
    }
    anexo = to_xlsx_bytes_multiplas_abas(abas)

    return html, anexo, nome_anexo, assunto, is_fechamento


def _enviar_email(hoje: datetime) -> bool:
    html, anexo, nome_anexo, assunto, is_fechamento = montar_corpo_e_anexo(hoje)
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
    return is_fechamento


# ---------------------------------------------------------------------------
# Loop diário (resiliente, com regras solicitadas)
# ---------------------------------------------------------------------------
def _loop_diario() -> None:
    """
    - Se for DIA 1 (calendário) e horário >= HORA_ALVO: envia FECHAMENTO (sempre).
    - Nos demais dias, horário >= HORA_ALVO: envia DIÁRIO somente se houve FATURAMENTO ontem.
    - Usa state para evitar reenvio no mesmo dia e para marcar último fechamento enviado.
    """
    import time as _time

    st = _load_state()

    while True:
        try:
            agora = datetime.now()
            hoje = agora.date()
            if agora.time() >= HORA_ALVO:
                last_sent = st.get("last_sent_date")  # "YYYY-MM-DD"
                if last_sent != hoje.isoformat():
                    if hoje.day == 1:
                        # FECHAMENTO: SEMPRE
                        ref = hoje.replace(day=1) - timedelta(days=1)
                        last_fech_key = st.get("last_fechamento_key")  # "YYYY-MM"
                        key = f"{ref.year}-{ref.month:02d}"
                        if key != last_fech_key:
                            logging.info("Fechamento detectado (%s). Enviando…", key)
                            _enviar_email(agora)  # is_fechamento=True no dia 1
                            st["last_fechamento_key"] = key
                            st["last_sent_date"] = hoje.isoformat()
                            _save_state(st)
                        else:
                            # Já enviou fechamento deste mês; evita duplicar
                            st["last_sent_date"] = hoje.isoformat()
                            _save_state(st)
                    else:
                        # DIÁRIO: somente se houve faturamento no dia anterior
                        if _teve_faturamento_ontem(agora):
                            logging.info(
                                "Faturamento detectado ontem. Enviando diário…"
                            )
                            _enviar_email(agora)
                            st["last_sent_date"] = hoje.isoformat()
                            _save_state(st)
                        else:
                            logging.info("Sem faturamento ontem. Não enviar diário.")
                            st["last_sent_date"] = (
                                hoje.isoformat()
                            )  # marca o dia como tratado
                            _save_state(st)

            _time.sleep(POLL_SECONDS)

        except Exception as e:
            logging.exception("Falha no loop diário: %s", e)
            _time.sleep(min(POLL_SECONDS, 60))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
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
