#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import io
import locale
import logging
import smtplib
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Union, Callable
from datetime import datetime, timedelta, time as dt_time

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders

from dotenv import load_dotenv
import pandas as pd

load_dotenv()


# ------------------------------------------------------------
# Locale / Logging / Pastas padrões
# ------------------------------------------------------------
def setup_locale_pt() -> None:
    try:
        locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
    except locale.Error:
        locale.setlocale(locale.LC_TIME, "")


def ensure_dir(p: Union[str, Path]) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def setup_logging(log_filename: str, level: int = logging.INFO) -> None:
    log_dir = ensure_dir(Path(__file__).resolve().parent / "log")
    log_file = log_dir / log_filename
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    logging.basicConfig(
        handlers=[
            logging.FileHandler(log_file, "a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
    )


# ------------------------------------------------------------
# SQL helpers
# ------------------------------------------------------------
def sql_path(filename: Union[str, Path]) -> Path:
    fn = Path(filename)
    base = Path(__file__).resolve().parent / "sql"
    return fn if fn.is_absolute() else base / fn


def load_sql(filename: Union[str, Path]) -> str:
    with open(sql_path(filename), "r", encoding="utf-8") as f:
        return f.read()


# ------------------------------------------------------------
# DataFrame helpers
# ------------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def moeda_br(v) -> str:
    try:
        return (
            f"R$ {float(v):,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        )
    except Exception:
        return "R$ 0,00"


def to_xlsx_bytes(df_or_sheets: Union[pd.DataFrame, dict]) -> bytes:
    """
    Converte DataFrame único ou dict{sheet_name: DataFrame} em .xlsx (bytes).
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        if isinstance(df_or_sheets, dict):
            for name, d in df_or_sheets.items():
                (d if isinstance(d, pd.DataFrame) else pd.DataFrame(d)).to_excel(
                    w, sheet_name=str(name)[:31], index=False
                )
        else:
            (
                df_or_sheets
                if isinstance(df_or_sheets, pd.DataFrame)
                else pd.DataFrame(df_or_sheets)
            ).to_excel(w, sheet_name="Planilha1", index=False)
    buf.seek(0)
    return buf.getvalue()


def filial_label(cod, mapping: Optional[dict] = None) -> str:
    base = mapping or {"1": "F1 PB", "2": "F1 RN", "3": "BR"}
    return base.get(str(cod).strip(), str(cod))


# ------------------------------------------------------------
# Assunto / Rodapé
# ------------------------------------------------------------
def build_subject(
    titulo: str,
    contexto: Optional[Union[str, Sequence[str]]] = None,
    data_hora: Optional[datetime] = None,
    incluir_hora: bool = False,
) -> str:
    data_hora = data_hora or datetime.now()
    data_fmt = "%d/%m/%Y %H:%M" if incluir_hora else "%d/%m/%Y"
    data_txt = data_hora.strftime(data_fmt)

    bloco_ctx = ""
    if contexto:
        if isinstance(contexto, str):
            parts = [contexto.strip()] if contexto.strip() else []
        else:
            parts = [str(x).strip() for x in contexto if str(x).strip()]
        if parts:
            bloco_ctx = " - " + " - ".join(parts)

    return f"Sentinela · {titulo}{bloco_ctx} - {data_txt}"


def rodape_padrao() -> str:
    return (
        "<p>Este é um e-mail automático. Não responda.</p>"
        "<p><strong>Equipe de TI · Grupo BRF1</strong></p>"
    )


# ------------------------------------------------------------
# E-mail (Office 365)
# ------------------------------------------------------------
def _smtp_cfg() -> Tuple[str, str, str, int]:
    user = os.getenv("EMAIL_USER", "")
    pwd = os.getenv("EMAIL_PASSWORD", "")
    host = os.getenv("OFFICE365_SMTP_SERVER", "smtp.office365.com")
    port = int(os.getenv("OFFICE365_SMTP_PORT", "587"))
    return user, pwd, host, port


def _split_emails(v: Optional[str]) -> List[str]:
    if not v:
        return []
    parts = re.split(r"[;,]", v)
    return [p.strip() for p in parts if "@" in p]


def get_env_recipients() -> Tuple[List[str], List[str], List[str]]:
    to = _split_emails(os.getenv("EMAIL_PARA", ""))
    cc = _split_emails(os.getenv("EMAIL_CC", ""))
    cco = _split_emails(os.getenv("EMAIL_CCO", ""))
    if not to:
        to = [d for d in os.getenv("EMAIL_DESTINATARIOS", "").split(";") if "@" in d]
        to = [p.strip() for p in to]
    return to, cc, cco


def send_email_html(
    to: Sequence[str],
    subject: str,
    html: str,
    attachments: Optional[List[Tuple[str, bytes]]] = None,
    cc: Optional[Sequence[str]] = None,
    cco: Optional[Sequence[str]] = None,
    high_priority: bool = True,
) -> bool:
    to = [m for m in (to or []) if m]
    cc = [m for m in (cc or []) if m]
    cco = [m for m in (cco or []) if m]
    if not to:
        logging.error("Lista de destinatários vazia - envio cancelado.")
        return False

    user, pwd, host, port = _smtp_cfg()
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    if high_priority:
        msg["X-Priority"] = "1"
    msg.attach(MIMEText(html, "html", "utf-8"))

    for name, content in attachments or []:
        subtype = (
            "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            if name.lower().endswith(".xlsx")
            else "octet-stream"
        )
        part = MIMEApplication(content, _subtype=subtype)
        part.add_header("Content-Disposition", "attachment", filename=name)
        encoders.encode_base64(part)
        msg.attach(part)

    try:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.starttls()
            smtp.login(user, pwd)
            smtp.sendmail(user, to + cc + cco, msg.as_string())
        logging.info(
            "E-mail enviado → To:%s | Cc:%s | Cco:%s",
            "; ".join(to) or "-",
            "; ".join(cc) or "-",
            "; ".join(cco) or "-",
        )
        return True
    except Exception as exc:
        logging.error("Falha no envio SMTP: %s", exc)
        return False


# ------------------------------------------------------------
# Render de e-mail (template universal)
# - Usa comments como ganchos para não quebrar o linter do VS Code
# ------------------------------------------------------------
def render_email_base(
    title: str,
    content_html: str,
    subtitle_html: str = "",
    footer_html: Optional[str] = None,
    extra_css: str = "",
    base_template_path: Optional[Union[str, Path]] = None,
) -> str:
    """
    Se existir email_base.html, usa; senão, usa um base embutido.
    Ganchos:
      {{TITLE}}, {{SUBTITLE}}, {{CONTENT}}, {{FOOTER}}
      <!-- EXTRA_CSS -->
    """

    def _apply_hooks(txt: str) -> str:
        txt = txt.replace("{{TITLE}}", title)
        txt = txt.replace("{{SUBTITLE}}", subtitle_html or "")
        txt = txt.replace("{{CONTENT}}", content_html or "")
        txt = txt.replace("{{FOOTER}}", footer_html or rodape_padrao())
        if "<!-- EXTRA_CSS -->" in txt and extra_css:
            txt = txt.replace("<!-- EXTRA_CSS -->", f"<style>\n{extra_css}\n</style>")
        else:
            txt = txt.replace("<!-- EXTRA_CSS -->", "")
        return txt

    # preferir arquivo externo se existir
    if base_template_path:
        p = Path(base_template_path)
        if p.exists():
            return _apply_hooks(p.read_text(encoding="utf-8"))

    # base simples embutido
    base = """<!DOCTYPE html>
<html lang="pt-BR">
  <head>
    <meta charset="utf-8" />
    <title>{{TITLE}}</title>
    <style>
      body{margin:0;padding:20px;font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111827;background:#fff;line-height:1.45}
      h2{margin:0 0 6px;text-align:center}
      .container{max-width:1200px;margin:0 auto}
      .tblWrap{overflow-x:auto;margin:0 auto 20px;max-width:100%}
      table{border-collapse:separate;border:1px solid #d1d5db;border-radius:8px;overflow:hidden;width:100%;min-width:720px}
      th,td{border:1px solid #e5e7eb;padding:8px;text-align:center;white-space:nowrap;font-size:13px}
      th{background:#282248;color:#fff;text-transform:uppercase}
      tr:nth-child(even){background:#f9fafb}
      tr:hover{background:#eef2ff}
    </style>
    <!-- EXTRA_CSS -->
  </head>
  <body>
    <div class="container">
      <h2>{{TITLE}}</h2>
      {{SUBTITLE}}
      {{CONTENT}}
      <div style="text-align:center;font-size:12.5px;color:#6b7280;margin-top:14px">
        {{FOOTER}}
      </div>
    </div>
  </body>
</html>
"""
    return _apply_hooks(base)


# ------------------------------------------------------------
# Agenda / Loop / CLI helper
# ------------------------------------------------------------
def proximo_horario(agenda: list) -> datetime:
    agora = datetime.now()
    prox = None
    for cfg in agenda:
        for wd in cfg.get("dias", []):
            delta = (wd - agora.weekday()) % 7
            alvo = datetime.combine(
                agora.date() + timedelta(days=delta), cfg["horario"]
            )
            if alvo <= agora:
                alvo += timedelta(days=7)
            prox = alvo if prox is None or alvo < prox else prox
    return prox


def loop_agendado(agenda: list, callback: Callable[[], None]) -> None:
    import time as _t

    while True:
        nxt = proximo_horario(agenda)
        logging.info("Próxima execução: %s", nxt.strftime("%d/%m/%Y %H:%M:%S"))
        _t.sleep(max(0, (nxt - datetime.now()).total_seconds()))
        callback()
