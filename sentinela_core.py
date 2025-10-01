# -*- coding: utf-8 -*-
r"""
sentinela_core.py
===============================================================================
Núcleo compartilhado para o "Sentinela · Corte".

Funções principais:
- Logging padronizado
- Leitura de SQLs do diretório ./sql
- Envio de e-mail HTML (SMTP Office 365)
- Renderização do HTML a partir do email_base.html (placeholders)
- Geração de XLSX (múltiplas abas) com nomes de aba sanitizados
- Helpers de formatação (moeda PT-BR), rótulos, assunto e nome do anexo

Notas importantes (boas práticas para e-mail):
- Largura do e-mail entre 600–640px no "cartão" central.
- Tabelas como "inline-table" dentro de um wrapper com overflow-x:auto para
  evitar corte em Outlook/Gmail sem estourar 100%.
- Hover: azul claro; mantém legibilidade no modo claro.
- Linhas "TOTAL" com negrito e bordas superiores/ inferiores.
===============================================================================
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from dotenv import load_dotenv

# SMTP / MIME
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()


# =============================================================================
# Logging
# =============================================================================
def setup_logging(log_file: str) -> None:
    """Configura logging com arquivo em ./log e saída no console."""
    base = Path(__file__).resolve().parent
    log_dir = base / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / log_file
    logging.basicConfig(
        handlers=[
            logging.FileHandler(log_path, "a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
    )


# =============================================================================
# SQL utils
# =============================================================================
def _sql_dir() -> Path:
    """Retorna o caminho da pasta ./sql (ao lado dos scripts)."""
    return Path(__file__).resolve().parent / "sql"


def load_sql(filename: str) -> str:
    """Carrega o arquivo SQL exato da pasta ./sql."""
    sql_dir = _sql_dir()
    sql_dir.mkdir(parents=True, exist_ok=True)
    path = sql_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Arquivo SQL não encontrado: {filename}")
    return path.read_text(encoding="utf-8")


# =============================================================================
# E-mail (SMTP Office 365)
# =============================================================================
class SMTPClient:
    """
    Cliente SMTP (Office 365, STARTTLS).
    Variáveis de ambiente:
      EMAIL_USER, EMAIL_PASSWORD, OFFICE365_SMTP_SERVER, OFFICE365_SMTP_PORT
    """

    def __init__(self) -> None:
        self.user = os.getenv("EMAIL_USER")
        self.password = os.getenv("EMAIL_PASSWORD")
        self.host = os.getenv("OFFICE365_SMTP_SERVER", "smtp.office365.com")
        self.port = int(os.getenv("OFFICE365_SMTP_PORT", "587"))

    def send_html(
        self,
        subject: str,
        html: str,
        to: List[str],
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        attachments: Optional[List[Tuple[str, bytes]]] = None,
        priority_high: bool = False,
    ) -> None:
        """Envia e-mail HTML com anexos opcionais."""
        if not to:
            logging.error("Nenhum destinatário definido (EMAIL_PARA).")
            return

        msg = MIMEMultipart()
        msg["From"] = self.user
        msg["To"] = ", ".join(to)
        if cc:
            msg["Cc"] = ", ".join(cc)
        if subject:
            msg["Subject"] = subject
        if priority_high:
            msg["X-Priority"] = "1"
        msg.attach(MIMEText(html, "html", "utf-8"))

        # Anexos
        if attachments:
            for filename, content in attachments:
                part = MIMEApplication(content, Name=filename)
                part.add_header("Content-Disposition", "attachment", filename=filename)
                msg.attach(part)

        recipients = to + (cc or []) + (bcc or [])

        try:
            with smtplib.SMTP(self.host, self.port) as smtp:
                smtp.starttls()
                smtp.login(self.user, self.password)
                smtp.sendmail(self.user, recipients, msg.as_string())
            logging.info(
                "E-mail enviado -> To: %s; Cc: %s; Bcc: %s",
                "; ".join(to),
                "; ".join(cc) if cc else "-",
                "; ".join(bcc) if bcc else "-",
            )
        except Exception as e:
            logging.error("Erro no envio de e-mail: %s", e)


def smtp_client() -> SMTPClient:
    """Factory do cliente SMTP."""
    return SMTPClient()


def read_env_emails() -> Dict[str, List[str]]:
    """
    Lê EMAIL_PARA / EMAIL_CC / EMAIL_CCO do .env e devolve dict com listas.
    """
    return {
        "to": [m.strip() for m in os.getenv("EMAIL_PARA", "").split(",") if "@" in m],
        "cc": [m.strip() for m in os.getenv("EMAIL_CC", "").split(",") if "@" in m],
        "bcc": [m.strip() for m in os.getenv("EMAIL_CCO", "").split(",") if "@" in m],
    }


# =============================================================================
# Template HTML (email_base.html)
# =============================================================================
def read_template(filename: str) -> str:
    """
    Lê o template HTML (deve conter {{TITLE}}, {{CONTENT}}, {{FOOTER}}).
    Comentário <!-- EXTRA_CSS --> é opcional para CSS adicional (não usado aqui).
    """
    base = Path(__file__).resolve().parent
    path = base / filename
    return path.read_text(encoding="utf-8")


def render_email(
    template: str,
    title: str,
    content: str,
    footer: str,
    extra_css: Optional[str] = None,
) -> str:
    """
    Substitui placeholders do template e retorna o HTML final.
    """
    html = template.replace("{{TITLE}}", title)
    html = html.replace("{{CONTENT}}", content)
    html = html.replace("{{FOOTER}}", footer)
    if "<!-- EXTRA_CSS -->" in html and extra_css:
        html = html.replace("<!-- EXTRA_CSS -->", f"<style>{extra_css}</style>")
    return html


# =============================================================================
# XLSX utils (múltiplas abas)
# =============================================================================
# Conjunto de caracteres inválidos para nomes de planilhas no Excel
_INVALID_SHEET_CHARS: Set[str] = set("[]:*?/\\'")  # Excel não permite esses


def _safe_sheet_name_base(name: str) -> str:
    """
    Sanitiza o nome da aba:
    - remove/substitui caracteres inválidos
    - limita a 31 caracteres
    """
    clean = "".join(("-" if ch in _INVALID_SHEET_CHARS else ch) for ch in str(name))
    clean = clean.strip() or "Planilha"
    return clean[:31]


def safe_sheet_name(name: str, used: Optional[Set[str]] = None) -> str:
    """
    Sanitiza e garante unicidade (sufixos " (2)", " (3)"...) sem ultrapassar 31 chars.
    """
    base = _safe_sheet_name_base(name)
    if used is None:
        return base
    if base not in used:
        used.add(base)
        return base
    for i in range(2, 200):
        suffix = f" ({i})"
        lim = 31 - len(suffix)
        candidate = (base[:lim]).rstrip() + suffix
        if candidate not in used:
            used.add(candidate)
            return candidate
    fallback = f"{base[:27]} ({len(used)+1})"
    used.add(fallback)
    return fallback


def to_xlsx_bytes_multiplas_abas(dfs: Dict[str, pd.DataFrame]) -> bytes:
    """
    Constrói um XLSX em memória com múltiplas abas.
    - Usa openpyxl (compatível com Office)
    - Nomes de abas sanitizados
    """
    buf = io.BytesIO()
    used: Set[str] = set()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for raw_name, df in (dfs or {}).items():
            sheet = safe_sheet_name(raw_name, used)
            (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_excel(
                w, sheet_name=sheet, index=False
            )
    buf.seek(0)
    return buf.getvalue()


# =============================================================================
# Helpers de formatação e rótulos
# =============================================================================
def moeda_br(v) -> str:
    """Formata número para moeda BR (R$ 1.234,56) com 2 casas."""
    try:
        return (
            f"R$ {float(v):,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        )
    except Exception:
        return "R$ 0,00"


def label_filial(codigo) -> str:
    """
    Mapeia código de filial → rótulo legível.
    Ajuste conforme a necessidade da sua base.
    """
    return {"1": "FARMAUM PB", "2": "FARMAUM RN", "3": "BRASIL"}.get(
        str(codigo), str(codigo)
    )


def build_subject_corte(hoje: datetime, is_fechamento: bool) -> str:
    """
    Produz o assunto do e-mail:
    - Fechamento (dia 1): 'Sentinela · Corte · Fechamento - Mês/Ano'
    - Demais dias:        'Sentinela · Corte - dd/mm/aaaa HH:MM'
    """
    if is_fechamento:
        ref = hoje.replace(day=1) - timedelta(days=1)
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
        return f"Sentinela · Corte · Fechamento - {meses[ref.month - 1]}/{ref.year}"
    return f"Sentinela · Corte - {hoje.strftime('%d/%m/%Y %H:%M')}"


def build_attachment_name(hoje: datetime, is_fechamento: bool) -> str:
    """
    Monta o nome do anexo:
    - Sem underscore, com espaços, data ddmmyyyy
    - No fechamento, inclui 'Fechamento <Mês Ano>'
    """
    data_ddmmyyyy = hoje.strftime("%d%m%Y")
    if is_fechamento:
        ref = hoje.replace(day=1) - timedelta(days=1)
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
        return f"Sentinela Corte Fechamento {meses[ref.month - 1]} {ref.year} {data_ddmmyyyy}.xlsx"
    return f"Sentinela Corte {data_ddmmyyyy}.xlsx"
