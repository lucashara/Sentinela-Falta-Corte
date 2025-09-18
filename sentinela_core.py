# sentinela_core.py
# ==============================================================================
# Núcleo compartilhado para os scripts "Sentinela · ...".
# Responsabilidades:
#   • Logging padronizado
#   • Utilitários de SQL (carregar arquivo .sql do diretório ./sql)
#   • Envio de e-mail via Office 365 (SMTP com STARTTLS)
#   • Renderização de HTML a partir de template base (email_base.html)
#   • Geração de XLSX em memória com múltiplas abas (com sanitização de nomes)
#   • Helpers: moeda_br, label_filial, compute_next_run, build_subject
#
# Variáveis de ambiente esperadas (.env):
#   EMAIL_USER, EMAIL_PASSWORD, OFFICE365_SMTP_SERVER, OFFICE365_SMTP_PORT
#   EMAIL_PARA, EMAIL_CC, EMAIL_CCO  (listas separadas por vírgula)
#   DB_* (no config_bd.py)
#
# Observação:
#   Este módulo NÃO acessa banco diretamente. O acesso é responsabilidade do
#   main.py (via config_bd.session_scope e SQLAlchemy).
# ==============================================================================

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
from dotenv import load_dotenv

# SMTP / MIME p/ envio de e-mail
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()


# ==============================================================================
# Logging
# ==============================================================================
def setup_logging(log_file: str) -> None:
    """
    Configura logging com saída em ./log/<log_file> e no console.
    - Cria a pasta ./log se não existir
    - Formato padrão dd/mm/aaaa HH:MM:SS [LEVEL] mensagem
    """
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


# ==============================================================================
# SQL utils
# ==============================================================================
def _sql_dir() -> Path:
    """Retorna o caminho padrão da pasta ./sql (ao lado dos scripts)."""
    return Path(__file__).resolve().parent / "sql"


def load_sql(filename: str) -> str:
    """
    Carrega um arquivo .sql da pasta ./sql exatamente pelo nome informado.

    Exemplo:
      sql_text = load_sql("sintetico_corte_falta.sql")
    """
    sql_dir = _sql_dir()
    path = sql_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Arquivo SQL não encontrado: {filename}")
    return path.read_text(encoding="utf-8")


# ==============================================================================
# E-mail (SMTP Office 365)
# ==============================================================================
class SMTPClient:
    """
    Cliente SMTP simples para Office 365 (STARTTLS).
    Variáveis:
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
        """
        Envia e-mail HTML.
        - subject: assunto
        - html: corpo em HTML (string completa)
        - to/cc/bcc: listas de destinatários
        - attachments: lista de tuplas (filename, bytes)
        - priority_high: se True, define X-Priority=1
        """
        if not to:
            logging.error("Nenhum destinatário definido (EMAIL_PARA).")
            return

        # Montagem MIME
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

        # Anexos (se houver)
        if attachments:
            for filename, content in attachments:
                part = MIMEApplication(content, Name=filename)
                part.add_header("Content-Disposition", "attachment", filename=filename)
                msg.attach(part)

        recipients = to + (cc or []) + (bcc or [])

        # Envio
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
    - Separação por vírgula (,)
    """
    return {
        "to": [m.strip() for m in os.getenv("EMAIL_PARA", "").split(",") if "@" in m],
        "cc": [m.strip() for m in os.getenv("EMAIL_CC", "").split(",") if "@" in m],
        "bcc": [m.strip() for m in os.getenv("EMAIL_CCO", "").split(",") if "@" in m],
    }


# ==============================================================================
# Template HTML
# ==============================================================================
def read_template(filename: str) -> str:
    """
    Lê um arquivo de template HTML do diretório raiz (ex.: email_base.html).
    Atenção:
      - O template precisa conter os placeholders {{TITLE}}, {{CONTENT}}, {{FOOTER}}
      - E opcionalmente o marcador <!-- EXTRA_CSS -->
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
    Renderiza o e-mail substituindo os placeholders do template.
    """
    html = template.replace("{{TITLE}}", title)
    html = html.replace("{{CONTENT}}", content)
    html = html.replace("{{FOOTER}}", footer)
    html = html.replace(
        "<!-- EXTRA_CSS -->", f"<style>{extra_css}</style>" if extra_css else ""
    )
    return html


# ==============================================================================
# XLSX utils (múltiplas abas)
# ==============================================================================
# Conjunto de caracteres inválidos para títulos de planilhas no Excel
_INVALID_SHEET_CHARS: Set[str] = set("[]:*?/\\'")


def safe_sheet_name(name: str, used: Optional[Set[str]] = None) -> str:
    r"""
    Sanitiza nome de aba:
      - Substitui caracteres inválidos [] : * ? / \ ' por '-'
      - Limita a 31 caracteres
      - Garante unicidade com sufixo " (2)", " (3)"... se já existir
    """
    clean = "".join(("-" if ch in _INVALID_SHEET_CHARS else ch) for ch in str(name))
    clean = clean.strip() or "Planilha"
    base = clean[:31]  # limite do Excel

    if used is None:
        return base

    if base not in used:
        used.add(base)
        return base

    # Resolve colisões por truncamento/sanitização
    for i in range(2, 200):
        suffix = f" ({i})"
        lim = 31 - len(suffix)
        candidate = (base[:lim]).rstrip() + suffix
        if candidate not in used:
            used.add(candidate)
            return candidate

    # Fallback (não deve chegar aqui em uso normal)
    fallback = f"{base[:27]} ({len(used)+1})"
    used.add(fallback)
    return fallback


def to_xlsx_bytes_multiplas_abas(dfs: Dict[str, pd.DataFrame]) -> bytes:
    """
    Constrói um XLSX em memória com múltiplas abas.
    Parâmetros:
      - dfs: dict {"Nome da Aba": DataFrame}
    Observações:
      - Nomes de abas são sanitizados (safe_sheet_name)
      - DataFrames vazios também são gravados (mantém estrutura)
    Retorno:
      - bytes prontos para anexar no e-mail
    """
    buf = io.BytesIO()
    used: Set[str] = set()

    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for raw_name, df in dfs.items():
            sheet_name = safe_sheet_name(raw_name, used)
            (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_excel(
                w, sheet_name=sheet_name, index=False
            )

    buf.seek(0)
    return buf.getvalue()


# ==============================================================================
# Helpers
# ==============================================================================
def moeda_br(v) -> str:
    """Formata número para moeda BR (R$ 1.234,56)."""
    try:
        return (
            f"R$ {float(v):,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        )
    except Exception:
        return "R$ 0,00"


def label_filial(codigo) -> str:
    """Mapeia código de filial → rótulo legível."""
    return {"1": "FARMAUM PB", "2": "FARMAUM RN", "3": "BRASIL"}.get(
        str(codigo), str(codigo)
    )


def build_subject(nome: str, dt: Optional[datetime] = None) -> str:
    """'Sentinela · <nome> - dd/mm/aaaa HH:MM'."""
    dt = dt or datetime.now()
    return f"Sentinela · {nome} - {dt.strftime('%d/%m/%Y %H:%M')}"


def compute_next_run(agenda: List[Dict]) -> datetime:
    """
    Dada uma agenda (lista de dicts com 'dias' [0..6] e 'horario' datetime.time),
    retorna a próxima data/hora de execução.
    """
    agora = datetime.now()
    prox = None
    for cfg in agenda:
        for d in cfg["dias"]:
            alvo = datetime.combine(
                (agora + timedelta(days=(d - agora.weekday()) % 7)).date(),
                cfg["horario"],
            )
            if alvo <= agora:
                alvo += timedelta(days=7)
            if prox is None or alvo < prox:
                prox = alvo
    return prox
