# sentinela_core.py
# Núcleo compartilhado para os scripts "Sentinela · ..."

import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()


# --------------------------- Logging --------------------------- #
def setup_logging(log_file: str) -> None:
    """Configura logging com saída em arquivo ./log/<log_file> e no console."""
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


# --------------------------- SQL utils --------------------------- #
def _sql_dir() -> Path:
    return Path(__file__).resolve().parent / "sql"


def _normalize_name(name: str) -> str:
    return name.lower().replace(" ", "").replace("_", "")


def load_sql(filename: str) -> str:
    """
    Carrega um arquivo SQL da pasta ./sql (tolerante a espaço/underscore/caixa).
    """
    sql_dir = _sql_dir()
    sql_dir.mkdir(parents=True, exist_ok=True)

    direct = sql_dir / filename
    if direct.exists():
        return direct.read_text(encoding="utf-8")

    wanted = _normalize_name(filename)
    for p in sql_dir.glob("*.sql"):
        if _normalize_name(p.name) == wanted:
            logging.info("Usando SQL detectado (fuzzy): %s", p.name)
            return p.read_text(encoding="utf-8")

    raise FileNotFoundError(f"Arquivo SQL não encontrado: {filename}")


# --------------------------- E-mail utils --------------------------- #
class SMTPClient:
    """Wrapper simples para envio de e-mails via SMTP (Office365, etc.)."""

    def __init__(self):
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
        priority_high: bool = False,
    ) -> None:
        if not to:
            logging.error(
                "Nenhum destinatário (EMAIL_PARA) definido. Cancelando envio."
            )
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
    """Factory de cliente SMTP (Office 365 por padrão)."""
    return SMTPClient()


def read_env_emails() -> Dict[str, List[str]]:
    """Lê EMAIL_PARA / EMAIL_CC / EMAIL_CCO do .env."""
    return {
        "to": [m.strip() for m in os.getenv("EMAIL_PARA", "").split(",") if "@" in m],
        "cc": [m.strip() for m in os.getenv("EMAIL_CC", "").split(",") if "@" in m],
        "bcc": [m.strip() for m in os.getenv("EMAIL_CCO", "").split(",") if "@" in m],
    }


# --------------------------- Template utils --------------------------- #
def read_template(filename: str) -> str:
    base = Path(__file__).resolve().parent
    path = base / filename
    return path.read_text(encoding="utf-8")


def render_email(
    template: str, title: str, content: str, footer: str, extra_css: str = None
) -> str:
    html = template.replace("{{TITLE}}", title)
    html = html.replace("{{CONTENT}}", content)
    html = html.replace("{{FOOTER}}", footer)
    if extra_css:
        html = html.replace("<!-- EXTRA_CSS -->", f"<style>{extra_css}</style>")
    else:
        html = html.replace("<!-- EXTRA_CSS -->", "")
    return html


# --------------------------- Helpers comuns --------------------------- #
def moeda_br(v) -> str:
    try:
        return (
            f"R$ {float(v):,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        )
    except Exception:
        return "R$ 0,00"


def label_filial(codigo) -> str:
    return {"1": "FARMAUM PB", "2": "FARMAUM RN", "3": "BRASIL"}.get(
        str(codigo), str(codigo)
    )


def compute_next_run(agenda: List[Dict]) -> datetime:
    """Dada uma agenda (dias + horário), retorna próxima execução."""
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
