# -*- coding: utf-8 -*-
"""
pipeline_cf_status.py
VERSÃO FINAL OTIMIZADA E VALIDADA COM JSON REAL DA API

Otimizações:
- Coleta única (todos códigos em uma chamada)
- Incremental (last_run.txt)
- Deduplicação por NF + Série + Chave + Transportadora
- Prioridade de status (ENTREGUE > CANCELADO > ...)
- Paginação dinâmica
- Extração correta do status via tipoOcorrencia.codigo
"""

import os, json, time
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import argparse
import logging

# ===================== CONFIG =====================
CF_EMAIL       = os.getenv("CF_EMAIL", "")
CF_SENHA       = os.getenv("CF_SENHA", "")
CF_IDCLIENTE   = int(os.getenv("CF_IDCLIENTE", "206"))
CF_IDPRODUTO   = int(os.getenv("CF_IDPRODUTO", "1"))
LOOKBACK_DIAS  = int(os.getenv("LOOKBACK_DIAS", "30"))

SHEET_ID   = os.getenv("SHEET_ID", "")
SHEET_RANGE = "Entregues e Barrados!A2:E"

GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

DEXPARA_XLSX_PATH = os.getenv("DEXPARA_XLSX_PATH")
DEXPARA_SHEET     = os.getenv("DEXPARA_SHEET", "TRANSPORTADORA")

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "out_status"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LAST_RUN_PATH = OUTPUT_DIR / "last_run.txt"
INCREMENTAL = True

BASE_URL  = "https://utilities.confirmafacil.com.br"
LOGIN_URL = f"{BASE_URL}/login/login"
OCORR_URL = f"{BASE_URL}/filter/ocorrencia"

PAGE_SIZE = 1000
TIMEOUT = (5, 120)

DEBUG = os.getenv("DEBUG", "").lower() in ("1","true","yes")
logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO,
                    format="%(levelname)s: %(message)s")

# ===================== STATUS =====================
CODES = {
    "ENTREGUE": "1,2,37,999",
    "CANCELADO": "25,102,203,303,325,327",
    "DADOS CONFIRMADOS": "200,201,202",
    "CONTATOS CONFIRMADOS": "7,206",
}

STATUS_PRIORITY = ["ENTREGUE", "CANCELADO", "DADOS CONFIRMADOS", "CONTATOS CONFIRMADOS"]
PRIORITY_RANK = {s: i for i, s in enumerate(STATUS_PRIORITY)}

STATUS_MAP = {
    c: st
    for st, codes in CODES.items()
    for c in codes.split(",")
}

ALL_CODES = ",".join(STATUS_MAP.keys())

# ===================== UTIL =====================
def _norm(x):
    return "" if pd.isna(x) else str(x).strip().upper()

def dt_api(dt, end=False):
    if end:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.strftime("%Y/%m/%d %H:%M:%S")

def periodo():
    agora = datetime.now()
    if INCREMENTAL and LAST_RUN_PATH.exists():
        di = datetime.fromisoformat(LAST_RUN_PATH.read_text()) + timedelta(seconds=1)
        logging.info(f"Incremental ON desde {di}")
    else:
        di = agora - timedelta(days=LOOKBACK_DIAS)
    return di, agora

# ===================== API =====================
def make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods={"GET","POST"})
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    return s

def autenticar(session):
    r = session.post(
        LOGIN_URL,
        json={
            "email": CF_EMAIL,
            "senha": CF_SENHA,
            "idcliente": CF_IDCLIENTE,
            "idproduto": CF_IDPRODUTO
        },
        timeout=TIMEOUT
    )
    r.raise_for_status()
    return r.json()["resposta"]["token"]

def fetch_page(session, token, params):
    r = session.get(OCORR_URL, headers={"Authorization": token},
                    params=params, timeout=TIMEOUT)
    if r.status_code != 200:
        return []
    return r.json().get("respostas", [])

def iter_respostas(session, token, di, df):
    page = 0
    while True:
        params = {
            "page": page,
            "size": PAGE_SIZE,
            "serie": "1,4",
            "de": dt_api(di),
            "ate": dt_api(df, True),
            "codigoOcorrencia": ALL_CODES,
            "tipoData": "OCORRENCIA",
        }
        data = fetch_page(session, token, params)
        if not data:
            break
        yield data
        page += 1

# ===================== EXTRAÇÃO =====================
def extract_codigo(item):
    try:
        return str(item["tipoOcorrencia"]["codigo"])
    except Exception:
        return ""

def should_replace(old, new):
    if not old:
        return True
    return PRIORITY_RANK[new] < PRIORITY_RANK[old]

def coletar_dados(session, token, di, df):
    best = {}

    for page in iter_respostas(session, token, di, df):
        for item in page:
            emb = item.get("embarque", {})
            serie = emb.get("serie", "")
            if str(serie) == "3":
                continue

            codigo = extract_codigo(item)
            status = STATUS_MAP.get(codigo)
            if not status:
                continue

            key = (
                emb.get("numero",""),
                emb.get("serie",""),
                emb.get("chave",""),
                (emb.get("transportadora") or {}).get("nome","")
            )

            if key not in best or should_replace(best[key]["STATUS"], status):
                best[key] = {
                    "NUMERO": key[0],
                    "SERIE": key[1],
                    "CHAVE": key[2],
                    "TRANSPORTADORA": key[3],
                    "STATUS": status
                }

    return pd.DataFrame(best.values())

# ===================== DExPARA =====================
def aplicar_dexpara(df):
    mapa = pd.read_excel(
        DEXPARA_XLSX_PATH,
        sheet_name=DEXPARA_SHEET,
        usecols=[0,1],
        header=None,
        names=["Original","Novo"],
        dtype=str
    )
    m = {_norm(o): str(n).strip() for o,n in mapa.values if _norm(o)}
    df["TRANSPORTADORA"] = df["TRANSPORTADORA"].map(lambda x: m.get(_norm(x), x))
    return df

# ===================== GOOGLE SHEETS =====================
def gsheets_service():
    if GOOGLE_CREDENTIALS_JSON:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDENTIALS_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    return build("sheets","v4",credentials=creds)

def publicar(df):
    svc = gsheets_service()
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=SHEET_RANGE
    ).execute()

    values = df.values.tolist()
    for i in range(0, len(values), 10000):
        svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=SHEET_RANGE,
            valueInputOption="RAW",
            body={"values": values[i:i+10000]}
        ).execute()

# ===================== RUN =====================
def run():
    di, df = periodo()
    sess = make_session()
    token = autenticar(sess)

    df_final = coletar_dados(sess, token, di, df)
    df_final = aplicar_dexpara(df_final)

    out_xlsx = OUTPUT_DIR / "ATUALIZACAO_DE_STATUS.xlsx"
    df_final.to_excel(out_xlsx, index=False)

    publicar(df_final)

    LAST_RUN_PATH.write_text(df.isoformat())
    logging.info(f"Pipeline finalizado ({len(df_final)} linhas).")

if __name__ == "__main__":
    run()
