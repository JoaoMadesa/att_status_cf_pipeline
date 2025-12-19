# -*- coding: utf-8 -*-
"""
pipeline_cf_status.py
MODELO C — BASE HISTÓRICA LOCAL (PARQUET) + INCREMENTAL

- Incremental da API via last_run.txt
- Histórico persistente em Parquet
- Merge pela CHAVE NF-e (PK absoluta)
- Snapshot completo enviado ao Google Sheets
"""

import os, json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import logging

# ===================== CONFIG =====================
CF_EMAIL = os.getenv("CF_EMAIL")
CF_SENHA = os.getenv("CF_SENHA")
CF_IDCLIENTE = int(os.getenv("CF_IDCLIENTE", "206"))
CF_IDPRODUTO = int(os.getenv("CF_IDPRODUTO", "1"))
LOOKBACK_DIAS = int(os.getenv("LOOKBACK_DIAS", "15"))

SHEET_ID = os.getenv("SHEET_ID")
SHEET_RANGE = "Entregues e Barrados!A2:E"

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "out_status"))
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_PARQUET = OUTPUT_DIR / "base_status.parquet"
LAST_RUN_PATH = OUTPUT_DIR / "last_run.txt"

DEXPARA_XLSX_PATH = os.getenv("DEXPARA_XLSX_PATH")
DEXPARA_SHEET = "TRANSPORTADORA"

BASE_URL = "https://utilities.confirmafacil.com.br"
LOGIN_URL = f"{BASE_URL}/login/login"
OCORR_URL = f"{BASE_URL}/filter/ocorrencia"

PAGE_SIZE = 1000
TIMEOUT = (5, 120)

DEBUG = os.getenv("DEBUG", "").lower() in ("1","true","yes")
logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO,
                    format="%(levelname)s: %(message)s")

# ===================== STATUS =====================
STATUS_MAP = {
    "1": "ENTREGUE",
    "2": "ENTREGUE",
    "37": "ENTREGUE",
    "999": "ENTREGUE",
    "25": "CANCELADO",
    "102": "CANCELADO",
    "203": "CANCELADO",
    "303": "CANCELADO",
    "325": "CANCELADO",
    "327": "CANCELADO",
    "200": "DADOS CONFIRMADOS",
    "201": "DADOS CONFIRMADOS",
    "202": "DADOS CONFIRMADOS",
    "7": "CONTATOS CONFIRMADOS",
    "206": "CONTATOS CONFIRMADOS",
}

PRIORITY = ["ENTREGUE","CANCELADO","DADOS CONFIRMADOS","CONTATOS CONFIRMADOS"]
PRIORITY_RANK = {s:i for i,s in enumerate(PRIORITY)}
ALL_CODES = ",".join(STATUS_MAP.keys())

COLS = ["CHAVE","NUMERO","SERIE","TRANSPORTADORA","STATUS","DATA_ULTIMA_OCORRENCIA"]

# ===================== UTIL =====================
def dt_api(dt, end=False):
    if end:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.strftime("%Y/%m/%d %H:%M:%S")

def periodo():
    now = datetime.now()
    if LAST_RUN_PATH.exists():
        di = datetime.fromisoformat(LAST_RUN_PATH.read_text()) + timedelta(seconds=1)
        logging.info(f"Incremental ON desde {di}")
    else:
        di = now - timedelta(days=LOOKBACK_DIAS)
        logging.info(f"Primeira execução (lookback {LOOKBACK_DIAS} dias)")
    return di, now

# ===================== API =====================
def make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods={"GET","POST"})
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def autenticar(session):
    r = session.post(LOGIN_URL, json={
        "email": CF_EMAIL,
        "senha": CF_SENHA,
        "idcliente": CF_IDCLIENTE,
        "idproduto": CF_IDPRODUTO
    }, timeout=TIMEOUT)
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

# ===================== COLETA =====================
def coletar_incremental(session, token, di, df):
    registros = {}
    for page in iter_respostas(session, token, di, df):
        for item in page:
            emb = item.get("embarque", {})
            chave = emb.get("chave")
            if not chave:
                continue

            serie = str(emb.get("serie",""))
            if serie == "3":
                continue

            codigo = str(item.get("tipoOcorrencia",{}).get("codigo",""))
            status = STATUS_MAP.get(codigo)
            if not status:
                continue

            data_occ = item.get("data")

            atual = registros.get(chave)
            if not atual or PRIORITY_RANK[status] < PRIORITY_RANK[atual["STATUS"]]:
                registros[chave] = {
                    "CHAVE": chave,
                    "NUMERO": emb.get("numero",""),
                    "SERIE": serie,
                    "TRANSPORTADORA": (emb.get("transportadora") or {}).get("nome",""),
                    "STATUS": status,
                    "DATA_ULTIMA_OCORRENCIA": data_occ,
                }

    return pd.DataFrame(registros.values(), columns=COLS)

# ===================== BASE LOCAL =====================
def carregar_base():
    if BASE_PARQUET.exists():
        return pd.read_parquet(BASE_PARQUET)
    return pd.DataFrame(columns=COLS)

def merge_base(base, novo):
    base = base.set_index("CHAVE")
    novo = novo.set_index("CHAVE")

    base.update(novo)
    novos = novo.loc[~novo.index.isin(base.index)]
    base = pd.concat([base, novos])

    return base.reset_index()

# ===================== DEXPARA =====================
def aplicar_dexpara(df):
    mapa = pd.read_excel(DEXPARA_XLSX_PATH, sheet_name=DEXPARA_SHEET,
                         usecols=[0,1], header=None, names=["O","N"], dtype=str)
    d = {o.strip().upper(): (n or "").strip()
         for o,n in mapa.values if isinstance(o,str)}
    df["TRANSPORTADORA"] = df["TRANSPORTADORA"].map(lambda x: d.get(str(x).upper(), x))
    return df

# ===================== SHEETS =====================
def gsheets():
    creds = Credentials.from_service_account_file(
        os.getenv("GOOGLE_CREDENTIALS_PATH"),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets","v4",credentials=creds)

def publicar(df):
    svc = gsheets()
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=SHEET_RANGE
    ).execute()

    values = df[["NUMERO","SERIE","CHAVE","TRANSPORTADORA","STATUS"]].values.tolist()
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

    df_inc = coletar_incremental(sess, token, di, df)
    base = carregar_base()
    base = merge_base(base, df_inc)
    base = aplicar_dexpara(base)

    base.to_parquet(BASE_PARQUET, index=False)
    publicar(base)

    LAST_RUN_PATH.write_text(df.isoformat())
    logging.info(f"Pipeline OK | Base histórica: {len(base)} registros")

if __name__ == "__main__":
    run()
