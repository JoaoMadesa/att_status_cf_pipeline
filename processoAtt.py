# -*- coding: utf-8 -*-
"""
processoAtt.py
MODELO C — BASE HISTÓRICA LOCAL (PARQUET) + INCREMENTAL

- Janela fixa ontem/hoje; lookback inicial sem base
- Histórico persistente em Parquet
- Merge pela CHAVE NF-e (PK absoluta)
- Snapshot completo enviado ao Google Sheets
- Logs detalhados de progresso
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
import logging

# ===================== CONFIG =====================
CF_EMAIL = os.getenv("CF_EMAIL")
CF_SENHA = os.getenv("CF_SENHA")
CF_IDCLIENTE = int(os.getenv("CF_IDCLIENTE", "206"))
CF_IDPRODUTO = int(os.getenv("CF_IDPRODUTO", "1"))
LOOKBACK_DIAS = int(os.getenv("LOOKBACK_DIAS", "15"))

SHEET_ID = os.getenv("SHEET_ID")

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

DEBUG = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

# ===================== STATUS =====================
STATUS_MAP = {
    "1": "ENTREGUE", "2": "ENTREGUE", "37": "ENTREGUE", "999": "ENTREGUE",
    "25": "CANCELADO", "102": "CANCELADO", "203": "CANCELADO",
    "303": "CANCELADO", "325": "CANCELADO", "327": "CANCELADO",
    "200": "DADOS CONFIRMADOS", "201": "DADOS CONFIRMADOS", "202": "DADOS CONFIRMADOS",
    "7": "CONTATOS CONFIRMADOS", "206": "CONTATOS CONFIRMADOS",
}

PRIORITY = ["ENTREGUE", "CANCELADO", "DADOS CONFIRMADOS", "CONTATOS CONFIRMADOS"]
PRIORITY_RANK = {s: i for i, s in enumerate(PRIORITY)}
ALL_CODES = ",".join(STATUS_MAP.keys())

COLS = ["CHAVE", "NUMERO", "SERIE", "TRANSPORTADORA", "STATUS", "DATA_ULTIMA_OCORRENCIA"]

# ===================== UTIL =====================
def dt_api(dt, end=False):
    if end:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.strftime("%Y/%m/%d %H:%M:%S")

def periodo():
    now = datetime.now()
    if BASE_PARQUET.exists():
        di = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        logging.info("Janela fixa | Buscando ocorrencias de ontem 00:00 ate hoje 23:59:59")
    else:
        di = (now - timedelta(days=LOOKBACK_DIAS)).replace(hour=0, minute=0, second=0, microsecond=0)
        logging.info(f"Sem base historica | Lookback = {LOOKBACK_DIAS} dias")
    return di, now

def validar_config():
    faltando = []
    for nome, valor in [
        ("CF_EMAIL", CF_EMAIL),
        ("CF_SENHA", CF_SENHA),
        ("SHEET_ID", SHEET_ID),
        ("DEXPARA_XLSX_PATH", DEXPARA_XLSX_PATH),
    ]:
        if not valor:
            faltando.append(nome)
    if faltando:
        raise RuntimeError(f"Variaveis obrigatorias ausentes: {', '.join(faltando)}")
    if not os.path.exists(DEXPARA_XLSX_PATH):
        raise RuntimeError(f"Arquivo DExPARA nao encontrado: {DEXPARA_XLSX_PATH}")

# ===================== API =====================
def make_session():
    s = requests.Session()
    retries = Retry(
        total=3, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET", "POST"}
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def autenticar(session):
    logging.info("Autenticando na API Confirma Fácil...")
    r = session.post(LOGIN_URL, json={
        "email": CF_EMAIL,
        "senha": CF_SENHA,
        "idcliente": CF_IDCLIENTE,
        "idproduto": CF_IDPRODUTO
    }, timeout=TIMEOUT)
    r.raise_for_status()
    logging.info("Autenticação OK.")
    return r.json()["resposta"]["token"]

def fetch_page(session, token, params):
    r = session.get(OCORR_URL, headers={"Authorization": token},
                    params=params, timeout=TIMEOUT)
    if r.status_code == 401:
        logging.warning("Token expirado (401). Reautenticando e repetindo a pagina...")
        token = autenticar(session)
        r = session.get(OCORR_URL, headers={"Authorization": token},
                        params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json(), token

def iter_respostas(session, token, di, df):
    total = 0
    t0 = time.perf_counter()

    params = {
        "page": 0,
        "size": PAGE_SIZE,
        "serie": "1,4,6",
        "de": dt_api(di),
        "ate": dt_api(df, True),
        "codigoOcorrencia": ALL_CODES,
        "tipoData": "CRIACAO",
    }

    payload, token = fetch_page(session, token, params)
    respostas = payload.get("respostas", []) or []
    total_pages = int(payload.get("totalPages", 0) or 0)
    if respostas:
        total += len(respostas)
        logging.info(f"Pagina 0 coletada | {len(respostas)} ocorrencias | Total parcial {total}")
        yield respostas

    if total_pages <= 0:
        total_pages = 1

    for page in range(1, total_pages):
        params["page"] = page
        payload, token = fetch_page(session, token, params)
        respostas = payload.get("respostas", []) or []
        if not respostas:
            continue
        total += len(respostas)
        logging.info(f"Pagina {page} coletada | {len(respostas)} ocorrencias | Total parcial {total}")
        yield respostas

    logging.info(f"Coleta finalizada | {total} ocorrencias brutas | {_fmt(time.perf_counter()-t0)}")

def _fmt(s):
    h = int(s)//3600
    m = (int(s)%3600)//60
    sec = int(s)%60
    return f"{h:02d}:{m:02d}:{sec:02d}"

# ===================== COLETA =====================
def coletar_incremental(session, token, di, df):
    registros = {}
    for page in iter_respostas(session, token, di, df):
        for item in page:
            emb = item.get("embarque", {})
            chave = emb.get("chave")
            if not chave:
                continue

            serie = str(emb.get("serie", ""))
            if serie == "3":
                continue

            codigo = str(item.get("tipoOcorrencia", {}).get("codigo", ""))
            status = STATUS_MAP.get(codigo)
            if not status:
                continue

            data_occ = item.get("data")
            data_occ_dt = pd.to_datetime(data_occ, errors="coerce")
            atual = registros.get(chave)

            if not atual:
                registros[chave] = {
                    "CHAVE": chave,
                    "NUMERO": emb.get("numero", ""),
                    "SERIE": serie,
                    "TRANSPORTADORA": (emb.get("transportadora") or {}).get("nome", ""),
                    "STATUS": status,
                    "DATA_ULTIMA_OCORRENCIA": data_occ,
                }
                continue

            atual_rank = PRIORITY_RANK[atual["STATUS"]]
            novo_rank = PRIORITY_RANK[status]
            if novo_rank < atual_rank:
                registros[chave] = {
                    "CHAVE": chave,
                    "NUMERO": emb.get("numero", ""),
                    "SERIE": serie,
                    "TRANSPORTADORA": (emb.get("transportadora") or {}).get("nome", ""),
                    "STATUS": status,
                    "DATA_ULTIMA_OCORRENCIA": data_occ,
                }
            elif novo_rank == atual_rank:
                atual_dt = pd.to_datetime(atual.get("DATA_ULTIMA_OCORRENCIA"), errors="coerce")
                if pd.isna(atual_dt) or (not pd.isna(data_occ_dt) and data_occ_dt > atual_dt):
                    registros[chave] = {
                        "CHAVE": chave,
                        "NUMERO": emb.get("numero", ""),
                        "SERIE": serie,
                        "TRANSPORTADORA": (emb.get("transportadora") or {}).get("nome", ""),
                        "STATUS": status,
                        "DATA_ULTIMA_OCORRENCIA": data_occ,
                    }

    df = pd.DataFrame(registros.values(), columns=COLS)
    logging.info(f"Após deduplicação: {len(df)} registros únicos por CHAVE")
    return df

# ===================== BASE LOCAL =====================
def carregar_base():
    if BASE_PARQUET.exists():
        df = pd.read_parquet(BASE_PARQUET)
        logging.info(f"Base histórica carregada | {len(df)} registros")
        return df
    logging.info("Base histórica inexistente | Criando nova")
    return pd.DataFrame(columns=COLS)

def merge_base(base, novo):
    if novo.empty:
        logging.info("Nenhuma atualização incremental para aplicar.")
        return base

    base = base.set_index("CHAVE")
    novo = novo.set_index("CHAVE")

    base.update(novo)
    novos = novo.loc[~novo.index.isin(base.index)]
    base = pd.concat([base, novos])

    logging.info(f"Merge concluído | Base total agora com {len(base)} registros")
    return base.reset_index()

# ===================== DEXPARA =====================
def aplicar_dexpara(df):
    mapa = pd.read_excel(
        DEXPARA_XLSX_PATH,
        sheet_name=DEXPARA_SHEET,
        usecols=[0, 1],
        header=None,
        names=["O", "N"],
        dtype=str
    )
    d = {o.strip().upper(): (n or "").strip()
         for o, n in mapa.values if isinstance(o, str)}

    df["TRANSPORTADORA"] = df["TRANSPORTADORA"].map(lambda x: d.get(str(x).upper(), x))
    logging.info("DExPARA aplicado na transportadora")
    return df

# ===================== SHEETS =====================
def gsheets():
    path = os.getenv("GOOGLE_CREDENTIALS_PATH")
    if not path or not os.path.exists(path):
        raise RuntimeError(f"Credenciais do Google não encontradas: {path}")

    creds = Credentials.from_service_account_file(
        path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def publicar(df):
    logging.info("Publicando snapshot no Google Sheets (modo overwrite real)...")
    svc = gsheets()

    # 1️⃣ Descobrir sheetId da aba
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    sheets = meta["sheets"]

    aba_nome = "Entregues e Barrados"
    sheet_id = None

    for s in sheets:
        if s["properties"]["title"] == aba_nome:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
        raise RuntimeError(f"Aba '{aba_nome}' não encontrada no Sheets.")

    linhas = len(df) + 1  # +1 se tiver cabeçalho
    colunas = 5

    # 2️⃣ RESETAR TAMANHO DA ABA (isso resolve o erro)
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "rowCount": linhas,
                                "columnCount": colunas
                            }
                        },
                        "fields": "gridProperties"
                    }
                }
            ]
        }
    ).execute()

    # 3️⃣ LIMPAR VALORES
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{aba_nome}!A1:E"
    ).execute()

    # 4️⃣ ESCREVER DE UMA VEZ (overwrite real)
    values = [df.columns.tolist()] + df.values.tolist()

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{aba_nome}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    logging.info("Publicação concluída com overwrite real (sem crescimento de células).")


# ===================== RUN =====================
def run():
    t0 = time.perf_counter()

    validar_config()
    di, df = periodo()
    sess = make_session()
    token = autenticar(sess)

    df_inc = coletar_incremental(sess, token, di, df)
    base = carregar_base()
    base = merge_base(base, df_inc)
    base = aplicar_dexpara(base)

    base.to_parquet(BASE_PARQUET, index=False)
    LAST_RUN_PATH.write_text(df.isoformat())

    publicar(base)

    logging.info(f"Pipeline FINALIZADO com sucesso | Tempo total {_fmt(time.perf_counter()-t0)}")

if __name__ == "__main__":
    run()
