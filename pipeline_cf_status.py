# -*- coding: utf-8 -*-
"""
pipeline_cf_status.py
Pipeline completo: CF API → 4 CSVs → DE→PARA → Excel → Google Sheets
"""

import os, json, time
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import argparse
import logging

# ===================== CONFIG via ENV =====================
# Confirma Fácil
CF_EMAIL       = os.getenv("CF_EMAIL", "")
CF_SENHA       = os.getenv("CF_SENHA", "")
CF_IDCLIENTE   = int(os.getenv("CF_IDCLIENTE", "206"))
CF_IDPRODUTO   = int(os.getenv("CF_IDPRODUTO", "1"))
LOOKBACK_DIAS  = int(os.getenv("LOOKBACK_DIAS", "90"))

# Google Sheets
SHEET_ID   = os.getenv("SHEET_ID", "")  # obrigatório em CI
SHEET_RANGE = "Entregues e Barrados!A2:E"
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

# DE→PARA
DEXPARA_XLSX_PATH = os.getenv("DEXPARA_XLSX_PATH", str(Path("data") / "DExPARA.xlsx"))
DEXPARA_SHEET     = os.getenv("DEXPARA_SHEET", "TRANSPORTADORA")

# Saída
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", str(Path("out_status"))))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Requests
BASE_URL   = "https://utilities.confirmafacil.com.br"
LOGIN_URL  = f"{BASE_URL}/login/login"
OCORR_URL  = f"{BASE_URL}/filter/ocorrencia"
PAGE_SIZE  = int(os.getenv("PAGE_SIZE", "500"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
TIMEOUT    = (5, 120)
TOTAL_RETRIES = 3
BACKOFF      = 1

# Códigos de ocorrência
CODES = {
    "ENTREGUES": "1,2,37,999",             # :contentReference[oaicite:2]{index=2}
    "CANCELADOS": "25,102,203,303,325,327",# :contentReference[oaicite:3]{index=3}
    "DADOS CONFIRMADOS": "200,201,202",    # :contentReference[oaicite:4]{index=4}
    "CONTATOS CONFIRMADOS": "7,206",       # :contentReference[oaicite:5]{index=5}
}

# DEBUG
DEBUG = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO, format="%(levelname)s: %(message)s")

# ===================== Helpers =====================
def _fmt(s):  # duração bonitinha
    ms = int((s - int(s)) * 1000); h = int(s)//3600; m = (int(s)%3600)//60; sec = int(s)%60
    return f"{h:02d}:{m:02d}:{sec:02d}.{ms:03d}"

def _norm(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()


def periodo(dias):
    hoje = datetime.today()
    di = (hoje - timedelta(days=dias)).strftime("%d-%m-%Y")
    df = hoje.strftime("%d-%m-%Y")
    logging.info(f"Período: {di} até {df}")
    return di, df

def make_session(max_pool=40, total_retries=TOTAL_RETRIES, backoff=BACKOFF):
    s = requests.Session()
    retries = Retry(
        total=total_retries, connect=total_retries, read=total_retries,
        backoff_factor=backoff, status_forcelist=[429,500,502,503,504],
        allowed_methods=frozenset({"GET","POST"}), raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=max_pool, pool_maxsize=max_pool, max_retries=retries)
    s.mount("https://", adapter); s.mount("http://", adapter)
    s.headers.update({"Accept-Encoding": "gzip, deflate"})
    return s

def autenticar(session: requests.Session) -> str:
    if not CF_EMAIL or not CF_SENHA:
        raise RuntimeError("Defina CF_EMAIL e CF_SENHA (env).")
    payload = {"email": CF_EMAIL, "senha": CF_SENHA, "idcliente": CF_IDCLIENTE, "idproduto": CF_IDPRODUTO}
    r = session.post(LOGIN_URL, headers={"Content-Type":"application/json"}, data=json.dumps(payload), timeout=TIMEOUT)
    r.raise_for_status()
    token = r.json().get("resposta", {}).get("token")
    if not token:
        raise RuntimeError("Falha na autenticação: token não retornado.")
    return token

def montar_params(di, df, page, codigos):
    return {
        "page": page, "size": PAGE_SIZE, "serie": "1,4",
        "de":  datetime.strptime(di, "%d-%m-%Y").strftime("%Y/%m/%d 00:00:00"),
        "ate": datetime.strptime(df, "%d-%m-%Y").strftime("%Y/%m/%d 23:59:59"),
        "codigoOcorrencia": codigos, "tipoData": "OCORRENCIA",
    }

def fetch_page(session, token, params):
    r = session.get(OCORR_URL, headers={"Authorization": token, "accept": "application/json"}, params=params, timeout=TIMEOUT)
    try:
        r.raise_for_status()
        data = r.json().get("respostas", []) or []
        if DEBUG:
            logging.debug(f"Fetched page {params.get('page')} size={len(data)}")
        return data
    except Exception as e:
        snippet = (r.text[:500] + "...") if r is not None else ""
        logging.warning(f"fetch_page falhou para page={params.get('page')}: {e} - resposta: {snippet}")
        raise

def consultar(session, token, di, df, codigos: str):
    params0 = montar_params(di, df, page=0, codigos=codigos)
    r0 = session.get(OCORR_URL, headers={"Authorization": token, "accept":"application/json"}, params=params0, timeout=TIMEOUT)
    try:
        r0.raise_for_status()
        j0 = r0.json()
    except Exception as e:
        snippet = (r0.text[:1000] + "...") if r0 is not None else ""
        logging.error(f"Falha ao obter página 0: {e} - conteúdo: {snippet}")
        raise

    total_pages = int(j0.get("totalPages", 0))
    resultados = j0.get("respostas", []) or []
    logging.info(f"Consulta codigos={codigos} -> totalPages={total_pages} page0_len={len(resultados)}")

    if total_pages <= 1:
        return resultados

    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for p in range(1, total_pages):
            futures.append(ex.submit(fetch_page, session, token, montar_params(di, df, p, codigos)))
        for f in as_completed(futures):
            try:
                page_res = f.result()
                resultados.extend(page_res)
            except Exception as e:
                logging.warning(f"[WARN] Página falhou: {e}")
    logging.info(f"Total respostas coletadas para codigos={codigos}: {len(resultados)}")
    return resultados

def format_data_iso_to_br(iso_str: str) -> str:
    if not iso_str: return ""
    try:
        return datetime.fromisoformat(iso_str).strftime("%d/%m/%Y")
    except ValueError:
        try:
            return datetime.strptime(iso_str.replace("T"," ").replace("Z",""), "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
        except Exception:
            return ""

def extrair_df(respostas) -> pd.DataFrame:
    linhas = []
    for item in respostas:
        emb = item.get("embarque") or {}
        entregas = emb.get("entregas") or []
        # Se houver múltiplas entregas, crie uma linha por entrega (antes pegava apenas a primeira)
        if entregas:
            for ent in entregas:
                data_raw = ent.get("dataEntrega", "") if ent else ""
                linhas.append({
                    "NF":            emb.get("numero", ""),
                    "Serie":         emb.get("serie", ""),  # alguns endpoints não trazem; ficará vazio
                    "Transportadora":(emb.get("transportadora") or {}).get("nome", ""),
                    "Chave":         emb.get("chave", ""),
                    "Pedido":        (emb.get("pedido") or {}).get("numero", ""),
                    "DataEntrega":   format_data_iso_to_br(data_raw),
                })
            if DEBUG and len(entregas) > 1:
                logging.debug(f"embarque {emb.get('numero','?')} tem {len(entregas)} entregas; expandi para {len(entregas)} linhas")
        else:
            linhas.append({
                "NF":            emb.get("numero", ""),
                "Serie":         emb.get("serie", ""),
                "Transportadora":(emb.get("transportadora") or {}).get("nome", ""),
                "Chave":         emb.get("chave", ""),
                "Pedido":        (emb.get("pedido") or {}).get("numero", ""),
                "DataEntrega":   "",
            })
    df = pd.DataFrame(linhas).astype(str).fillna("")
    return df

def salvar_csv(df: pd.DataFrame, nome_base: str) -> Path:
    path = OUTPUT_DIR / f"{nome_base}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logging.info(f"arquivo: {path} ({len(df):,} linhas)")
    return path

# ===================== ETAPA 1: gerar 4 CSVs =====================
def gerar_todos_csvs(lookback: int):
    t0 = time.perf_counter()
    di, df = periodo(lookback)
    sess = make_session(); token = autenticar(sess)

    feitos = {}
    for nome, cod in CODES.items():
        tA = time.perf_counter()
        resp = consultar(sess, token, di, df, cod)
        d   = extrair_df(resp)
        feitos[nome] = salvar_csv(d, nome)  # ENTREGUES.csv, etc
        logging.info(f"{nome}: {_fmt(time.perf_counter()-tA)}")

    logging.info(f"CSVs gerados em {_fmt(time.perf_counter()-t0)}")
    return feitos

# ===================== ETAPA 2: DE→PARA + Excel =====================
def update_status(_output_dir: Path):
    # Caminhos dos 4 CSVs recém-gerados
    file_paths = {
        "ENTREGUE":           str(_output_dir / "ENTREGUES.csv"),
        "CANCELADO":          str(_output_dir / "CANCELADOS.csv"),
        "DADOS CONFIRMADOS":  str(_output_dir / "DADOS CONFIRMADOS.csv"),
        "CONTATOS CONFIRMADOS": str(_output_dir / "CONTATOS CONFIRMADOS.csv"),
    }

    planilhas = {}
    for status, p in file_paths.items():
        df = pd.read_csv(p, sep=",", dtype=str, encoding="utf-8", keep_default_na=False)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={
            "NF":"NUMERO", "Serie":"SERIE", "Transportadora":"TRANSPORTADORA",
            "Chave":"CHAVE", "Pedido":"PEDIDO", "DataEntrega":"DATA_ENTREGA",
        })
        df["STATUS"] = status
        for col in ["NUMERO","SERIE","TRANSPORTADORA","STATUS"]:
            if col not in df.columns: df[col] = ""
        # filtro SERIE != 3
        s_num = pd.to_numeric(df["SERIE"], errors="coerce")
        df = df[s_num != 3]
        planilhas[status] = df[["NUMERO","SERIE","CHAVE","TRANSPORTADORA","STATUS"]].copy()

    ordem = ["ENTREGUE","CANCELADO","DADOS CONFIRMADOS","CONTATOS CONFIRMADOS"]
    notas = set()
    for st in ordem:
        if st not in planilhas: continue
        chave = planilhas[st][["NUMERO","TRANSPORTADORA"]].apply(tuple, axis=1)
        planilhas[st] = planilhas[st][~chave.isin(notas)]
        notas.update(planilhas[st][["NUMERO", "TRANSPORTADORA"]].apply(tuple, axis=1))

    df_final = pd.concat([planilhas[st] for st in ordem if st in planilhas], ignore_index=True)

    # DE→PARA
    mapa = pd.read_excel(DEXPARA_XLSX_PATH, sheet_name=DEXPARA_SHEET, usecols=[0,1],
                         header=None, names=["Original","Novo"], dtype=str, engine="openpyxl")
    mapping = {}
    for _, r in mapa.iterrows():
        o = _norm(r["Original"]); n = "" if pd.isna(r["Novo"]) else str(r["Novo"]).strip()
        if o: mapping[o] = n

    orig_transp = df_final["TRANSPORTADORA"].copy()
    nao_mapeadas = (orig_transp[~orig_transp.map(lambda x: _norm(x) in mapping)].dropna().unique().tolist())
    df_final["TRANSPORTADORA"] = orig_transp.map(lambda x: mapping.get(_norm(x), x))
    df_final = df_final.dropna(subset=["NUMERO","TRANSPORTADORA"]).astype(str)

    out_xlsx = OUTPUT_DIR / "ATUALIZACAO_DE_STATUS.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df_final.to_excel(w, index=False, sheet_name="Atualizacao de Status")
        pd.DataFrame(sorted(nao_mapeadas), columns=["TRANSPORTADORA_NAO_ENCONTRADA"]).to_excel(
            w, index=False, sheet_name="Transportadoras Nao Encontradas"
        )
    logging.info(f"Excel gerado: {out_xlsx}")
    return out_xlsx

# ===================== ETAPA 3: Google Sheets =====================
def gsheets_service():
    # 1) JSON inline via env
    if GOOGLE_CREDENTIALS_JSON:
        info = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        return build("sheets","v4",credentials=creds)
    # 2) caminho para arquivo
    p = Path(os.path.expandvars(GOOGLE_CREDENTIALS_PATH)).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"Credenciais não encontradas em {p}")
    creds = Credentials.from_service_account_file(str(p), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds)

def clear_google_sheet():
    if not SHEET_ID: raise RuntimeError("Defina SHEET_ID")
    svc = gsheets_service()
    svc.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=SHEET_RANGE).execute()
    logging.info("Limpou aba no Google Sheets.")

def copy_to_google_sheet(xlsx_path: Path):
    if not SHEET_ID: raise RuntimeError("Defina SHEET_ID")
    svc = gsheets_service()
    df = pd.read_excel(xlsx_path, sheet_name="Atualizacao de Status", dtype=str).fillna("")
    df = df.reindex(columns=["NUMERO","SERIE","CHAVE","TRANSPORTADORA","STATUS"])
    body = {"values": df.values.tolist()}
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=SHEET_RANGE, valueInputOption="RAW", body=body
    ).execute()
    logging.info("Dados publicados no Google Sheets.")

# ===================== ORQUESTRAÇÃO =====================
def run_pipeline(lookback: int, clear_first: bool = True):
    t0 = time.perf_counter()
    gerar_todos_csvs(lookback)
    xlsx = update_status(OUTPUT_DIR)
    
    if clear_first:
        clear_google_sheet()  # limpa apenas a planilha principal

    # Atualiza planilha principal
    copy_to_google_sheet(xlsx)

def cli():
    p = argparse.ArgumentParser(description="Pipeline CF → 4 CSVs → DE→PARA → Excel → Google Sheets")
    p.add_argument("--run", action="store_true", help="Executa o pipeline completo.")
    p.add_argument("--lookback", type=int, default=LOOKBACK_DIAS, help="Dias para trás (padrão: env LOOKBACK_DIAS ou 30).")
    p.add_argument("--noclear", action="store_true", help="Não limpar a aba do Google antes de colar.")
    args = p.parse_args()
    if args.run:
        run_pipeline(args.lookback, clear_first=not args.noclear)
    else:
        # modo simples: só gera CSVs e Excel (sem Google)
        gerar_todos_csvs(args.lookback)
        update_status(OUTPUT_DIR)
        logging.info("Rode com --run para publicar no Google.")
if __name__ == "__main__":
    cli()
