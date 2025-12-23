# att_status_cf_pipeline

Pipeline para atualizar o status de entregas no Google Sheets usando dados do Confirma Facil.
O processo e incremental (usa last_run.txt), mantem uma base historica em Parquet e publica
um snapshot completo na aba "Entregues e Barrados".

## Estrutura do repositorio
- `processoAtt.py`: pipeline principal (coleta, merge, DExPARA e publicacao)
- `data/DExPARA.xlsx`: mapeamento de transportadoras (origem -> novo)
- `.github/workflows/madesa_status_pipeline.yml`: GitHub Actions

## Como funciona (resumo)
1) Define periodo de coleta:
   - Se `out_status/last_run.txt` existir, busca a partir dele (+1 segundo)
   - Caso contrario, usa `LOOKBACK_DIAS`
2) Busca ocorrencias na API com codigos relevantes
3) Deduplica por CHAVE (prioridade de status + ultima ocorrencia)
4) Faz merge com base historica local (Parquet)
5) Aplica DExPARA em transportadoras
6) Publica o snapshot no Google Sheets

## Dependencias
Python 3.10+ recomendado.

Instale localmente:
```powershell
pip install pandas requests google-api-python-client google-auth google-auth-httplib2 urllib3 pyarrow openpyxl
```

## Variaveis de ambiente
Obrigatorias:
- `CF_EMAIL`
- `CF_SENHA`
- `SHEET_ID`
- `DEXPARA_XLSX_PATH`
- `GOOGLE_CREDENTIALS_PATH`

Opcionais (com padrao):
- `CF_IDCLIENTE` (default: 206)
- `CF_IDPRODUTO` (default: 1)
- `LOOKBACK_DIAS` (default: 15)
- `OUTPUT_DIR` (default: `out_status`)
- `DEBUG` (1/true/yes ativa logs mais verbosos)

## Rodar localmente
1) Ajuste as variaveis de ambiente (exemplo):
```powershell
$env:CF_EMAIL="seu_email"
$env:CF_SENHA="sua_senha"
$env:SHEET_ID="id_da_planilha"
$env:GOOGLE_CREDENTIALS_PATH="C:\caminho\gsa.json"
$env:DEXPARA_XLSX_PATH="C:\Users\j.rhoden\Desktop\git\trabalho\att_status_cf_pipeline\data\DExPARA.xlsx"
```

2) Execute:
```powershell
python .\processoAtt.py
```

## Saidas geradas
- `out_status/base_status.parquet`: base historica
- `out_status/last_run.txt`: controle do incremental
- Snapshot publicado na aba **Entregues e Barrados** do Sheets

## GitHub Actions
Workflow: `.github/workflows/madesa_status_pipeline.yml`

Triggers:
- Agendado (08:50 UTC)
- Manual (workflow_dispatch)

Secrets necessarios:
- `CF_EMAIL`
- `CF_SENHA`
- `SHEET_ID`
- `GSA_JSON_B64` (service account em base64)

Gerar base64 no Windows:
```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("C:\caminho\gsa.json"))
```

O workflow:
1) Instala dependencias
2) Escreve `secrets/gsa.json` a partir do secret base64
3) Restaura cache do incremental
4) Executa `processoAtt.py`

## Observacoes
- A aba de destino e fixa em `Entregues e Barrados`.
- Se o DExPARA nao existir ou credenciais estiverem ausentes, o script falha cedo com erro explicito.
- O cache incremental usa `out_status/last_run.txt` + `out_status/base_status.parquet`.
