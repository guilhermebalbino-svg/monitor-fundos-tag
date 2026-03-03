"""
Monitor de Fundos Condominiais TAG
Busca dados de cotas na API da CVM e benchmarks de fontes públicas.
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import io
import zipfile
from datetime import datetime, date, timedelta
import calendar

# ──────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Monitor de Fundos TAG",
    page_icon="📊",
    layout="wide",
)

# ──────────────────────────────────────────────────────────────────────────────
# FUND CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
# cnpj (digits only), display_name, tx_gestao, liquidez, pub_alvo
FUND_GROUPS = [
    {
        "group": "DI / TIT PUB",
        "funds": [
            {
                "cnpj": "52818575000110",
                "name": "TB PORTO SELIC SIMPLES FI RF",
                "tx_gestao": "0,10%",
                "liquidez": "D0",
                "pub_alvo": "Geral",
            },
        ],
        "benchmarks": [
            {"name": "CDI ACUMULADO", "key": "cdi"},
            {"name": "IMAB5", "key": "imab5"},
            {"name": "IMAB", "key": "imab"},
            {"name": "IMAB5+", "key": "imab5plus"},
        ],
    },
    {
        "group": "CRÉDITO",
        "funds": [
            {
                "cnpj": "11145288000109",
                "name": "TB PORTO FIC MULTIMERCADO",
                "tx_gestao": "0,45%",
                "liquidez": "D1",
                "pub_alvo": "Qualificado",
            },
            {
                "cnpj": "44417536000182",
                "name": "VIT HIGH YIELD - QUALIFICADO",
                "tx_gestao": "0,45%",
                "liquidez": "D180",
                "pub_alvo": "Qualificado",
            },
            {
                "cnpj": "41033372000100",
                "name": "VIT HIGH YIELD 180 - PROFISSIONAL",
                "tx_gestao": "N/A",
                "liquidez": "D180",
                "pub_alvo": "Profissional",
            },
            {
                "cnpj": "24068085000108",
                "name": "TB HIGH YIELD",
                "tx_gestao": "0,95%",
                "liquidez": "D180",
                "pub_alvo": "Profissional",
            },
        ],
        "benchmarks": [
            {"name": "JGP Debêntures CDI+", "key": "cdi"},
        ],
    },
    {
        "group": "MULTIMERCADO",
        "funds": [
            {
                "cnpj": "42479970000161",
                "name": "VIT MM",
                "tx_gestao": "N/A",
                "liquidez": "D30",
                "pub_alvo": "Qualificado",
            },
            {
                "cnpj": "36014032000193",
                "name": "TAG NOVOS GESTORES",
                "tx_gestao": "0,45%",
                "liquidez": "D33",
                "pub_alvo": "Qualificado",
            },
        ],
        "benchmarks": [
            {"name": "CDI ACUMULADO", "key": "cdi"},
        ],
    },
    {
        "group": "AÇÕES",
        "funds": [
            {
                "cnpj": "42562516000170",
                "name": "VIT AÇÕES",
                "tx_gestao": "N/A",
                "liquidez": "D30",
                "pub_alvo": "Qualificado",
            },
            {
                "cnpj": "13574572000126",
                "name": "TRANCOSO FIA",
                "tx_gestao": "0,70%",
                "liquidez": "D30",
                "pub_alvo": "Qualificado",
            },
            {
                "cnpj": "41033303000198",
                "name": "VIT LONG BIAS",
                "tx_gestao": "N/A",
                "liquidez": "D32",
                "pub_alvo": "Qualificado",
            },
        ],
        "benchmarks": [
            {"name": "IBOVESPA - BENCHMARK", "key": "ibovespa"},
        ],
    },
    {
        "group": "ILÍQUIDOS",
        "funds": [
            {
                "cnpj": "39466597000108",
                "name": "TAG VENTURES",
                "tx_gestao": "0,45%",
                "liquidez": "Fechado",
                "pub_alvo": "Profissional",
            },
            {
                "cnpj": "40899683000185",
                "name": "TAG PRIVATE EQUITY",
                "tx_gestao": "0,45%",
                "liquidez": "Fechado",
                "pub_alvo": "Profissional",
            },
            {
                "cnpj": "33162799000171",
                "name": "TAG DOMO",
                "tx_gestao": "0,45%",
                "liquidez": "Fechado",
                "pub_alvo": "Profissional",
            },
            {
                "cnpj": "34706548000173",
                "name": "TAG TREECORP",
                "tx_gestao": "0,45%",
                "liquidez": "Fechado",
                "pub_alvo": "Profissional",
            },
            {
                "cnpj": "46041024000190",
                "name": "VIT SPECIAL SITS",
                "tx_gestao": "N/A",
                "liquidez": "Fechado",
                "pub_alvo": "Profissional",
            },
        ],
        "benchmarks": [
            {"name": "IPCA +6% - BENCHMARK", "key": "ipca6"},
        ],
    },
    {
        "group": "OFFSHORE",
        "funds": [
            {
                "cnpj": "39254283000133",
                "name": "MULTI ASSETS SOLUTION (COTA ONSHORE)",
                "tx_gestao": "N/A",
                "liquidez": "D9",
                "pub_alvo": "Qualificado",
            },
            {
                "cnpj": "37782470000164",
                "name": "TB HEDGE FUNDS (COTA ONSHORE)",
                "tx_gestao": "N/A",
                "liquidez": "D180",
                "pub_alvo": "Profissional",
            },
        ],
        "benchmarks": [
            {"name": "DÓLAR/BRL - BENCHMARK", "key": "usdbrl"},
        ],
    },
    {
        "group": "PREVIDÊNCIA",
        "funds": [
            {
                "cnpj": "27036278000175",
                "name": "TB PREV - OLD",
                "tx_gestao": "0,75%",
                "liquidez": "D9",
                "pub_alvo": "Profissional",
            },
            {
                "cnpj": "57326791000161",
                "name": "TB PREV - MODERADO",
                "tx_gestao": "0,58%",
                "liquidez": "D9",
                "pub_alvo": "Profissional",
            },
        ],
        "benchmarks": [
            {"name": "CDI ACUMULADO", "key": "cdi"},
        ],
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# COMDINHEIRO API CREDENTIALS  (necessário para IHFA)
# ──────────────────────────────────────────────────────────────────────────────
# Para obter o Bearer Token:
#   1. Acesse https://www.comdinheiro.com.br com login Tag.invest / Tag.invest1!
#   2. Vá em "Minha Conta" → "API" → gere / copie o Bearer Token
#   3. Cole abaixo:
COMDINHEIRO_BEARER_TOKEN = ""   # ex: "eyJhbGciOiJSUzI1NiIsInR5..."

# ──────────────────────────────────────────────────────────────────────────────
# ANBIMA API CREDENTIALS  (alternativa – caso prefira usar ANBIMA diretamente)
# ──────────────────────────────────────────────────────────────────────────────
# Cadastro GRATUITO em: https://developers.anbima.com.br/
ANBIMA_CLIENT_ID     = ""   # ex: "a1b2c3d4e5f6..."
ANBIMA_CLIENT_SECRET = ""   # ex: "AbCdEfGh1234..."

# ──────────────────────────────────────────────────────────────────────────────
# DATA FETCHING FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cvm_monthly(year: int, month: int) -> pd.DataFrame:
    """Download CVM daily fund data for a given month."""
    url = (
        f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
        f"inf_diario_fi_{year}{month:02d}.zip"
    )
    try:
        r = requests.get(url, timeout=120)
        if r.status_code != 200:
            return pd.DataFrame()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        df = pd.read_csv(
            z.open(z.namelist()[0]),
            sep=";",
            encoding="latin1",
            usecols=["CNPJ_FUNDO_CLASSE", "DT_COMPTC", "VL_QUOTA"],
            dtype={"CNPJ_FUNDO_CLASSE": str, "DT_COMPTC": str, "VL_QUOTA": float},
        )
        df["CNPJ_norm"] = df["CNPJ_FUNDO_CLASSE"].str.replace(r"\D", "", regex=True)
        df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_bcb_series(series_code: int, start: str, end: str) -> pd.Series:
    """Fetch a BCB time series and return as daily-indexed Series."""
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
        f"?formato=json&dataInicial={start}&dataFinal={end}"
    )
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return pd.Series(dtype=float)
        data = r.json()
        df = pd.DataFrame(data)
        df["data"] = pd.to_datetime(df["data"], dayfirst=True)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df = df.set_index("data")["valor"]
        return df
    except Exception:
        return pd.Series(dtype=float)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_yf_ticker(symbol: str, start_ts: int, end_ts: int) -> pd.Series:
    """Fetch daily closing prices from Yahoo Finance for any ticker."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?period1={start_ts}&period2={end_ts}&interval=1d"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            return pd.Series(dtype=float)
        data = r.json()
        result = data["chart"]["result"][0]
        ts = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        df = pd.DataFrame({"ts": ts, "close": closes})
        df["date"] = pd.to_datetime(df["ts"], unit="s").dt.normalize()
        df = df.dropna(subset=["close"]).set_index("date")["close"]
        return df.sort_index()
    except Exception:
        return pd.Series(dtype=float)


# ──────────────────────────────────────────────────────────────────────────────
# RETURN CALCULATION HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def nearest_before(series: pd.Series, target_date) -> float:
    """Return the series value at or before target_date."""
    if series.empty:
        return np.nan
    target = pd.Timestamp(target_date)
    subset = series[series.index <= target]
    if subset.empty:
        return np.nan
    return subset.iloc[-1]


def pct_return(v_end, v_start) -> float:
    """Percentage return between two values."""
    if pd.isna(v_end) or pd.isna(v_start) or v_start == 0:
        return np.nan
    return (v_end / v_start - 1) * 100


def compound_monthly_returns(monthly_series: pd.Series, from_date, to_date) -> float:
    """
    Compound monthly percentage returns between two dates.
    monthly_series: indexed by first-of-month dates, values are % returns.
    Returns the compounded % return.
    """
    if monthly_series.empty:
        return np.nan
    from_d = pd.Timestamp(from_date)
    to_d = pd.Timestamp(to_date)
    # Select months after from_date up to to_date
    subset = monthly_series[
        (monthly_series.index > from_d) & (monthly_series.index <= to_d)
    ]
    if subset.empty:
        return np.nan
    factor = np.prod(1 + subset.values / 100)
    return (factor - 1) * 100


def compute_fund_returns(quota_series: pd.Series, today: date) -> dict:
    """Compute D, M, ANO, 1ANO, 2ANOS returns for a quota series."""
    if quota_series.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    # Last available date
    last_ts = quota_series.index.max()
    last_val = quota_series.iloc[-1]

    # Day return: last vs previous observation
    prev_obs = quota_series[quota_series.index < last_ts]
    if prev_obs.empty:
        d_ret = np.nan
    else:
        d_ret = pct_return(last_val, prev_obs.iloc[-1])

    # Month return: last vs end of previous month
    first_of_month = last_ts.replace(day=1)
    prev_month_end = first_of_month - timedelta(days=1)
    v_prev_month = nearest_before(quota_series, prev_month_end)
    m_ret = pct_return(last_val, v_prev_month)

    # Year to date: last vs end of previous year
    prev_year_end = date(last_ts.year - 1, 12, 31)
    v_prev_year_end = nearest_before(quota_series, prev_year_end)
    ano_ret = pct_return(last_val, v_prev_year_end)

    # 1 year: last vs same day 1 year ago
    one_year_ago = last_ts - pd.DateOffset(years=1)
    v_1y = nearest_before(quota_series, one_year_ago)
    y1_ret = pct_return(last_val, v_1y)

    # 2 years: last vs same day 2 years ago
    two_years_ago = last_ts - pd.DateOffset(years=2)
    v_2y = nearest_before(quota_series, two_years_ago)
    y2_ret = pct_return(last_val, v_2y)

    return {
        "D": d_ret,
        "M": m_ret,
        "ANO": ano_ret,
        "1ANO": y1_ret,
        "2ANOS": y2_ret,
        "ultima_cota": last_ts.date(),
    }


def compute_cdi_returns(cdi_daily: pd.Series, ref_date: date) -> dict:
    """CDI acumulado até ref_date (alinhado com última cota dos fundos)."""
    if cdi_daily.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    ref_ts = pd.Timestamp(ref_date)
    s = cdi_daily[cdi_daily.index <= ref_ts]
    if s.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    last_ts = s.index.max()

    def cum(from_d, to_d):
        subset = s[
            (s.index > pd.Timestamp(from_d)) & (s.index <= pd.Timestamp(to_d))
        ]
        if subset.empty:
            return np.nan
        return (np.prod(1 + subset.values / 100) - 1) * 100

    d_ret  = float(s.iloc[-1])
    prev_m = last_ts.replace(day=1) - timedelta(days=1)
    m_ret  = cum(prev_m, last_ts)
    ano_ret = cum(date(last_ts.year - 1, 12, 31), last_ts)
    y1_ret  = cum(last_ts - pd.DateOffset(years=1), last_ts)
    y2_ret  = cum(last_ts - pd.DateOffset(years=2), last_ts)

    return {"D": d_ret, "M": m_ret, "ANO": ano_ret,
            "1ANO": y1_ret, "2ANOS": y2_ret, "ultima_cota": last_ts.date()}


def compute_price_returns(price_series: pd.Series, ref_date: date) -> dict:
    """
    Retornos a partir de série de preços diários (IBOVESPA, ETFs IMA-B, etc.)
    cortada em ref_date para alinhar com a última cota dos fundos.
    """
    if price_series.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    ref_ts = pd.Timestamp(ref_date)
    s = price_series[price_series.index <= ref_ts]
    if s.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    last_ts  = s.index.max()
    last_val = float(s.iloc[-1])

    prev_obs = s[s.index < last_ts]
    d_ret = pct_return(last_val, prev_obs.iloc[-1]) if not prev_obs.empty else np.nan

    prev_m = last_ts.replace(day=1) - timedelta(days=1)
    m_ret   = pct_return(last_val, nearest_before(s, prev_m))
    ano_ret = pct_return(last_val, nearest_before(s, date(last_ts.year - 1, 12, 31)))
    y1_ret  = pct_return(last_val, nearest_before(s, last_ts - pd.DateOffset(years=1)))
    y2_ret  = pct_return(last_val, nearest_before(s, last_ts - pd.DateOffset(years=2)))

    return {"D": d_ret, "M": m_ret, "ANO": ano_ret,
            "1ANO": y1_ret, "2ANOS": y2_ret, "ultima_cota": last_ts.date()}


def compute_ipca6_returns(ipca_monthly: pd.Series, ref_date: date) -> dict:
    """
    IPCA+6% a.a. alinhado com ref_date.

    Convenção (igual ao mercado):
    - O IPCA de cada mês é 'ganho' durante aquele mês.
    - Para o mês corrente ainda não publicado, usa o último IPCA como proxy.
    - Período inclui todos os meses entre o baseline e o mês de ref_date.
    """
    if ipca_monthly.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    premium_m = ((1 + 0.06) ** (1 / 12) - 1) * 100  # ~0.4868% ao mês

    # Estende série: se mês corrente não publicado, adiciona proxy
    ref_ts = pd.Timestamp(ref_date)
    cur_month_start = ref_ts.replace(day=1)
    ext = ipca_monthly.copy()
    if ext.index.max() < cur_month_start:
        ext[cur_month_start] = float(ext.iloc[-1])  # proxy = último IPCA disponível
    ext = ext.sort_index()

    combined = ext + premium_m  # série mensal (IPCA + prêmio)

    def cum(from_d, to_d):
        """
        Inclui todos os meses cuja data-início >= primeiro mês APÓS from_d
        E <= primeiro mês DE to_d.
        """
        from_next = (pd.Timestamp(from_d).replace(day=1) + pd.DateOffset(months=1))
        to_month  = pd.Timestamp(to_d).replace(day=1)
        subset = combined[(combined.index >= from_next) & (combined.index <= to_month)]
        if subset.empty:
            return np.nan
        return (np.prod(1 + subset.values / 100) - 1) * 100

    d_ret   = float(combined.iloc[-1]) / 22          # aproximação diária
    prev_m  = cur_month_start - timedelta(days=1)
    m_ret   = cum(prev_m, ref_ts)
    ano_ret = cum(date(ref_date.year - 1, 12, 31), ref_ts)
    y1_ret  = cum(ref_ts - pd.DateOffset(years=1), ref_ts)
    y2_ret  = cum(ref_ts - pd.DateOffset(years=2), ref_ts)

    return {"D": d_ret, "M": m_ret, "ANO": ano_ret,
            "1ANO": y1_ret, "2ANOS": y2_ret, "ultima_cota": ref_date}


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_sofr_series() -> pd.Series:
    """SOFR diário do FRED (taxa % ao dia)."""
    try:
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SOFR"
        r = requests.get(url, timeout=15)
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = ["date", "rate"]
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["rate"] != "."].copy()
        df["rate"] = pd.to_numeric(df["rate"]) / 252  # anual → diário
        return df.set_index("date")["rate"].sort_index()
    except Exception:
        return pd.Series(dtype=float)


@st.cache_data(ttl=3500, show_spinner=False)
def fetch_anbima_token() -> str:
    """Obtém token OAuth2 da API ANBIMA. Retorna '' se credenciais não configuradas."""
    if not ANBIMA_CLIENT_ID or not ANBIMA_CLIENT_SECRET:
        return ""
    try:
        r = requests.post(
            "https://api.anbima.com.br/oauth/access-token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": ANBIMA_CLIENT_ID,
                "client_secret": ANBIMA_CLIENT_SECRET,
            },
            timeout=15,
        )
        if r.status_code == 200:
            return r.json().get("access_token", "")
        return ""
    except Exception:
        return ""


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ihfa_comdinheiro() -> pd.Series:
    """
    IHFA via API ComDinheiro (EndPoint002, Bearer Token).
    Retorna série de níveis do índice.
    Retorna pd.Series vazio se COMDINHEIRO_BEARER_TOKEN não configurado.
    """
    if not COMDINHEIRO_BEARER_TOKEN:
        return pd.Series(dtype=float)

    today = date.today()
    start = f"01/01/{today.year - 2}"
    end   = today.strftime("%d/%m/%Y")

    try:
        r = requests.post(
            "https://www.comdinheiro.com.br/Clientes/API/EndPoint002.php",
            params={"code": COMDINHEIRO_BEARER_TOKEN},
            data={
                "p":           "HistoricoIndicadores",
                "Indicadores": "IHFA",
                "dt_ini":      start,
                "dt_fim":      end,
                "periodicidade": "D",
                "format":      "json3",
            },
            headers={
                "Authorization": f"Bearer {COMDINHEIRO_BEARER_TOKEN}",
                "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            },
            timeout=30,
        )
        if r.status_code != 200:
            return pd.Series(dtype=float)
        data = r.json()
        # ComDinheiro json3: {"data": {"IHFA": {"dates": [...], "values": [...]}}}
        # ou lista plana de {data, valor}
        if isinstance(data, list):
            df = pd.DataFrame(data)
            if "data" in df.columns and "valor" in df.columns:
                df["data"] = pd.to_datetime(df["data"], dayfirst=True)
                df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
                return df.dropna().set_index("data")["valor"].sort_index()
        # Tenta estrutura aninhada
        inner = (data.get("data") or data.get("dados") or data.get("result") or {})
        if isinstance(inner, dict):
            ihfa_data = inner.get("IHFA", {})
            dates  = ihfa_data.get("dates",  ihfa_data.get("data",   []))
            values = ihfa_data.get("values", ihfa_data.get("valores", []))
            if dates and values:
                idx = pd.to_datetime(dates, dayfirst=True)
                s = pd.Series(pd.to_numeric(values, errors="coerce"), index=idx)
                return s.dropna().sort_index()
        return pd.Series(dtype=float)
    except Exception:
        return pd.Series(dtype=float)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ihfa_anbima() -> pd.Series:
    """
    IHFA via API ANBIMA (fallback se ComDinheiro não configurado).
    Retorna pd.Series vazio se credenciais ANBIMA não configuradas.
    """
    token = fetch_anbima_token()
    if not token:
        return pd.Series(dtype=float)

    today = date.today()
    start = f"01/01/{today.year - 2}"
    end   = today.strftime("%d/%m/%Y")
    url   = (
        f"https://api.anbima.com.br/feed/precos-indices/v1/indices/"
        f"historico-por-indices?indice=IHFA&dataInicio={start}&dataFim={end}"
    )
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=30,
        )
        if r.status_code != 200:
            return pd.Series(dtype=float)
        data = r.json()
        records = data if isinstance(data, list) else data.get("dados", data.get("result", []))
        df = pd.DataFrame(records)
        if df.empty or "numIndice" not in df.columns:
            return pd.Series(dtype=float)
        date_col = "data" if "data" in df.columns else df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)
        df["numIndice"] = pd.to_numeric(df["numIndice"], errors="coerce")
        s = df.dropna(subset=["numIndice"]).set_index(date_col)["numIndice"]
        return s.sort_index()
    except Exception:
        return pd.Series(dtype=float)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ihfa_series() -> pd.Series:
    """
    IHFA – tenta ComDinheiro primeiro, depois ANBIMA como fallback.
    Retorna pd.Series vazio se nenhuma credencial configurada.
    """
    s = fetch_ihfa_comdinheiro()
    if not s.empty:
        return s
    return fetch_ihfa_anbima()


def compute_sofr_returns(sofr_daily: pd.Series, ref_date: date) -> dict:
    """SOFR acumulado até ref_date."""
    if sofr_daily.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    ref_ts = pd.Timestamp(ref_date)
    s = sofr_daily[sofr_daily.index <= ref_ts]
    if s.empty:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

    last_ts = s.index.max()

    def cum(from_d, to_d):
        subset = s[
            (s.index > pd.Timestamp(from_d)) & (s.index <= pd.Timestamp(to_d))
        ]
        if subset.empty:
            return np.nan
        return (np.prod(1 + subset.values / 100) - 1) * 100

    d_ret  = float(s.iloc[-1])
    prev_m = last_ts.replace(day=1) - timedelta(days=1)
    m_ret  = cum(prev_m, last_ts)
    ano_ret = cum(date(last_ts.year - 1, 12, 31), last_ts)
    y1_ret  = cum(last_ts - pd.DateOffset(years=1), last_ts)
    y2_ret  = cum(last_ts - pd.DateOffset(years=2), last_ts)

    return {"D": d_ret, "M": m_ret, "ANO": ano_ret,
            "1ANO": y1_ret, "2ANOS": y2_ret, "ultima_cota": last_ts.date()}


# ──────────────────────────────────────────────────────────────────────────────
# MAIN DATA LOADING
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_all_data():
    today = date.today()

    # Build list of (year, month) combos to fetch for CVM.
    # We need:
    #   - Current and previous month  → for D and M returns
    #   - Month before previous       → to get end-of-prev-month quota
    #   - Dec of prev year            → for YTD (ANO)
    #   - Same month 1 year ago ±1    → for 1ANO
    #   - Dec of 2 years ago          → anchor for 2ANO
    #   - Same month 2 years ago ±1   → for 2ANOS
    months_to_fetch = set()

    # Rolling 4-month window for M return (current month + 3 previous)
    # Use the first-of-month anchor and subtract whole months
    for delta_m in range(4):
        # Go back delta_m full months from the current month
        y = today.year
        m = today.month - delta_m
        while m < 1:
            m += 12; y -= 1
        months_to_fetch.add((y, m))

    # 1-year window (±1 month)
    for delta_m in [-1, 0, 1]:
        ref = (today - timedelta(days=365)).replace(day=1)
        m = ref.month + delta_m
        y = ref.year
        if m < 1:
            m += 12; y -= 1
        elif m > 12:
            m -= 12; y += 1
        months_to_fetch.add((y, m))

    # 2-year window (±1 month)
    for delta_m in [-1, 0, 1]:
        ref = (today - timedelta(days=730)).replace(day=1)
        m = ref.month + delta_m
        y = ref.year
        if m < 1:
            m += 12; y -= 1
        elif m > 12:
            m -= 12; y += 1
        months_to_fetch.add((y, m))

    # Always include Dec of previous two years for YTD anchors
    months_to_fetch.add((today.year - 1, 12))
    months_to_fetch.add((today.year - 2, 12))

    # Sort and fetch
    all_dfs = []
    for yr, mo in sorted(months_to_fetch):
        if yr < 2020 or (yr > today.year) or (yr == today.year and mo > today.month):
            continue
        df = fetch_cvm_monthly(yr, mo)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        cvm_df = pd.concat(all_dfs, ignore_index=True)
        cvm_df = cvm_df.sort_values(["CNPJ_norm", "DT_COMPTC"]).drop_duplicates()
    else:
        cvm_df = pd.DataFrame()

    # Fetch benchmarks
    start_str = f"01/01/{today.year - 2}"
    end_str = today.strftime("%d/%m/%Y")

    cdi_daily    = fetch_bcb_series(12,  start_str, end_str)  # CDI diário %
    ipca_monthly = fetch_bcb_series(433, start_str, end_str)  # IPCA mensal %

    # IMA-B via ETFs B3 (dados diários, mais precisos que série mensal BCB)
    # IMAB11.SA = IMA-B (todos vencimentos)
    # B5P211.SA = IMA-B5 (até 5 anos)
    # IB5M11.SA = IMA-B5+ (acima de 5 anos)
    start_ts = int(datetime(today.year - 2, 1, 1).timestamp())
    end_ts   = int(datetime.now().timestamp())
    ibov_daily       = fetch_yf_ticker("%5EBVSP",   start_ts, end_ts)
    imab_prices      = fetch_yf_ticker("IMAB11.SA", start_ts, end_ts)
    imab5_prices     = fetch_yf_ticker("B5P211.SA", start_ts, end_ts)
    imab5plus_prices = fetch_yf_ticker("IB5M11.SA", start_ts, end_ts)
    usdbrl_prices    = fetch_bcb_series(1, start_str, end_str)           # USD/BRL PTAX compra oficial BCB
    sofr_daily       = fetch_sofr_series()
    ihfa_series      = fetch_ihfa_series()   # vazio se sem credenciais configuradas

    return (cvm_df, cdi_daily, imab_prices, imab5_prices,
            imab5plus_prices, ipca_monthly, ibov_daily,
            usdbrl_prices, sofr_daily, ihfa_series)


# ──────────────────────────────────────────────────────────────────────────────
# RETURN DISPATCHER FOR BENCHMARKS
# ──────────────────────────────────────────────────────────────────────────────

def get_benchmark_returns(key: str, ref_date: date,
                          cdi_daily, imab_prices, imab5_prices, imab5plus_prices,
                          ipca_monthly, ibov_daily, usdbrl_prices,
                          sofr_daily, ihfa_series) -> dict:
    """Dispatcher: retorna dict de retornos para o benchmark indicado por key,
    todos cortados em ref_date (= última data de cota comum dos fundos)."""
    if key == "cdi":
        return compute_cdi_returns(cdi_daily, ref_date)
    elif key == "imab":
        return compute_price_returns(imab_prices, ref_date)
    elif key == "imab5":
        return compute_price_returns(imab5_prices, ref_date)
    elif key == "imab5plus":
        return compute_price_returns(imab5plus_prices, ref_date)
    elif key == "ibovespa":
        return compute_price_returns(ibov_daily, ref_date)
    elif key == "usdbrl":
        return compute_price_returns(usdbrl_prices, ref_date)
    elif key == "ipca6":
        return compute_ipca6_returns(ipca_monthly, ref_date)
    elif key == "sofr":
        return compute_sofr_returns(sofr_daily, ref_date)
    elif key == "ihfa":
        return compute_price_returns(ihfa_series, ref_date)
    else:
        return {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}


# ──────────────────────────────────────────────────────────────────────────────
# HTML TABLE RENDERING
# ──────────────────────────────────────────────────────────────────────────────

COLOR_HEADER = "#7B2D40"        # Dark maroon
COLOR_BG_HEADER_TEXT = "white"
COLOR_POS = "#C8E6C9"           # Light green
COLOR_NEG = "#FFCDD2"           # Light red
COLOR_NEUTRAL = "#FFFFFF"
COLOR_BMARK = "#F5F5F5"         # Light gray for benchmark rows
COLOR_FUND = "#FFFFFF"          # White for fund rows
COLOR_GROUP_TEXT = "white"
COLOR_SECTION = "#7B2D40"


def fmt_pct(v, decimals=2) -> str:
    if pd.isna(v):
        return "-"
    return f"{v:.{decimals}f}%"


def cell_color(v) -> str:
    if pd.isna(v):
        return COLOR_NEUTRAL
    return COLOR_POS if v >= 0 else COLOR_NEG


def build_html_table(rows: list) -> str:
    """Build styled HTML table from list of row dicts."""
    th_style = (
        f"background:{COLOR_HEADER}; color:{COLOR_BG_HEADER_TEXT}; "
        "padding:6px 10px; text-align:center; font-size:11px; "
        "border:1px solid #5a1e2e; white-space:nowrap;"
    )
    th_name_style = (
        f"background:{COLOR_HEADER}; color:{COLOR_BG_HEADER_TEXT}; "
        "padding:6px 10px; text-align:left; font-size:11px; "
        "border:1px solid #5a1e2e; white-space:nowrap;"
    )

    html = f"""
    <style>
        .tag-table {{ border-collapse: collapse; width: 100%; font-family: 'Segoe UI', Arial, sans-serif; font-size:12px; }}
        .tag-table td {{ padding: 5px 10px; border: 1px solid #ddd; white-space: nowrap; }}
        .section-row td {{ background: {COLOR_HEADER}; color: white; font-weight: bold;
                          font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
                          padding: 4px 10px; border: 1px solid #5a1e2e; }}
        .fund-row td {{ background: #FFFFFF; }}
        .bmark-row td {{ background: {COLOR_BMARK}; color: #555; font-style: italic; }}
        .num-cell {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .name-cell {{ text-align: left; font-weight: 500; }}
        .bmark-name-cell {{ text-align: left; color: #555; font-style: italic; padding-left: 20px !important; }}
    </style>
    <table class="tag-table">
    <thead>
        <tr>
            <th style="{th_name_style}; min-width:220px;">FUNDO</th>
            <th style="{th_style}">Tx Gestão</th>
            <th style="{th_style}">D</th>
            <th style="{th_style}">M</th>
            <th style="{th_style}">ANO</th>
            <th style="{th_style}">1 ANO</th>
            <th style="{th_style}">2 ANOS</th>
            <th style="{th_style}">DATA ÚLTIMA COTA</th>
            <th style="{th_style}">LIQUIDEZ</th>
            <th style="{th_style}">PUB. ALVO</th>
        </tr>
    </thead>
    <tbody>
    """

    for row in rows:
        rtype = row.get("type", "fund")

        if rtype == "section":
            html += f"""
            <tr class="section-row">
                <td colspan="10">{row['name']}</td>
            </tr>"""

        elif rtype == "benchmark":
            ret = row.get("returns", {})
            d_col = fmt_pct(ret.get("D"))
            m_col = fmt_pct(ret.get("M"))
            ano_col = fmt_pct(ret.get("ANO"))
            y1_col = fmt_pct(ret.get("1ANO"))
            y2_col = fmt_pct(ret.get("2ANOS"))
            uc = ret.get("ultima_cota")
            uc_str = uc.strftime("%d/%m/%Y") if uc and not pd.isna(uc) else "-"

            def bm_num_cell(v, raw):
                color = cell_color(raw)
                return (
                    f'<td class="num-cell" style="background:{color}; '
                    f'text-align:right;">{v}</td>'
                )

            html += f"""
            <tr class="bmark-row">
                <td class="bmark-name-cell">{row['name']}</td>
                <td class="num-cell">-</td>
                {bm_num_cell(d_col, ret.get('D'))}
                {bm_num_cell(m_col, ret.get('M'))}
                {bm_num_cell(ano_col, ret.get('ANO'))}
                {bm_num_cell(y1_col, ret.get('1ANO'))}
                {bm_num_cell(y2_col, ret.get('2ANOS'))}
                <td class="num-cell">{uc_str}</td>
                <td class="num-cell">-</td>
                <td class="num-cell">-</td>
            </tr>"""

        else:  # fund row
            ret = row.get("returns", {})
            d_col = fmt_pct(ret.get("D"))
            m_col = fmt_pct(ret.get("M"))
            ano_col = fmt_pct(ret.get("ANO"))
            y1_col = fmt_pct(ret.get("1ANO"))
            y2_col = fmt_pct(ret.get("2ANOS"))
            uc = ret.get("ultima_cota")
            uc_str = uc.strftime("%d/%m/%Y") if uc and not pd.isna(uc) else "-"
            tx = row.get("tx_gestao", "-")
            liq = row.get("liquidez", "-")
            pub = row.get("pub_alvo", "-")

            def fund_num_cell(v, raw):
                color = cell_color(raw)
                return (
                    f'<td class="num-cell" style="background:{color}; '
                    f'text-align:right;">{v}</td>'
                )

            html += f"""
            <tr class="fund-row">
                <td class="name-cell">{row['name']}</td>
                <td class="num-cell">{tx}</td>
                {fund_num_cell(d_col, ret.get('D'))}
                {fund_num_cell(m_col, ret.get('M'))}
                {fund_num_cell(ano_col, ret.get('ANO'))}
                {fund_num_cell(y1_col, ret.get('1ANO'))}
                {fund_num_cell(y2_col, ret.get('2ANOS'))}
                <td class="num-cell">{uc_str}</td>
                <td class="num-cell">{liq}</td>
                <td class="num-cell">{pub}</td>
            </tr>"""

    html += "</tbody></table>"
    return html


# ──────────────────────────────────────────────────────────────────────────────
# MAIN APP
# ──────────────────────────────────────────────────────────────────────────────

def main():
    # Header
    st.markdown(
        """
        <div style="background:#7B2D40; padding:18px 24px; border-radius:6px; margin-bottom:16px;">
            <h2 style="color:white; margin:0; font-size:22px; font-family:'Segoe UI',Arial,sans-serif;">
                TAG Investimentos
            </h2>
            <p style="color:#f0c0cc; margin:4px 0 0 0; font-size:14px; font-family:'Segoe UI',Arial,sans-serif;">
                Monitor de Fundos Condominiais TAG
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Controls
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.markdown(
            f"<span style='color:#888; font-size:12px;'>Atualizado: "
            f"{datetime.now().strftime('%d/%m/%Y %H:%M')}</span>",
            unsafe_allow_html=True,
        )
    with col3:
        if st.button("🔄 Atualizar dados", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # Load data
    with st.spinner("Buscando dados da CVM e benchmarks..."):
        (
            cvm_df,
            cdi_daily,
            imab_prices,
            imab5_prices,
            imab5plus_prices,
            ipca_monthly,
            ibov_daily,
            usdbrl_prices,
            sofr_daily,
            ihfa_series,
        ) = load_all_data()

    today = date.today()

    # Build quota series per CNPJ
    quota_map: dict[str, pd.Series] = {}
    if not cvm_df.empty:
        for cnpj, grp in cvm_df.groupby("CNPJ_norm"):
            s = grp.set_index("DT_COMPTC")["VL_QUOTA"].sort_index()
            quota_map[cnpj] = s

    # ── ref_date: moda da última data de cota entre os fundos ativos ──────────
    # Benchmarks serão calculados até esta data para evitar discrepâncias.
    all_cnpjs = [f["cnpj"] for g in FUND_GROUPS for f in g["funds"]]
    last_dates = []
    for cnpj in all_cnpjs:
        if cnpj in quota_map and not quota_map[cnpj].empty:
            last_dates.append(quota_map[cnpj].index.max().date())

    if last_dates:
        from collections import Counter
        ref_date: date = Counter(last_dates).most_common(1)[0][0]
    else:
        ref_date = today

    # Build table rows
    table_rows = []

    for group in FUND_GROUPS:
        # Section header
        table_rows.append({"type": "section", "name": group["group"]})

        # Fund rows
        for fund in group["funds"]:
            cnpj = fund["cnpj"]
            max_stale = fund.get("max_stale_days", 45)
            if cnpj in quota_map:
                returns = compute_fund_returns(quota_map[cnpj], today)
                # Mark stale data as N/D for period returns
                uc = returns.get("ultima_cota")
                if uc and (today - uc).days > max_stale:
                    returns = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
            else:
                returns = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}

            table_rows.append(
                {
                    "type": "fund",
                    "name": fund["name"],
                    "tx_gestao": fund["tx_gestao"],
                    "liquidez": fund["liquidez"],
                    "pub_alvo": fund["pub_alvo"],
                    "returns": returns,
                }
            )

        # Benchmark rows
        for bm in group["benchmarks"]:
            bm_returns = get_benchmark_returns(
                bm["key"],
                ref_date,
                cdi_daily,
                imab_prices,
                imab5_prices,
                imab5plus_prices,
                ipca_monthly,
                ibov_daily,
                usdbrl_prices,
                sofr_daily,
                ihfa_series,
            )
            table_rows.append(
                {
                    "type": "benchmark",
                    "name": bm["name"],
                    "returns": bm_returns,
                }
            )

    # Render — usa components.html para garantir renderização correta de tabelas HTML
    import streamlit.components.v1 as components
    table_html = build_html_table(table_rows)
    if not ihfa_series.empty:
        ihfa_src = "ComDinheiro" if COMDINHEIRO_BEARER_TOKEN else "ANBIMA"
        ihfa_note = f" | IHFA: {ihfa_src}"
    else:
        ihfa_note = " | IHFA: N/D (configure COMDINHEIRO_BEARER_TOKEN no código)"
    footer = (
        "<div style='margin-top:16px; color:#aaa; font-size:11px; text-align:center;'>"
        "Fonte: CVM (cotas diárias), BCB SGS (CDI/IPCA/PTAX), Yahoo Finance (IBOVESPA/ETFs IMA-B), FRED (SOFR)"
        f"{ihfa_note}.<br>"
        "Retornos são brutos de IR e taxas."
        "</div>"
    )
    full_html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
  body {{ margin:0; padding:0; font-family:'Segoe UI',Arial,sans-serif; background:transparent; }}
</style>
</head>
<body>
{table_html}
{footer}
</body>
</html>"""
    components.html(full_html, height=1700, scrolling=True)


if __name__ == "__main__":
    main()
