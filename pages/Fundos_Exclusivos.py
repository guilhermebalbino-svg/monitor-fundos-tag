"""
Fundos_Exclusivos.py — Monitor de Fundos Exclusivos TAG
Página Streamlit independente (multi-page app).
"""
import io
import json
import re
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fundos Exclusivos — TAG",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Fund groups config ────────────────────────────────────────────────────────
FUND_GROUPS_EXCL = [
    {
        "group": "MULTIMERCADO",
        "benchmarks": [{"key": "cdi", "label": "CDI ACUMULADO"}],
        "funds": [
            {"cnpj": "08621422000196", "name": "ALFER FIF MULTIMERCADO"},
            {"cnpj": "18611538000106", "name": "ALRD FIF MULTIMERCADO"},
            {"cnpj": "47512461000107", "name": "FUTURO II FIF CIC MULTIMERCADO"},
            {"cnpj": "13591889000170", "name": "GENESIS PLUS RESP LIMITADA FIF MULTIMERCADO CRÉDITO PRIVADO"},
            {"cnpj": "17400234000129", "name": "GUVIDALA FIF MULTIMERCADO"},
            {"cnpj": "17335641000108", "name": "HL TB FIF MULTIMERCADO"},
            {"cnpj": "63696630000162", "name": "JFT FIF MULTIMERCADO"},
            {"cnpj": "64018596000130", "name": "JPA AGRO FIF (FIM 95)"},
            {"cnpj": "11409553000119", "name": "JUBA FIF MULTIMERCADO"},
            {"cnpj": "65223808000183", "name": "JUBA III FIF MULTIMERCADO (FIM 95)"},
            {"cnpj": "37287572000103", "name": "LA PLATA FIF CIC MULTIMERCADO"},
            {"cnpj": "57210425000142", "name": "LAGERTHA FIF CIC MULTIMERCADO (FIM 95)"},
            {"cnpj": "17413818000139", "name": "LAURUS FIF MULTIMERCADO"},
            {"cnpj": "21015772000177", "name": "LUSS FIF MULTIMERCADO"},
            {"cnpj": "17413812000161", "name": "MAGNÓLIAS FIF MULTIMERCADO (FIM 95)"},
            {"cnpj": "17425221000104", "name": "MAHALÁKSHMI FI MULTIMERCADO"},
            {"cnpj": "46420207000116", "name": "MARAU FI MULTIMERCADO"},
            {"cnpj": "53100651000110", "name": "MARAU II FIF MULTIMERCADO (FIM 95)"},
            {"cnpj": "53026176000189", "name": "MARIA SILVIA FI FINANCEIRO INVEST NO EXTERIOR RESP LIMITADA MULTIMERCADO CRÉDITO PRIVADO"},
            {"cnpj": "26342026000101", "name": "MEMÓRIAS FIF MULTIMERCADO"},
            {"cnpj": "10841486000144", "name": "MYBS FIF CIC MULTIMERCADO"},
            {"cnpj": "45560872000142", "name": "OCEANUS FIF MULTIMERCADO (FIM 95)"},
            {"cnpj": "66763983000126", "name": "PARANAÍBA RESP LIMITADA FIF MULTIMERCADO CRÉDITO PRIVADO (FIM 95)"},
            {"cnpj": "54912091000160", "name": "POTENTIA I FI MULTIMERCADO"},
            {"cnpj": "26768797000165", "name": "RAGNAR FIF MULTIMERCADO (FIM 95)"},
            {"cnpj": "65919962000194", "name": "RANATORI II FI MULTIMERCADO (FIM 95)"},
            {"cnpj": "09009733000161", "name": "RANATORI RESP LIMITADA FI MULTIMERCADO CRÉDITO PRIVADO"},
            {"cnpj": "08807608000134", "name": "RINTER FIF MULTIMERCADO"},
            {"cnpj": "53077066000146", "name": "RINTER II FI MULTIMERCADO (FIM 95)"},
            {"cnpj": "51389373000137", "name": "SB STONES FIF MULTIMERCADO"},
            {"cnpj": "39432540000180", "name": "SCUBI RESP LIMITADA FIF MULTIMERCADO CRÉDITO PRIVADO"},
            {"cnpj": "11827429000173", "name": "SOLARIUM FIF MULTIMERCADO"},
        ],
    },
    {
        "group": "PREVIDÊNCIA",
        "benchmarks": [{"key": "cdi", "label": "CDI ACUMULADO"}],
        "funds": [
            {"cnpj": "42682436000158", "name": "1358 PREVIDÊNCIA RESP LIMITADA FI MULTIMERCADO CRÉDITO PRIVADO"},
            {"cnpj": "30520937000159", "name": "BELLA ICATU RESP LIMITADA FIF MULTIMERCADO PREVIDENCIÁRIO"},
            {"cnpj": "44672883000151", "name": "VILA NOVA PREVIDÊNCIA FIF MULTIMERCADO"},
        ],
    },
    {
        "group": "RENDA FIXA",
        "benchmarks": [
            {"key": "cdi",        "label": "CDI ACUMULADO"},
            {"key": "ima_b",      "label": "IMA-B"},
            {"key": "ima_b5",     "label": "IMA-B 5"},
            {"key": "ima_b5plus", "label": "IMA-B 5+"},
        ],
        "funds": [
            {"cnpj": "58891176000160", "name": "TAG PRAIA VERMELHA II RESP LIMITADA FIF FI INFRA RENDA FIXA"},
            {"cnpj": "66175865000105", "name": "TREK 2 FIF FI INFRA RENDA FIXA"},
            {"cnpj": "63730018000169", "name": "JFT STRATEGY FIF CIC FI INFRA RENDA FIXA"},
            {"cnpj": "19941775000190", "name": "ALM RESP LIMITADA FIF RENDA FIXA"},
        ],
    },
    {
        "group": "AÇÕES",
        "benchmarks": [{"key": "ibovespa", "label": "IBOVESPA"}],
        "funds": [
            {"cnpj": "18307768000178", "name": "DUNAJUKO FI AÇÕES"},
            {"cnpj": "44230038000126", "name": "JUBA II FIF AÇÕES"},
            {"cnpj": "19418925000185", "name": "MARIA SILVIA INVESTIMENTO NO EXTERIOR RESP LIMITADA FIF AÇÕES"},
            {"cnpj": "35002734000194", "name": "PROFITABLE GROWTH FI AÇÕES"},
            {"cnpj": "13549299000180", "name": "SOLIS FI AÇÕES"},
        ],
    },
]

_CNPJS_EXCL = frozenset(
    f["cnpj"] for g in FUND_GROUPS_EXCL for f in g["funds"]
)

# ── Colors (idênticas ao monitor principal) ───────────────────────────────────
COLOR_ORANGE    = "#E8801A"
COLOR_FUND_BG   = "#1a0d0d"
COLOR_BMARK_BG  = "#120808"
COLOR_POS_BG    = "#163316"
COLOR_POS_TEXT  = "#8fd68f"
COLOR_NEG_BG    = "#331616"
COLOR_NEG_TEXT  = "#d68f8f"
COLOR_EMPTY_BG  = "#1a1212"
COLOR_EMPTY_TEXT = "#554444"
COLOR_META_TEXT = "#b09090"
COLOR_DATE_TEXT = "#c8b8b8"

# ── CVM loading ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_cvm_excl(year: int, month: int) -> pd.DataFrame:
    url = (
        f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
        f"inf_diario_fi_{year}{month:02d}.zip"
    )
    try:
        r = requests.get(url, timeout=120)
        if r.status_code != 200:
            return pd.DataFrame()
        raw = io.BytesIO(r.content)
        del r
        z = zipfile.ZipFile(raw)
        df = pd.read_csv(
            z.open(z.namelist()[0]),
            sep=";",
            encoding="latin1",
            usecols=["CNPJ_FUNDO_CLASSE", "DT_COMPTC", "VL_QUOTA", "VL_PATRIM_LIQ"],
            dtype={"CNPJ_FUNDO_CLASSE": str, "DT_COMPTC": str,
                   "VL_QUOTA": float, "VL_PATRIM_LIQ": float},
        )
        z.close()
        df["CNPJ_norm"] = df["CNPJ_FUNDO_CLASSE"].str.replace(r"\D", "", regex=True)
        df = df[df["CNPJ_norm"].isin(_CNPJS_EXCL)].copy()
        df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_bcb_excl(series_code: int, start: str, end: str) -> pd.Series:
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
        f"?formato=json&dataInicial={start}&dataFinal={end}"
    )
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return pd.Series(dtype=float)
        df = pd.DataFrame(r.json())
        df["data"] = pd.to_datetime(df["data"], dayfirst=True)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        return df.set_index("data")["valor"]
    except Exception:
        return pd.Series(dtype=float)


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_yf_excl(symbol: str, start_ts: int, end_ts: int) -> pd.Series:
    qs = f"?period1={start_ts}&period2={end_ts}&interval=1d"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    for host in ("query1", "query2"):
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}{qs}"
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code != 200:
                continue
            data   = r.json()
            result = data["chart"]["result"][0]
            ts     = result["timestamp"]
            adj    = result["indicators"].get("adjclose")
            closes = (
                adj[0]["adjclose"]
                if adj and adj[0].get("adjclose")
                else result["indicators"]["quote"][0]["close"]
            )
            df = pd.DataFrame({"ts": ts, "close": closes})
            df["date"] = pd.to_datetime(df["ts"], unit="s").dt.normalize()
            return df.dropna(subset=["close"]).set_index("date")["close"].sort_index()
        except Exception:
            continue
    return pd.Series(dtype=float)


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_maisretorno_excl(slug: str) -> dict:
    try:
        r = requests.get(
            f"https://maisretorno.com/indice/{slug}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=15,
        )
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            r.text, re.DOTALL,
        )
        if not m:
            return {}
        data   = json.loads(m.group(1))
        stats  = data["props"]["pageProps"]["stats"]["stats"]
        tf     = stats["timeframe"]
        lqd_ms = stats.get("last_quote_date", 0)
        if lqd_ms:
            lqd = datetime.utcfromtimestamp(lqd_ms / 1000).date()
            while lqd.weekday() >= 5:
                lqd -= timedelta(days=1)
        else:
            lqd = np.nan
        return {
            "D":           np.nan,
            "M":           tf["mtd"]["profitability"],
            "ANO":         tf["ytd"]["profitability"],
            "1ANO":        tf["last_12_months"]["profitability"],
            "2ANOS":       tf["last_24_months"]["profitability"],
            "ultima_cota": lqd,
        }
    except Exception:
        return {}


# ── Return calculation helpers ────────────────────────────────────────────────
def _nearest_before(series: pd.Series, target_date) -> float:
    if series.empty:
        return np.nan
    subset = series[series.index <= pd.Timestamp(target_date)]
    return np.nan if subset.empty else subset.iloc[-1]


def _pct_return(v_end, v_start) -> float:
    if pd.isna(v_end) or pd.isna(v_start) or v_start == 0:
        return np.nan
    return (v_end / v_start - 1) * 100


def compute_fund_returns(quota_series: pd.Series, today: date) -> dict:
    nan_row = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
    if quota_series.empty:
        return nan_row

    last_ts  = quota_series.index.max()
    last_val = quota_series.iloc[-1]

    prev_obs = quota_series[quota_series.index < last_ts]
    d_ret = np.nan if prev_obs.empty else _pct_return(last_val, prev_obs.iloc[-1])

    m_ret   = _pct_return(last_val, _nearest_before(quota_series, last_ts.replace(day=1) - timedelta(days=1)))
    ano_ret = _pct_return(last_val, _nearest_before(quota_series, date(last_ts.year - 1, 12, 31)))
    y1_ret  = _pct_return(last_val, _nearest_before(quota_series, last_ts - pd.DateOffset(years=1)))
    y2_ret  = _pct_return(last_val, _nearest_before(quota_series, last_ts - pd.DateOffset(years=2)))

    return {
        "D": d_ret, "M": m_ret, "ANO": ano_ret,
        "1ANO": y1_ret, "2ANOS": y2_ret,
        "ultima_cota": last_ts.date(),
    }


def compute_cdi_returns(cdi_daily: pd.Series, ref_date: date) -> dict:
    nan_row = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
    if cdi_daily.empty:
        return nan_row

    s = cdi_daily[cdi_daily.index <= pd.Timestamp(ref_date)]
    if s.empty:
        return nan_row

    last_ts = s.index.max()

    def cum(from_d, to_d):
        subset = s[(s.index > pd.Timestamp(from_d)) & (s.index <= pd.Timestamp(to_d))]
        return np.nan if subset.empty else (np.prod(1 + subset.values / 100) - 1) * 100

    return {
        "D":     float(s.iloc[-1]),
        "M":     cum(last_ts.replace(day=1) - timedelta(days=1), last_ts),
        "ANO":   cum(date(last_ts.year - 1, 12, 31), last_ts),
        "1ANO":  cum(last_ts - pd.DateOffset(years=1), last_ts),
        "2ANOS": cum(last_ts - pd.DateOffset(years=2), last_ts),
        "ultima_cota": last_ts.date(),
    }


def compute_price_returns(price_series: pd.Series, ref_date: date) -> dict:
    nan_row = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
    if price_series.empty:
        return nan_row

    s = price_series[price_series.index <= pd.Timestamp(ref_date)]
    if s.empty:
        return nan_row

    last_ts  = s.index.max()
    last_val = float(s.iloc[-1])

    prev_obs = s[s.index < last_ts]
    d_ret   = _pct_return(last_val, prev_obs.iloc[-1]) if not prev_obs.empty else np.nan
    m_ret   = _pct_return(last_val, _nearest_before(s, last_ts.replace(day=1) - timedelta(days=1)))
    ano_ret = _pct_return(last_val, _nearest_before(s, date(last_ts.year - 1, 12, 31)))
    y1_ret  = _pct_return(last_val, _nearest_before(s, last_ts - pd.DateOffset(years=1)))
    y2_ret  = _pct_return(last_val, _nearest_before(s, last_ts - pd.DateOffset(years=2)))

    return {
        "D": d_ret, "M": m_ret, "ANO": ano_ret,
        "1ANO": y1_ret, "2ANOS": y2_ret,
        "ultima_cota": last_ts.date(),
    }


# ── Main data loader ──────────────────────────────────────────────────────────
@st.cache_data(ttl=900, show_spinner=False)
def load_exclusivos_data():
    today = date.today()

    # Mesma estratégia do monitor principal: janela de 4 meses + âncoras de 1/2 anos ±1 mês
    months_to_fetch = set()
    for delta in range(4):
        y, m = today.year, today.month - delta
        while m < 1:
            m += 12; y -= 1
        months_to_fetch.add((y, m))
    for ref_days, in [(365,), (730,)]:
        ref = (today - timedelta(days=ref_days)).replace(day=1)
        for delta in [-1, 0, 1]:
            y, m = ref.year, ref.month + delta
            while m < 1:
                m += 12; y -= 1
            while m > 12:
                m -= 12; y += 1
            months_to_fetch.add((y, m))
    months_to_fetch.add((today.year - 1, 12))
    months_to_fetch.add((today.year - 2, 12))

    valid_months = [
        (yr, mo) for yr, mo in sorted(months_to_fetch)
        if not (yr < 2020 or yr > today.year or (yr == today.year and mo > today.month))
    ]

    start_str = f"01/01/{today.year - 2}"
    end_str   = today.strftime("%d/%m/%Y")
    start_ts  = int(datetime(today.year - 2, 1, 1).timestamp())
    end_ts    = int(datetime.now().timestamp())

    with ThreadPoolExecutor(max_workers=8) as ex:
        cvm_futs    = [ex.submit(_fetch_cvm_excl, yr, mo) for yr, mo in valid_months]
        cdi_fut     = ex.submit(_fetch_bcb_excl, 12, start_str, end_str)
        ibov_fut    = ex.submit(_fetch_yf_excl, "%5EBVSP", start_ts, end_ts)
        imab_fut    = ex.submit(_fetch_maisretorno_excl, "ima-b")
        imab5_fut   = ex.submit(_fetch_maisretorno_excl, "ima-b-5")
        imab5p_fut  = ex.submit(_fetch_maisretorno_excl, "ima-b-5-mais")
        cvm_dfs     = [f.result() for f in cvm_futs]
        cdi_daily   = cdi_fut.result()
        ibov_daily  = ibov_fut.result()
        imab_ret    = imab_fut.result()
        imab5_ret   = imab5_fut.result()
        imab5p_ret  = imab5p_fut.result()

    dfs = [d for d in cvm_dfs if not d.empty]
    quota_map = {}
    pl_map    = {}
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined.sort_values(["CNPJ_norm", "DT_COMPTC"]).drop_duplicates()
        for cnpj, grp in combined.groupby("CNPJ_norm"):
            grp_s = grp.set_index("DT_COMPTC").sort_index()
            quota_map[cnpj] = grp_s["VL_QUOTA"]
            if "VL_PATRIM_LIQ" in grp_s.columns:
                pl_s = grp_s["VL_PATRIM_LIQ"].dropna()
                if not pl_s.empty:
                    pl_map[cnpj] = float(pl_s.iloc[-1])

    all_last = [s.index.max() for s in quota_map.values() if not s.empty]
    ref_date = max(all_last).date() if all_last else today

    # Benchmark returns por chave — IMA-Bs com ultima_cota alinhada ao ref_date dos fundos
    def _align(d: dict) -> dict:
        r = dict(d)
        r["ultima_cota"] = ref_date
        return r

    bmark_returns = {
        "cdi":        compute_cdi_returns(cdi_daily, ref_date),
        "ibovespa":   compute_price_returns(ibov_daily, ref_date),
        "ima_b":      _align(imab_ret)   if imab_ret   else {k: np.nan for k in ["D","M","ANO","1ANO","2ANOS","ultima_cota"]},
        "ima_b5":     _align(imab5_ret)  if imab5_ret  else {k: np.nan for k in ["D","M","ANO","1ANO","2ANOS","ultima_cota"]},
        "ima_b5plus": _align(imab5p_ret) if imab5p_ret else {k: np.nan for k in ["D","M","ANO","1ANO","2ANOS","ultima_cota"]},
    }

    groups_data = []
    for group in FUND_GROUPS_EXCL:
        fund_rows = []
        for fund in group["funds"]:
            cnpj = fund["cnpj"]
            if cnpj in quota_map:
                ret = compute_fund_returns(quota_map[cnpj], today)
                uc  = ret.get("ultima_cota")
                if uc and (today - uc).days > 45:
                    ret = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
            else:
                ret = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
            pl = pl_map.get(cnpj, np.nan)
            fund_rows.append({"name": fund["name"], "returns": ret, "pl": pl})

        groups_data.append({
            "name":       group["group"],
            "fund_rows":  fund_rows,
            "benchmarks": [
                {"label": b["label"], "ret": bmark_returns.get(b["key"], {})}
                for b in group["benchmarks"]
            ],
        })

    return {"groups": groups_data, "ref_date": ref_date, "today": today}


# ── Formatting / rendering ────────────────────────────────────────────────────
def fmt_pct(v, decimals=2) -> str:
    return "-" if pd.isna(v) else f"{v:.{decimals}f}%"


def fmt_pl(v) -> str:
    if pd.isna(v) or v <= 0:
        return "-"
    if v >= 1_000_000_000:
        return f"R$ {v/1_000_000_000:.1f} Bi"
    return f"R$ {v/1_000_000:.1f} M"


def _num_cell(v_str: str, raw) -> str:
    if pd.isna(raw):
        return (
            f'<td style="text-align:right; padding:5px 12px; '
            f'background:{COLOR_EMPTY_BG}; color:{COLOR_EMPTY_TEXT}; '
            f'white-space:nowrap; font-variant-numeric:tabular-nums;">{v_str}</td>'
        )
    if raw == 0:
        return (
            f'<td style="text-align:right; padding:5px 12px; '
            f'background:{COLOR_EMPTY_BG}; color:{COLOR_META_TEXT}; '
            f'white-space:nowrap; font-variant-numeric:tabular-nums;">{v_str}</td>'
        )
    bg  = COLOR_POS_BG   if raw > 0 else COLOR_NEG_BG
    txt = COLOR_POS_TEXT if raw > 0 else COLOR_NEG_TEXT
    return (
        f'<td style="text-align:right; padding:5px 12px; '
        f'background:{bg}; color:{txt}; white-space:nowrap; '
        f'font-variant-numeric:tabular-nums;">{v_str}</td>'
    )


def build_html_table(data: dict) -> str:
    TH = (
        f"background:{COLOR_BMARK_BG}; color:#c8b8a8; "
        "padding:7px 12px; text-align:center; font-size:11px; font-weight:600; "
        "border-bottom:1px solid #3a2020; border-right:1px solid #2a1010; "
        "white-space:nowrap; letter-spacing:0.6px;"
    )
    TH_L = TH.replace("text-align:center", "text-align:left")

    cols = [
        ("FUNDO",     "min-width:220px; text-align:left;",  TH_L),
        ("PL",        "min-width:100px; text-align:right;",  TH),
        ("D",         "min-width:72px;  text-align:right;",  TH),
        ("M",         "min-width:72px;  text-align:right;",  TH),
        ("ANO",       "min-width:72px;  text-align:right;",  TH),
        ("1 ANO",     "min-width:76px;  text-align:right;",  TH),
        ("2 ANOS",    "min-width:76px;  text-align:right;",  TH),
        ("ÚLT. COTA", "min-width:110px; text-align:center;", TH),
        ("LIQUIDEZ",  "min-width:72px;  text-align:center;", TH),
        ("PUB. ALVO", "min-width:90px;  text-align:center;", TH),
    ]
    n_cols = len(cols)

    header_cells = "".join(
        f'<th style="{th_s} {col_s}">{label}</th>'
        for label, col_s, th_s in cols
    )

    html = f"""
    <style>
      body  {{ background:#0d0608; margin:0; padding:4px 0;
               font-family:'Segoe UI',Arial,sans-serif; }}
      table {{ border-collapse:collapse; width:100%; font-size:12px; }}
      td    {{ border-bottom:1px solid #2a1010; white-space:nowrap; }}
      tr:hover td {{ filter:brightness(1.12); }}
      .sec td {{
        background:#0d0608; color:{COLOR_ORANGE}; font-weight:700;
        font-size:11px; text-transform:uppercase; letter-spacing:2px;
        padding:10px 12px 4px 12px; border-bottom:none; border-top:none;
      }}
      .fund td {{ background:{COLOR_FUND_BG}; }}
      .bmark td {{ background:{COLOR_BMARK_BG}; font-style:italic; }}
      .name  {{ text-align:left;   padding:5px 12px; color:#e8d8d8; font-weight:500; }}
      .bname {{ text-align:left;   padding:5px 12px 5px 24px;
                color:{COLOR_META_TEXT}; font-style:italic; }}
      .meta  {{ text-align:center; padding:5px 12px;
                color:{COLOR_META_TEXT}; white-space:nowrap; }}
      .pl    {{ text-align:right;  padding:5px 12px;
                color:{COLOR_META_TEXT}; white-space:nowrap; }}
      .date  {{ text-align:center; padding:5px 12px;
                color:{COLOR_DATE_TEXT}; white-space:nowrap; }}
    </style>
    <table>
    <thead><tr>{header_cells}</tr></thead>
    <tbody>
    """

    for group in data["groups"]:
        html += (
            f'<tr class="sec"><td colspan="{n_cols}">'
            f'{group["name"]}</td></tr>\n'
        )

        for row in group["fund_rows"]:
            ret  = row["returns"]
            uc   = ret.get("ultima_cota")
            uc_s = uc.strftime("%d/%m/%Y") if uc and not pd.isna(uc) else "-"
            pl_s = fmt_pl(row.get("pl", np.nan))
            html += (
                f'<tr class="fund">'
                f'<td class="name">{row["name"]}</td>'
                f'<td class="pl">{pl_s}</td>'
                f'{_num_cell(fmt_pct(ret.get("D")),     ret.get("D"))}'
                f'{_num_cell(fmt_pct(ret.get("M")),     ret.get("M"))}'
                f'{_num_cell(fmt_pct(ret.get("ANO")),   ret.get("ANO"))}'
                f'{_num_cell(fmt_pct(ret.get("1ANO")),  ret.get("1ANO"))}'
                f'{_num_cell(fmt_pct(ret.get("2ANOS")), ret.get("2ANOS"))}'
                f'<td class="date">{uc_s}</td>'
                f'<td class="meta">N/A</td>'
                f'<td class="meta">N/A</td>'
                f'</tr>\n'
            )

        for bmark in group["benchmarks"]:
            bret   = bmark["ret"]
            uc_b   = bret.get("ultima_cota")
            uc_b_s = uc_b.strftime("%d/%m/%Y") if uc_b and not pd.isna(uc_b) else "-"
            html += (
                f'<tr class="bmark">'
                f'<td class="bname">{bmark["label"]}</td>'
                f'<td class="pl">—</td>'
                f'{_num_cell(fmt_pct(bret.get("D")),     bret.get("D"))}'
                f'{_num_cell(fmt_pct(bret.get("M")),     bret.get("M"))}'
                f'{_num_cell(fmt_pct(bret.get("ANO")),   bret.get("ANO"))}'
                f'{_num_cell(fmt_pct(bret.get("1ANO")),  bret.get("1ANO"))}'
                f'{_num_cell(fmt_pct(bret.get("2ANOS")), bret.get("2ANOS"))}'
                f'<td class="date">{uc_b_s}</td>'
                f'<td class="meta">—</td>'
                f'<td class="meta">—</td>'
                f'</tr>\n'
            )

    html += "</tbody></table>"
    return html


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # ── CSS global ────────────────────────────────────────────────────────────
    st.markdown("""
    <style>
      .stApp, [data-testid="stAppViewContainer"], .main { background-color:#0d0608 !important; }
      [data-testid="stSidebar"] { background-color:#0a0406 !important;
                                  border-right:1px solid #3a1515 !important; }
      .main .block-container { background-color:#0d0608; padding-top:0.8rem; max-width:100%; }
      #MainMenu { visibility:hidden; }
      footer    { visibility:hidden; }
      header[data-testid="stHeader"] { visibility:hidden; height:0; }
      /* Oculta navegação nativa de páginas do Streamlit */
      section[data-testid="stSidebar"] nav,
      [data-testid="stSidebarNav"],
      [data-testid="stSidebarNavItems"] { display:none !important; }
      p, div, span, label { color:#e0d0d0; }
    </style>
    """, unsafe_allow_html=True)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    try:
        with st.sidebar:
            import os as _os
            logo_path = _os.path.join(_os.path.dirname(__file__), "..", "tag_logo.png")
            if _os.path.exists(logo_path):
                st.image(logo_path, use_container_width=True)
            st.markdown("""
            <div style="padding:4px 16px 20px 16px; margin-top:4px;">
              <div style="color:#7B2D40; font-size:9px; letter-spacing:1.5px;
                          text-transform:uppercase; font-weight:700; margin-bottom:8px;">
                MONITOR
              </div>
              <div style="color:#e8d8d8; font-size:13px; padding:7px 10px;
                          background:#1a0c0c; border-radius:3px;
                          border-left:3px solid #444;">
                <a href="/" target="_self"
                   style="color:#b09090; text-decoration:none;">
                  Monitor - Fundos Condominiais
                </a>
              </div>
              <div style="color:#e8d8d8; font-size:13px; padding:7px 10px; margin-top:4px;
                          background:#1a0c0c; border-radius:3px;
                          border-left:3px solid #7B2D40;">
                Monitor - Fundos Exclusivos
              </div>
            </div>
            """, unsafe_allow_html=True)
    except Exception:
        pass

    # ── Cabeçalho ─────────────────────────────────────────────────────────────
    st.markdown(
        f"""
        <div style="padding:4px 0 10px 0;">
          <div style="font-size:21px; font-weight:600; color:#ffffff;
                      font-family:'Segoe UI',Arial,sans-serif; letter-spacing:0.4px;">
            Monitor de Fundos Exclusivos
          </div>
          <div style="height:2px; background:linear-gradient(to right,#E8801A,transparent);
                      margin-top:6px;"></div>
          <div style="color:#7a6060; font-size:11px; margin-top:6px;
                      font-family:'Segoe UI',Arial,sans-serif;">
            Atualizado em {datetime.now(ZoneInfo('America/Sao_Paulo')).strftime('%d/%m/%Y às %H:%M')}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Carrega dados ─────────────────────────────────────────────────────────
    with st.spinner("Carregando dados CVM, CDI e IBOVESPA..."):
        data = load_exclusivos_data()

    # ── Tabela ────────────────────────────────────────────────────────────────
    table_html = build_html_table(data)

    full_html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
  html, body {{ margin:0; padding:0; background:#0d0608; }}
</style>
</head>
<body>
{table_html}
</body>
</html>"""

    components.html(full_html, height=2000, scrolling=True)


import traceback as _tb
try:
    main()
except Exception as _e:
    st.error(f"Erro ao executar o app: {_e}")
    st.code(_tb.format_exc())
