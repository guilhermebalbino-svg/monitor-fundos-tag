"""
Fundos_Exclusivos.py — Monitor de Fundos Exclusivos TAG
Página Streamlit independente (multi-page app).
"""
import io
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
    initial_sidebar_state="collapsed",
)

# ── Fund config ───────────────────────────────────────────────────────────────
EXCLUSIVOS_FUNDS = [
    {"cnpj": "08621422000196", "name": "ALFER FIF MULTIMERCADO"},
    {"cnpj": "18611538000106", "name": "ALRD FIF MULTIMERCADO"},
    {"cnpj": "47512461000107", "name": "FUTURO II FIF CIC MULTIMERCADO"},
    {"cnpj": "13591889000170", "name": "GENESIS PLUS RESP LIMITADA FIF MULTIMERCADO CRÉDITO PRIVADO"},
    {"cnpj": "17400234000129", "name": "GUVIDALA FIF MULTIMERCADO"},
    {"cnpj": "17335641000108", "name": "HL TB FIF MULTIMERCADO"},
    {"cnpj": "63696630000162", "name": "JFT FIF MULTIMERCADO"},
    {"cnpj": "64018596000130", "name": "JPA AGRO FIF"},
    {"cnpj": "11409553000119", "name": "JUBA FIF MULTIMERCADO"},
    {"cnpj": "65223808000183", "name": "JUBA III FIF MULTIMERCADO"},
    {"cnpj": "37287572000103", "name": "LA PLATA FIF CIC MULTIMERCADO"},
    {"cnpj": "57210425000142", "name": "LAGERTHA FIF CIC MULTIMERCADO"},
    {"cnpj": "17413818000139", "name": "LAURUS FIF MULTIMERCADO"},
    {"cnpj": "21015772000177", "name": "LUSS FIF MULTIMERCADO"},
    {"cnpj": "17413812000161", "name": "MAGNÓLIAS FIF MULTIMERCADO"},
    {"cnpj": "17425221000104", "name": "MAHALÁKSHMI FI MULTIMERCADO"},
    {"cnpj": "46420207000116", "name": "MARAU FI MULTIMERCADO"},
    {"cnpj": "53100651000110", "name": "MARAU II FIF MULTIMERCADO"},
    {"cnpj": "53026176000189", "name": "MARIA SILVIA FI FINANCEIRO INVEST NO EXTERIOR RESP LIMITADA MULTIMERCADO CRÉDITO PRIVADO"},
    {"cnpj": "26342026000101", "name": "MEMÓRIAS FIF MULTIMERCADO"},
    {"cnpj": "10841486000144", "name": "MYBS FIF CIC MULTIMERCADO"},
    {"cnpj": "45560872000142", "name": "OCEANUS FIF MULTIMERCADO"},
    {"cnpj": "66763983000126", "name": "PARANAÍBA RESP LIMITADA FIF MULTIMERCADO CRÉDITO PRIVADO"},
    {"cnpj": "54912091000160", "name": "POTENTIA I FI MULTIMERCADO"},
    {"cnpj": "26768797000165", "name": "RAGNAR FIF MULTIMERCADO"},
    {"cnpj": "65919962000194", "name": "RANATORI II FI MULTIMERCADO"},
    {"cnpj": "09009733000161", "name": "RANATORI RESP LIMITADA FI MULTIMERCADO CRÉDITO PRIVADO"},
    {"cnpj": "08807608000134", "name": "RINTER FIF MULTIMERCADO"},
    {"cnpj": "53077066000146", "name": "RINTER II FI MULTIMERCADO"},
    {"cnpj": "26315508000172", "name": "ROPREV RESP LIMITADA FIF MULTIMERCADO CRÉDITO PRIVADO"},
    {"cnpj": "51389373000137", "name": "SB STONES FIF MULTIMERCADO"},
    {"cnpj": "39432540000180", "name": "SCUBI RESP LIMITADA FIF MULTIMERCADO CRÉDITO PRIVADO"},
    {"cnpj": "11827429000173", "name": "SOLARIUM FIF MULTIMERCADO"},
]

_CNPJS_EXCL = frozenset(f["cnpj"] for f in EXCLUSIVOS_FUNDS)

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
            usecols=["CNPJ_FUNDO_CLASSE", "DT_COMPTC", "VL_QUOTA"],
            dtype={"CNPJ_FUNDO_CLASSE": str, "DT_COMPTC": str, "VL_QUOTA": float},
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


# ── Return calculation helpers ────────────────────────────────────────────────
def _nearest_before(series: pd.Series, target_date) -> float:
    if series.empty:
        return np.nan
    target = pd.Timestamp(target_date)
    subset = series[series.index <= target]
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

    v_prev_month = _nearest_before(quota_series, last_ts.replace(day=1) - timedelta(days=1))
    m_ret = _pct_return(last_val, v_prev_month)

    v_prev_year = _nearest_before(quota_series, date(last_ts.year - 1, 12, 31))
    ano_ret = _pct_return(last_val, v_prev_year)

    v_1y = _nearest_before(quota_series, last_ts - pd.DateOffset(years=1))
    y1_ret = _pct_return(last_val, v_1y)

    v_2y = _nearest_before(quota_series, last_ts - pd.DateOffset(years=2))
    y2_ret = _pct_return(last_val, v_2y)

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
        "D":    float(s.iloc[-1]),
        "M":    cum(last_ts.replace(day=1) - timedelta(days=1), last_ts),
        "ANO":  cum(date(last_ts.year - 1, 12, 31), last_ts),
        "1ANO": cum(last_ts - pd.DateOffset(years=1), last_ts),
        "2ANOS": cum(last_ts - pd.DateOffset(years=2), last_ts),
        "ultima_cota": last_ts.date(),
    }


# ── Main data loader ──────────────────────────────────────────────────────────
@st.cache_data(ttl=900, show_spinner=False)
def load_exclusivos_data():
    today = date.today()

    months = []
    d = today.replace(day=1)
    for _ in range(3):
        months.append((d.year, d.month))
        d = (d - timedelta(days=1)).replace(day=1)

    start_str = f"01/01/{today.year - 2}"
    end_str   = today.strftime("%d/%m/%Y")

    with ThreadPoolExecutor(max_workers=4) as ex:
        cvm_futs  = [ex.submit(_fetch_cvm_excl, y, m) for y, m in months]
        cdi_fut   = ex.submit(_fetch_bcb_excl, 12, start_str, end_str)
        cvm_dfs   = [f.result() for f in cvm_futs]
        cdi_daily = cdi_fut.result()

    dfs = [d for d in cvm_dfs if not d.empty]
    quota_map = {}
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        for cnpj, grp in combined.groupby("CNPJ_norm"):
            quota_map[cnpj] = (
                grp.set_index("DT_COMPTC")["VL_QUOTA"].sort_index()
            )

    # ref_date = última cota disponível
    all_last = [s.index.max() for s in quota_map.values() if not s.empty]
    ref_date = max(all_last).date() if all_last else today

    fund_rows = []
    for fund in EXCLUSIVOS_FUNDS:
        cnpj = fund["cnpj"]
        if cnpj in quota_map:
            ret = compute_fund_returns(quota_map[cnpj], today)
            uc = ret.get("ultima_cota")
            if uc and (today - uc).days > 45:
                ret = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
        else:
            ret = {k: np.nan for k in ["D", "M", "ANO", "1ANO", "2ANOS", "ultima_cota"]}
        fund_rows.append({"name": fund["name"], "returns": ret})

    cdi_ret = compute_cdi_returns(cdi_daily, ref_date)

    return {
        "fund_rows": fund_rows,
        "cdi_ret":   cdi_ret,
        "ref_date":  ref_date,
        "today":     today,
    }


# ── Formatting / rendering ────────────────────────────────────────────────────
def fmt_pct(v, decimals=2) -> str:
    return "-" if pd.isna(v) else f"{v:.{decimals}f}%"


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
    fund_rows = data["fund_rows"]
    cdi_ret   = data["cdi_ret"]

    TH = (
        f"background:{COLOR_BMARK_BG}; color:#c8b8a8; "
        "padding:7px 12px; text-align:center; font-size:11px; font-weight:600; "
        "border-bottom:1px solid #3a2020; border-right:1px solid #2a1010; "
        "white-space:nowrap; letter-spacing:0.6px;"
    )
    TH_L = TH.replace("text-align:center", "text-align:left")

    cols = [
        ("FUNDO",     "min-width:260px; text-align:left;",  TH_L),
        ("D",         "min-width:72px;  text-align:right;",  TH),
        ("M",         "min-width:72px;  text-align:right;",  TH),
        ("ANO",       "min-width:72px;  text-align:right;",  TH),
        ("1 ANO",     "min-width:76px;  text-align:right;",  TH),
        ("2 ANOS",    "min-width:76px;  text-align:right;",  TH),
        ("ÚLT. COTA", "min-width:110px; text-align:center;", TH),
        ("LIQUIDEZ",  "min-width:72px;  text-align:center;", TH),
        ("PUB. ALVO", "min-width:90px;  text-align:center;", TH),
    ]

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
      .date  {{ text-align:center; padding:5px 12px;
                color:{COLOR_DATE_TEXT}; white-space:nowrap; }}
    </style>
    <table>
    <thead><tr>{header_cells}</tr></thead>
    <tbody>
    """

    # Seção MULTIMERCADO
    n_cols = len(cols)
    html += (
        f'<tr class="sec"><td colspan="{n_cols}">'
        f'MULTIMERCADO</td></tr>\n'
    )

    # Linhas dos fundos
    for row in fund_rows:
        ret = row["returns"]
        uc  = ret.get("ultima_cota")
        uc_s = uc.strftime("%d/%m/%Y") if uc and not pd.isna(uc) else "-"
        html += (
            f'<tr class="fund">'
            f'<td class="name">{row["name"]}</td>'
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

    # Benchmark CDI
    uc_cdi = cdi_ret.get("ultima_cota")
    uc_cdi_s = uc_cdi.strftime("%d/%m/%Y") if uc_cdi and not pd.isna(uc_cdi) else "-"
    html += (
        f'<tr class="bmark">'
        f'<td class="bname">CDI ACUMULADO</td>'
        f'{_num_cell(fmt_pct(cdi_ret.get("D")),     cdi_ret.get("D"))}'
        f'{_num_cell(fmt_pct(cdi_ret.get("M")),     cdi_ret.get("M"))}'
        f'{_num_cell(fmt_pct(cdi_ret.get("ANO")),   cdi_ret.get("ANO"))}'
        f'{_num_cell(fmt_pct(cdi_ret.get("1ANO")),  cdi_ret.get("1ANO"))}'
        f'{_num_cell(fmt_pct(cdi_ret.get("2ANOS")), cdi_ret.get("2ANOS"))}'
        f'<td class="date">{uc_cdi_s}</td>'
        f'<td class="meta">—</td>'
        f'<td class="meta">—</td>'
        f'</tr>\n'
    )

    html += "</tbody></table>"
    return html


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
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
                  📊 Fundos Condominiais
                </a>
              </div>
              <div style="color:#e8d8d8; font-size:13px; padding:7px 10px; margin-top:4px;
                          background:#1a0c0c; border-radius:3px;
                          border-left:3px solid #7B2D40;">
                📈 Fundos Exclusivos
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
    with st.spinner("Carregando dados CVM e CDI..."):
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

    components.html(full_html, height=1500, scrolling=True)


import traceback as _tb
try:
    main()
except Exception as _e:
    st.error(f"Erro ao executar o app: {_e}")
    st.code(_tb.format_exc())
