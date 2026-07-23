"""
britech_to_supabase.py
Puxa retornos da API Britech e salva em data/britech_cache.json no repositório.
Rode localmente (precisa de acesso à rede interna da TAG para a API Britech).

Uso:
    python britech_to_supabase.py
"""

import os
import sys
import json
import subprocess
import requests
from datetime import date, timedelta
from pathlib import Path

# ── Carrega .env ──────────────────────────────────────────────────────────────
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

BRITECH_USER = os.environ.get("BRITECH_USER", "")
BRITECH_PASS = os.environ.get("BRITECH_PASS", "")

if not BRITECH_USER or not BRITECH_PASS:
    sys.exit("Erro: defina BRITECH_USER e BRITECH_PASS no .env")

REPO_ROOT  = Path(__file__).parent.parent
CACHE_FILE = REPO_ROOT / "data" / "britech_cache.json"

BRITECH_FUNDS = [
    {"id_cliente": 2936, "britech_start": "2026-05-20"},
    {"id_cliente": 2778, "britech_start": "2026-05-04"},
]

# ── API ────────────────────────────────────────────────────────────────────────

def _call_periodo(id_cliente: int, start: str, end: str) -> dict | None:
    """
    Chama BuscaRentabEstrategia_Periodo.
    Retorna o row se tiver DataFim válido ou retorno não-zero; None se a API não tiver dados.
    A API retorna zeros (sem DataFim) quando dataFim está além da última cota disponível.
    """
    url = (
        f"https://tag.britech.com.br/WS/api/Rentabilidade"
        f"/BuscaRentabEstrategia_Periodo"
        f"?idCliente={id_cliente}&dataInicio={start}&dataFim={end}"
    )
    try:
        r = requests.get(url, auth=(BRITECH_USER, BRITECH_PASS),
                         headers={"Accept": "application/json"}, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data:
            return None
        row = data[0]
        # Se a API retornou DataFim, há dados — aceita independente do retorno
        if (row.get("DataFim") or "")[:10]:
            return row
        # Sem DataFim: API retorna zeros silenciosamente quando não tem dados
        rent = row.get("RentabilidadeCotaBruta") or 0
        rend = row.get("RendimentoBruta") or 0
        if rent == 0 and rend == 0:
            return None
        return row
    except Exception as e:
        print(f"  Erro API: {e}")
        return None


def find_last_cota_date(id_cliente: int, britech_start: str, from_date: date) -> date | None:
    """
    Retrocede a partir de D-2 para encontrar o último dia com cota via BuscaRentabEstrategia_Periodo.
    Usa DataFim da resposta: a API informa o último dia disponível mesmo quando pedimos data futura.
    """
    # Inicia de D-2 (cota confirmada, padrão do monitor)
    d = from_date - timedelta(days=2)
    while d.weekday() >= 5:
        d -= timedelta(days=1)

    for _ in range(20):
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        row = _call_periodo(id_cliente, britech_start, d.isoformat())
        if row is not None:
            data_fim_str = (row.get("DataFim") or "")[:10]
            last = date.fromisoformat(data_fim_str) if data_fim_str else d
            print(f"  Ultima cota: {last}")
            return last
        d -= timedelta(days=1)
    return None


def fetch_return(id_cliente: int, start: str, end: str) -> float | None:
    """Retorna RentabilidadeCotaBruta para o período, ou None."""
    row = _call_periodo(id_cliente, start, end)
    if row is None:
        return None
    return float(row["RentabilidadeCotaBruta"])


def prev_business_day(d: date) -> date:
    """Retorna o dia útil anterior (sem considerar feriados)."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def fetch_pl(id_cliente: int, last_date: date) -> float | None:
    """Busca PL do último dia de cota via BuscaHistoricoCotaDia."""
    end   = last_date.isoformat()
    start = (last_date - timedelta(days=14)).isoformat()
    url   = (
        f"https://tag.britech.com.br/WS/api/Fundo/BuscaHistoricoCotaDia"
        f"?idCarteira={id_cliente}&dataInicio={start}&dataFim={end}"
    )
    try:
        r = requests.get(url, auth=(BRITECH_USER, BRITECH_PASS),
                         headers={"Accept": "application/json"}, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data:
            return None
        for row in reversed(data):
            pl = float(row.get("PLAbertura") or row.get("PLFechamento") or 0)
            if pl > 0:
                return pl
        return None
    except Exception as e:
        print(f"  Erro ao buscar PL: {e}")
        return None


def compute_returns(id_cliente: int, britech_start: str) -> dict:
    start_dt  = date.fromisoformat(britech_start)
    today     = date.today()
    last_date = find_last_cota_date(id_cliente, britech_start, today)

    if last_date is None:
        print("  Nenhuma cota encontrada.")
        return {"d_ret": None, "m_ret": None, "ano_ret": None,
                "y1_ret": None, "y2_ret": None, "ref_date": today.isoformat(),
                "pl": None}

    ref_str = last_date.isoformat()

    def _get(ini: date, allow_collapse: bool = False) -> float | None:
        """allow_collapse=True: se ini < start_dt, usa start_dt (ex: ANO no 1º ano)."""
        if ini < start_dt:
            if not allow_collapse:
                return None  # sem histórico suficiente (1ANO, 2ANOS)
            ini = start_dt
        if ini >= last_date:
            return None
        return fetch_return(id_cliente, ini.isoformat(), ref_str)

    d_ini   = prev_business_day(last_date)
    m_ini   = last_date.replace(day=1) - timedelta(days=1)
    ano_ini = date(last_date.year - 1, 12, 31)
    y1_ini  = date(last_date.year - 1, last_date.month, last_date.day)
    y2_ini  = date(last_date.year - 2, last_date.month, last_date.day)

    pl = fetch_pl(id_cliente, last_date)
    print(f"  pl: {pl}")

    return {
        "d_ret":    _get(d_ini,   allow_collapse=True),
        "m_ret":    _get(m_ini,   allow_collapse=True),
        "ano_ret":  _get(ano_ini, allow_collapse=True),
        "y1_ret":   _get(y1_ini,  allow_collapse=False),
        "y2_ret":   _get(y2_ini,  allow_collapse=False),
        "ref_date": last_date.isoformat(),
        "pl":       pl,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    print(f"Atualizando retornos Britech — {today}\n")

    # Carrega cache existente para comparar datas
    existing = {}
    if CACHE_FILE.exists():
        try:
            existing = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    cache = {}
    for fund in BRITECH_FUNDS:
        id_c  = fund["id_cliente"]
        start = fund["britech_start"]
        print(f"ID {id_c} (inicio {start})")

        retornos = compute_returns(id_c, start)

        # Proteção: não sobrescreve com data mais antiga que a já gravada
        prev = existing.get(str(id_c), {})
        prev_date_str = prev.get("ref_date", "")
        new_date_str  = retornos.get("ref_date", "")
        if (prev_date_str and new_date_str and new_date_str < prev_date_str
                and retornos.get("d_ret") is None):
            print(f"  [AVISO] Nova data {new_date_str} < cache {prev_date_str} com retornos nulos — mantendo cache anterior.")
            retornos = prev
        elif prev_date_str and new_date_str and new_date_str < prev_date_str:
            print(f"  [AVISO] Britech retrocedeu: {prev_date_str} → {new_date_str}. Mantendo data anterior.")
            retornos = prev

        for k, v in retornos.items():
            print(f"  {k}: {v if v is not None else 'N/D'}")

        cache[str(id_c)] = retornos
        print()

    # Grava JSON
    CACHE_FILE.parent.mkdir(exist_ok=True)
    CACHE_FILE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Gravado em {CACHE_FILE}")

    # Commit e push
    try:
        subprocess.run(["git", "-C", str(REPO_ROOT), "add", "data/britech_cache.json"],
                       check=True)
        subprocess.run(["git", "-C", str(REPO_ROOT), "commit", "-m",
                        f"data: atualiza retornos Britech ({today})"],
                       check=True)
        subprocess.run(["git", "-C", str(REPO_ROOT), "push", "origin", "HEAD:main"],
                       check=True)
        print("Push para GitHub concluido.")
    except subprocess.CalledProcessError as e:
        print(f"Aviso: git falhou ({e}). Verifique manualmente.")

    print("\nConcluido.")


if __name__ == "__main__":
    main()
