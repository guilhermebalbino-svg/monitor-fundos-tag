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
    Retorna o primeiro elemento do JSON se ValorFinal > 0, senão None.
    A API retorna zeros silenciosamente quando dataFim está além da última cota.
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
        # A API retorna zeros silenciosamente quando dataFim está além da última cota
        if row.get("RentabilidadeCotaBruta", 0) == 0 and row.get("RendimentoBruta", 0) == 0:
            return None
        return row
    except Exception as e:
        print(f"  Erro API: {e}")
        return None


def find_last_cota_date(id_cliente: int, britech_start: str, from_date: date) -> date | None:
    """Retrocede até 14 dias para encontrar o último dia com cota disponível."""
    d = from_date
    for _ in range(14):
        d -= timedelta(days=1)
        if d.weekday() >= 5:
            continue
        row = _call_periodo(id_cliente, britech_start, d.isoformat())
        if row is not None:
            # DataFim real pode ser diferente do solicitado
            data_fim_str = row.get("DataFim", "")[:10]
            last = date.fromisoformat(data_fim_str) if data_fim_str else d
            print(f"  Ultima cota: {last}")
            return last
    return None


def fetch_return(id_cliente: int, start: str, end: str) -> float | None:
    """Retorna RentabilidadeCotaBruta para o período, ou None."""
    row = _call_periodo(id_cliente, start, end)
    if row is None:
        return None
    return float(row["RentabilidadeCotaBruta"])


def compute_returns(id_cliente: int, britech_start: str) -> dict:
    start_dt  = date.fromisoformat(britech_start)
    today     = date.today()
    last_date = find_last_cota_date(id_cliente, britech_start, today)

    if last_date is None:
        print("  Nenhuma cota encontrada.")
        return {"m_ret": None, "ano_ret": None, "y1_ret": None,
                "y2_ret": None, "ref_date": today.isoformat()}

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

    m_ini   = last_date.replace(day=1) - timedelta(days=1)
    ano_ini = date(last_date.year - 1, 12, 31)
    y1_ini  = date(last_date.year - 1, last_date.month, last_date.day)
    y2_ini  = date(last_date.year - 2, last_date.month, last_date.day)

    return {
        "m_ret":    _get(m_ini,   allow_collapse=True),
        "ano_ret":  _get(ano_ini, allow_collapse=True),  # usa início do fundo no 1º ano
        "y1_ret":   _get(y1_ini,  allow_collapse=False),
        "y2_ret":   _get(y2_ini,  allow_collapse=False),
        "ref_date": last_date.isoformat(),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    print(f"Atualizando retornos Britech — {today}\n")

    cache = {}
    for fund in BRITECH_FUNDS:
        id_c  = fund["id_cliente"]
        start = fund["britech_start"]
        print(f"ID {id_c} (inicio {start})")

        retornos = compute_returns(id_c, start)
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
