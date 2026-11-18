"""
britech_to_supabase.py
Puxa retornos da API Britech e salva em data/britech_cache.json no repositório.
Rode localmente (precisa de acesso à rede interna da TAG para a API Britech).

Credencial: vem do .env do TIME (credenciais/.env, via env_do_time), o mesmo
que o boleteiro usa. Não existe mais .env nesta pasta — uma senha só, num
lugar só. O que já estiver em os.environ continua vencendo, para dar sobrepor
na mão ao testar.

Uso:
    python britech_to_supabase.py
"""

import os
import sys
import json
import subprocess
import time
import requests
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT   = Path(__file__).parent.parent
CACHE_FILE  = REPO_ROOT / "data" / "britech_cache.json"
CREDENCIAIS = REPO_ROOT / "credenciais"

# ── Credencial do time ────────────────────────────────────────────────────────
if not (os.environ.get("BRITECH_USER") and os.environ.get("BRITECH_PASS")):
    sys.path.insert(0, str(CREDENCIAIS))
    try:
        import env_do_time
        env_do_time.carregar()
    except Exception as e:
        sys.exit(f"Erro ao ler {CREDENCIAIS / '.env'}: {e}")

BRITECH_USER = os.environ.get("BRITECH_USER", "")
BRITECH_PASS = os.environ.get("BRITECH_PASS", "")

if not BRITECH_USER or not BRITECH_PASS:
    sys.exit(f"Erro: BRITECH_USER/BRITECH_PASS nao encontrados em "
             f"{CREDENCIAIS / '.env'}")

BRITECH_FUNDS = [
    # id_type="carteira": usa BuscaHistoricoCotaDia (FIDCs sem endpoint de estratégia)
    {"id_cliente":   80, "britech_start": "2020-01-01", "id_type": "carteira"},
    {"id_cliente": 2936, "britech_start": "2026-05-20"},
    {"id_cliente": 2778, "britech_start": "2026-05-04"},
]

# Índices de mercado gravados junto no cache. O Streamlit Cloud não alcança a
# API da Britech (ela só responde de dentro da rede da TAG), então o benchmark
# precisa chegar lá pelo mesmo caminho dos fundos: JSON no GitHub.
# IdIndice descobertos via MarketData/BuscaCotacaoIndiceMaxDt.
BRITECH_INDICES = {
    "CDI":      1,
    "IMA-B":    102,
    "IMA-B 5":  107,
    "IMA-B 5+": 108,
}

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


def compute_returns_from_cotas(id_carteira: int, britech_start: str) -> dict:
    """Calcula retornos D/M/ANO/1ANO/2ANOS a partir do histórico de cotas (BuscaHistoricoCotaDia).
    Usado para FIDCs que não têm endpoint de estratégia."""
    start = (date.today() - timedelta(days=800)).isoformat()
    end   = date.today().isoformat()
    url   = (
        f"https://tag.britech.com.br/WS/api/Fundo/BuscaHistoricoCotaDia"
        f"?idCarteira={id_carteira}&dataInicio={start}&dataFim={end}"
    )
    try:
        r = requests.get(url, auth=(BRITECH_USER, BRITECH_PASS),
                         headers={"Accept": "application/json"}, timeout=30)
        if r.status_code != 200 or not r.json():
            return {"d_ret": None, "m_ret": None, "ano_ret": None,
                    "y1_ret": None, "y2_ret": None,
                    "ref_date": date.today().isoformat(), "pl": None}
        rows = r.json()
    except Exception as e:
        print(f"  Erro API cotas: {e}")
        return {"d_ret": None, "m_ret": None, "ano_ret": None,
                "y1_ret": None, "y2_ret": None,
                "ref_date": date.today().isoformat(), "pl": None}

    cotas = {row["Data"][:10]: {"cota": float(row["CotaFechamento"]),
                                 "pl":   float(row["PLFechamento"])}
             for row in rows}
    sorted_dates = sorted(cotas.keys())
    ref      = sorted_dates[-1]
    ref_cota = cotas[ref]["cota"]
    ref_pl   = cotas[ref]["pl"]
    print(f"  Ultima cota: {ref} | Cota: {ref_cota:.7f} | PL: {ref_pl:,.2f}")

    def closest_before(target):
        for d in reversed(sorted_dates):
            if d <= target and d != ref:
                return cotas[d]["cota"]
        return None

    ref_dt  = date.fromisoformat(ref)
    prev_bd = ref_dt - timedelta(days=1)
    while prev_bd.weekday() >= 5:
        prev_bd -= timedelta(days=1)

    def ret(ini_str):
        c = closest_before(ini_str)
        if c is None:
            return None
        return round((ref_cota / c - 1) * 100, 6)

    return {
        "d_ret":    ret(prev_bd.isoformat()),
        "m_ret":    ret((ref_dt.replace(day=1) - timedelta(days=1)).isoformat()),
        "ano_ret":  ret(f"{ref_dt.year - 1}-12-31"),
        "y1_ret":   ret(ref_dt.replace(year=ref_dt.year - 1).isoformat()),
        "y2_ret":   ret(ref_dt.replace(year=ref_dt.year - 2).isoformat()),
        "ref_date": ref,
        "pl":       ref_pl,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def busca_indices() -> dict:
    """Retornos dos índices de mercado, numa única chamada.

    Fundo/BuscaRentabilidadeIndices aceita vários IdIndice separados por
    vírgula e devolve as janelas já acumuladas em %. Um ID inexistente derruba
    a chamada inteira com HTTP 400, então a lista tem de estar certa.

    Atenção: as janelas são o mês/ano-calendário que CONTÉM DataIndice,
    calculadas com o dado existente no momento da consulta — não são
    point-in-time. Para consulta ao vivo (que é o caso aqui) está correto.
    """
    ids = ",".join(str(i) for i in BRITECH_INDICES.values())
    try:
        r = requests.get(
            "https://tag.britech.com.br/WS/api/Fundo/BuscaRentabilidadeIndices",
            params={"idIndices": ids, "dataReferencia": date.today().isoformat()},
            auth=(BRITECH_USER, BRITECH_PASS),
            headers={"Accept": "application/json"}, timeout=60,
        )
        if r.status_code != 200:
            print(f"  [AVISO] indices: HTTP {r.status_code} — {r.text[:140]}")
            return {}
        rows = r.json()
    except Exception as e:
        print(f"  [AVISO] indices: {type(e).__name__}: {str(e)[:120]}")
        return {}

    por_id = {it["IdIndice"]: it for it in rows}
    fora = {}
    for nome, idx in BRITECH_INDICES.items():
        it = por_id.get(idx)
        if not it:
            print(f"  [AVISO] {nome} (id {idx}) nao veio na resposta.")
            continue
        fora[nome] = {
            "d_ret":    it.get("RentabilidadeDia"),
            "m_ret":    it.get("RentabilidadeMes"),
            "ano_ret":  it.get("RentabilidadeAno"),
            "y1_ret":   it.get("Rentabilidade12Meses"),
            "y2_ret":   it.get("Rentabilidade24Meses"),
            "ref_date": str(it.get("DataIndice") or "")[:10],
        }
    return fora


def checa_autenticacao() -> bool:
    """Confirma que a credencial ainda vale antes de tentar qualquer fundo.

    Sem isso, um 401 vira 'nenhuma cota encontrada' e o robô segue como se o
    fundo estivesse sem dado — foi o que apagou 2 semanas de histórico quando a
    senha mudou em 04/11/2026.
    """
    try:
        r = requests.get(
            "https://tag.britech.com.br/WS/api/Fundo/BuscaRentabilidadeIndices",
            params={"idIndices": "1", "dataReferencia": date.today().isoformat()},
            auth=(BRITECH_USER, BRITECH_PASS),
            headers={"Accept": "application/json"}, timeout=30,
        )
    except Exception as e:
        print(f"[ERRO] Nao foi possivel falar com a Britech: {e}")
        return False
    if r.status_code == 401:
        print("[ERRO] HTTP 401 — credencial recusada pela Britech.")
        print(f"       Atualize BRITECH_USER/BRITECH_PASS em {CREDENCIAIS / '.env'}")
        print("       e tambem nos Secrets do Streamlit Cloud.")
        print("       Nao reexecute em sequencia: varios 401 bloqueiam a conta.")
        return False
    if r.status_code != 200:
        print(f"[ERRO] Britech respondeu HTTP {r.status_code}: {r.text[:200]}")
        return False
    return True


def limpa_lock_orfao() -> None:
    """Remove .git/index.lock orfao (git morto nao deixa processo vivo).

    Um lock preso em 25/09/2026 fez o push falhar por 6 dias sem ninguem notar.
    """
    lock = REPO_ROOT / ".git" / "index.lock"
    if not lock.exists():
        return
    idade_min = (time.time() - lock.stat().st_mtime) / 60
    if idade_min < 10:
        print(f"[AVISO] index.lock recente ({idade_min:.0f} min) — "
              f"pode haver git rodando. Nao vou remover.")
        return
    try:
        lock.unlink()
        print(f"[AVISO] index.lock orfao ({idade_min:.0f} min) removido.")
    except Exception as e:
        print(f"[AVISO] nao consegui remover index.lock: {e}")


def main():
    today = date.today()
    print(f"Atualizando retornos Britech — {today}\n")

    if not checa_autenticacao():
        print("\nAbortando sem gravar nada — o cache antigo fica preservado.")
        sys.exit(2)

    falhas = []

    # Carrega cache existente para comparar datas
    existing = {}
    if CACHE_FILE.exists():
        try:
            existing = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    cache = {}
    for fund in BRITECH_FUNDS:
        id_c    = fund["id_cliente"]
        start   = fund["britech_start"]
        id_type = fund.get("id_type", "cliente")
        print(f"ID {id_c} (inicio {start}, tipo {id_type})")

        if id_type == "carteira":
            retornos = compute_returns_from_cotas(id_c, start)
        else:
            retornos = compute_returns(id_c, start)

        # Proteção 1: resultado totalmente vazio nunca sobrescreve dado bom.
        # Quando a API falha (401, timeout, fundo sem cota), compute_returns
        # devolve tudo None com ref_date = HOJE. Como essa data é mais nova que
        # a do cache, a trava por data não pegava e o dado bom era apagado.
        campos = ("d_ret", "m_ret", "ano_ret", "y1_ret", "y2_ret")
        veio_vazio = all(retornos.get(k) is None for k in campos)
        prev = existing.get(str(id_c), {})
        prev_tinha_dado = any(prev.get(k) is not None for k in campos)

        if veio_vazio and prev_tinha_dado:
            print(f"  [AVISO] API nao devolveu nenhum retorno — mantendo cache de "
                  f"{prev.get('ref_date')}.")
            retornos = prev
            falhas.append(id_c)
        elif veio_vazio:
            print(f"  [AVISO] API nao devolveu nenhum retorno e nao ha cache anterior.")
            falhas.append(id_c)
        else:
            # Proteção 2: não aceita data mais antiga que a já gravada
            prev_date_str = prev.get("ref_date", "")
            new_date_str  = retornos.get("ref_date", "")
            if prev_date_str and new_date_str and new_date_str < prev_date_str:
                print(f"  [AVISO] Britech retrocedeu: {prev_date_str} → {new_date_str}. "
                      f"Mantendo data anterior.")
                retornos = prev

        for k, v in retornos.items():
            print(f"  {k}: {v if v is not None else 'N/D'}")

        cache[str(id_c)] = retornos
        print()

    # ── Índices de mercado (CDI, IMA-B, IMA-B 5, IMA-B 5+) ────────────────────
    # Chave "indices" não colide com os IDs de fundo, que são numéricos.
    print("Indices de mercado")
    indices = busca_indices()
    prev_idx = existing.get("indices", {})
    if indices:
        # Mesma trava dos fundos: índice que não veio mantém o valor anterior.
        for nome in BRITECH_INDICES:
            if nome not in indices and nome in prev_idx:
                print(f"  [AVISO] {nome} nao veio — mantendo cache anterior.")
                indices[nome] = prev_idx[nome]
        for nome, v in indices.items():
            print(f"  {nome:<10} {v['ref_date']}  D={v['d_ret']}  M={v['m_ret']}  "
                  f"ANO={v['ano_ret']}")
        cache["indices"] = indices
    elif prev_idx:
        print("  [AVISO] nenhum indice veio — mantendo cache anterior.")
        cache["indices"] = prev_idx
        falhas.append("indices")
    else:
        print("  [AVISO] nenhum indice veio e nao ha cache anterior.")
        falhas.append("indices")
    print()

    # Grava JSON
    CACHE_FILE.parent.mkdir(exist_ok=True)
    CACHE_FILE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Gravado em {CACHE_FILE}")

    # Commit e push
    limpa_lock_orfao()
    git_ok = True
    try:
        subprocess.run(["git", "-C", str(REPO_ROOT), "add", "data/britech_cache.json"],
                       check=True)
        r = subprocess.run(["git", "-C", str(REPO_ROOT), "commit", "-m",
                            f"data: atualiza retornos Britech ({today})"],
                           capture_output=True, text=True)
        if r.returncode != 0 and "nothing to commit" not in (r.stdout + r.stderr):
            raise subprocess.CalledProcessError(r.returncode, "git commit",
                                                output=r.stdout, stderr=r.stderr)
        subprocess.run(["git", "-C", str(REPO_ROOT), "push", "origin", "HEAD:main"],
                       check=True)
        print("Push para GitHub concluido.")
    except subprocess.CalledProcessError as e:
        git_ok = False
        print(f"[ERRO] git falhou: {e}")

    # Sai com codigo != 0 quando algo deu errado, para a tarefa agendada do
    # Windows marcar a execucao como falha em vez de 'Pronto'.
    if falhas:
        print(f"\n[ERRO] Sem retorno da Britech para: {', '.join(map(str, falhas))}")
    if falhas or not git_ok:
        print("Concluido COM FALHAS.")
        sys.exit(1)

    print("\nConcluido.")


if __name__ == "__main__":
    main()
