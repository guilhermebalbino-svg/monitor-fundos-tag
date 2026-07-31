# Dashboard Offshore — Histórico e Documentação

_Última atualização: 2026-07-16_

## Arquivos na pasta
- **Dashboard_Offshore.html** — dashboard principal (versão atual).
- **Dashboard_Offshore_backup_2026-07-16.html** — backup datado (conteúdo idêntico ao principal em 16/07/2026).
- **Clientes Offshore - RESUMO CONTAS.xlsx** — planilha fonte (perfis, retornos, D/ND, PL).
- **Clientes_Off_Clientes_off_Retornos_Consolidada.xlsx** / **_v2_Aberta por ativo.xlsx** — extrações do Addepar.
- **Validacao.xlsx** — batimento/validação.

## O que o dashboard contém (abas)
1. **Overview** — nº de clientes, PL total/médio, D vs ND, PL por perfil.
2. **Retorno MTD — Jun/2026** — retorno de cada cliente vs. carteira modelo, delta (pp), status Acima/Abaixo, drivers.
3. **Retorno YTD 2026** — mesma lógica, acumulado do ano.
4. **Carteiras Modelo** — cards dos modelos, comparativo de retornos (IRR), risco × retorno e alocação por ativo.
5. **Comentários performance mensal** _(adicionada em 16/07/2026)_ — duas tabelas (clientes D e clientes ND) no molde do Retorno MTD, com coluna **Comentário editável**. Salva automaticamente no navegador (localStorage) e permite **Exportar (.csv)** e **Imprimir/PDF**.

## Estrutura técnica (HTML único, dados embutidos em JS)
- `const OV=[...]` — lista de clientes (nome, pl, mtd, ytd, irr12, irr24, vol, officer, dnd, perfil, inception, novo2026, banco, grupo, segmento).
- `const M={clients:[...], stats:{...}, total:{...}, benchmarks, drivers}` — comparação **MTD** vs modelo.
- `const Y={...}` — comparação **YTD** vs modelo.
- `const CM_DATA=[...]` — dados das carteiras modelo e seus ativos.
- Agregados por perfil ficam em `M.stats`/`Y.stats` (buckets **Conservador / Moderado / Arrojado**) e nos totais `M.total`/`Y.total` — **são valores fixos (hardcoded)**; ao mudar perfil de um cliente é preciso recalcular esses buckets e totais.

### Mapeamento perfil → carteira modelo
- Conservador, Conservador HF, Conservador s/ Bolsa → **BTG Conservador s/ Bolsa** (bucket "Conservador")
- Moderado, Moderado HF → **BTG Moderado HF** (bucket "Moderado")
- Agressivo HF → **BTG FOF IE SOFR+3** (bucket "Arrojado")

## Alterações feitas em 16/07/2026
### 1. Mudança de perfil de dois clientes
- **LUMA PARTNERS LTD:** Conservador s/ Bolsa → **Conservador** (mesmo modelo de referência; delta/status não mudam).
- **SPYKE COMPANY LTD:** Conservador → **Moderado HF** (passa a comparar vs BTG Moderado HF).
  - MTD: inverteu de **Abaixo → Acima** do modelo (−0,02% vs −0,88% do modelo).
  - YTD: segue **Abaixo** (1,46% vs 3,09%).
- Recalculado nos arrays `OV`, `M.clients`, `Y.clients` e nos buckets/totais:
  - Bucket Conservador: n 29 → 28; Bucket Moderado: n 23 → 24.
  - Total MTD acima do modelo: 25 → 26 (abaixo 32 → 31). Total YTD inalterado (12 acima / 45 abaixo).
- Observação: a planilha RESUMO CONTAS estava aberta no Excel; a alteração de perfil foi feita direto no dashboard com base nos novos perfis informados. **Salvar a planilha** para manter as duas fontes em sincronia.

### 2. Nova aba "Comentários performance mensal"
- Tabela de clientes **D** (17) e tabela de clientes **ND** (40), ordenadas por delta.
- Colunas: Cliente, Perfil, Modelo ref., Ret. cliente, Ret. modelo, Delta (pp), Status, **Comentário** (editável).
- Comentários salvos em `localStorage` (chave `cmt_notes_jun2026`) + exportação CSV como backup.

### 3. Incidente de corrupção e recuperação
- Durante a gravação da última edição, o arquivo foi **truncado**, apagando o código dos gráficos finais da aba "Carteiras Modelo".
- O trecho foi **reconstruído** (scatter risco × retorno + gráficos de alocação por modelo) e validado (jsdom): as 5 abas renderizam sem erros.
- **Recomendação:** manter o backup datado e criar nova cópia antes de futuras edições.

## Como manter o dashboard
- Ao mudar o perfil de um cliente: atualizar `OV`, `M.clients`, `Y.clients` e recalcular os buckets `stats` e os `total` (contagem acima/abaixo por modelo).
- Ao regerar o dashboard a partir das planilhas: confirmar que os perfis de LUMA (Conservador) e SPYKE (Moderado HF) estão corretos.
- Sempre fazer backup antes de editar.

## Dashboard_Realocacao.html — companion (adicionado em 31/07/2026)
Arquivo **novo e independente** (`Dashboard_Realocacao.html`), gerado a partir dos mesmos dados do Offshore. Não altera o dashboard principal.

**O que faz:** mostra, em US$, **quanto falta para cada cliente atingir a alocação-alvo do seu modelo**.
- Fórmula por cliente/classe: `gap$ = PL × (meta% − atual%)`. Positivo = falta comprar (verde); negativo = precisa reduzir (vermelho).
- 3 blocos: **KPIs** · **Consolidado por classe** (gap líquido do book inteiro, com barra atual vs. meta) · **Por cliente** (tabela filtrável; clique na linha abre o plano de realocação detalhado). Botões **Exportar CSV** e **Imprimir/PDF**.

**Fonte de dados:** reaproveita as constantes `DV_RAW` (PL + % atual por classe de cada cliente) e `DV_TGTS` (metas % por modelo) do próprio `Dashboard_Offshore.html`. As 6 classes: G7, ALT, CREDIT EM, COMM, EQ, CASH.

**Como regerar** (quando os dados do Offshore mudarem): re-extrair os blocos `DV_RAW` e `DV_TGTS` do `Dashboard_Offshore.html` e reinjetá-los no template (script Python usado na criação varre `const DV_RAW=[...]` e `const DV_TGTS={...}` por balanceamento de colchetes/chaves). Template guardado no scratchpad da sessão (`realoc_template.html`).

**Números de referência (snapshot 30/06/2026):** 57 clientes, PL US$ 192,3M, realocação total a executar ~US$ 63,4M (33% do PL). Book super-alocado em **Credit EM (−32,1M)**, sub-alocado em **Alternatives (+24,8M)** e **G7 (+11,4M)**.

**Ressalva de dados:** 3 clientes com classificação de ativos incompleta (cobertura ≠ 100%) — IMPERIUM (8,3%), MIDSOMMAR (95,2%), BOANNA (96,3%) — ficam sinalizados com badge "!" e aviso no plano; seus números são aproximados.
