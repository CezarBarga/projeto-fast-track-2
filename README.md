# 🏛️ Projeto Fast Track 2 — PySpark + Databricks Free Tier

## Engenharia de Dados | Câmara dos Deputados

---

## Arquitetura

```
API Câmara dos Deputados
        │  spark.createDataFrame() + urllib
        ▼
┌─────────────────────────────────────┐
│  BRONZE  — dados brutos + auditoria │
│  Delta Lake | MERGE via SQL         │
└──────────────────┬──────────────────┘
                   │  Window Functions, tipagem, SCD2
                   ▼
┌─────────────────────────────────────┐
│  PRATA  — normalizado + dimensional │
│  dim_* | fato_* | proposicoes_scd2  │
└──────────────────┬──────────────────┘
                   │  Agregações, Z-Score, Herfindahl
                   ▼
┌─────────────────────────────────────┐
│  OURO  — tabelas analíticas finais  │
│  Todos os "relatórios" = tabelas    │
│  Delta na camada Ouro               │
└─────────────────────────────────────┘
```

---

## Notebooks e Ordem de Execução

```
utils/00_config.py           ← 1º — sempre
utils/00_api_utils.py        ← 2º — sempre

bronze/01_bronze_deputados.py
bronze/02_bronze_frentes.py
bronze/03_bronze_eventos.py
bronze/04_bronze_votacoes.py
bronze/05_bronze_despesas.py
bronze/06_bronze_proposicoes_cpis.py

prata/11_prata_deputados_frentes.py
prata/12_prata_eventos_despesas_scd2.py

ouro/21_gold_frentes_eventos.py
ouro/22_gold_ceap_presenca_correlacao.py
ouro/23_gold_cpis_cdc.py
```

---

## Tabelas Ouro Geradas (entregáveis)

| Tabela | Entregável |
|--------|------------|
| `gold_frentes_membros` | Atlas frentes — membros completos |
| `gold_diversidade_partidaria_frentes` | Índice de Herfindahl |
| `gold_deputados_multi_frentes` | Deputados em mais frentes |
| `gold_sobreposicao_frentes` | Sobreposição entre frentes |
| `gold_evolucao_frentes_legislatura` | Evolução por legislatura |
| `gold_calendario_eventos` | Calendário com dim_orgao, dim_data |
| `gold_presenca_por_deputado_tipo_evento` | Taxa de presença |
| `gold_densidade_eventos_semanal` | Semanas sem atividade |
| `gold_eventos_futuros` | Eventos já agendados |
| `gold_fato_despesas_ceap` | Fato despesas enriquecido |
| `gold_despesas_anomalias` | Z-Score por categoria × UF |
| `gold_ranking_fornecedores` | Ranking com flags de suspeição |
| `gold_relatorio_mensal_gasto_partido` | Top 10 gastos/mês por partido |
| `gold_monitor_engajamento` | Score de engajamento por deputado |
| `gold_relatorio_mensal_engajamento_deputado` | Percentil mensal por deputado |
| `gold_coesao_votacao_frentes` | Correlação frentes × votações |
| `gold_cpis_timeline` | Timeline de CPIs |
| `gold_cpis_proposicoes_derivadas` | PLs derivados de CPIs |
| `gold_cpis_produtividade` | CPIs com/sem relatório final |
| `gold_tempo_medio_tramitacao` | Duração média por órgão |
| `gold_proposicoes_em_plenario` | PLs que chegaram ao Plenário |

---

## Decisões Técnicas

- **PySpark nativo**: toda ingestão usa `spark.createDataFrame()` + `unionByName()`
- **Sem Pandas**: zero conversões `toPandas()` — performance distribuída
- **MERGE INTO SQL**: compatível com Serverless Warehouse
- **SCD Type 2**: `valid_from / valid_to / is_current` via Window Functions
- **Z-Score**: `Window.partitionBy("categoria", "uf")` com `avg()` e `stddev()`
- **Herfindahl**: Σ(share²) calculado via Window + groupBy em PySpark
- **Hash MD5**: chave de carga incremental para despesas (sem ID nativo)
- **`unionByName(allowMissingColumns=True)`**: empilha DataFrames de schemas variáveis
