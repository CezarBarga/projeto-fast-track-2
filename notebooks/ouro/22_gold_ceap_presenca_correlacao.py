# Databricks notebook source
# MAGIC %md
# MAGIC # 22 - Ouro: CEAP, Monitor de Presenca e Correlacao Frentes x Votacoes
# MAGIC
# MAGIC Entregaveis gerados neste notebook:
# MAGIC - gold_fato_despesas_ceap: fato de despesas enriquecido com deputado e fornecedor
# MAGIC - gold_despesas_anomalias: Z-Score por categoria e UF para deteccao de anomalias
# MAGIC - gold_ranking_fornecedores: ranking com flags de suspeicao por fornecedor
# MAGIC - gold_relatorio_mensal_gasto_partido: top 10 partidos por gasto mensal
# MAGIC - gold_monitor_engajamento: score composto de engajamento por deputado
# MAGIC - gold_relatorio_mensal_engajamento_deputado: percentil mensal por deputado
# MAGIC - gold_coesao_votacao_frentes: indice de coesao de voto por frente parlamentar

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# Carrega dimensoes e fatos da Prata necessarios para o Raio-X CEAP
df_dep  = read_prata("dim_deputado")
df_desp = read_prata("fato_despesas")
df_forn = read_prata("dim_fornecedor")

# COMMAND ----------

# MAGIC %md ## 3 — Raio-X CEAP

# COMMAND ----------

# DBTITLE 1,Tabela: gold_fato_despesas_ceap

# Enriquece o fato de despesas com dados do deputado e do fornecedor
# Serve como base para as analises de anomalia, ranking e relatorio mensal
df_ceap = (
    df_desp
    .join(df_dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf"), "id_deputado", "left")
    .join(df_forn, "cnpj_cpf_fornecedor", "left")
)
save_ouro(df_ceap, "gold_fato_despesas_ceap")
print(f"gold_fato_despesas_ceap: {df_ceap.count()} registros")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_despesas_anomalias (Z-Score por categoria x UF)

# Detecta anomalias nas despesas usando Z-Score por categoria de despesa e UF do deputado
# Z-Score = (valor - media) / desvio_padrao
# Valores com |Z-Score| acima do threshold configurado sao classificados como anomalia
# Decisao: Z-Score calculado com Window Functions do PySpark, sem dependencias externas
# A granularidade por categoria e UF elimina vieses regionais e setoriais

w_cat_uf = Window.partitionBy("desc_categoria", "sigla_uf")

df_zscore = (
    df_ceap
    # Calcula media e desvio padrao por categoria de despesa e UF do deputado
    .withColumn("media_cat_uf", F.avg("valor_liquido").over(w_cat_uf))
    .withColumn("std_cat_uf",   F.stddev("valor_liquido").over(w_cat_uf))
    # Z-Score: zero quando desvio padrao e zero (categoria com valor unico)
    .withColumn("z_score",
        F.when(F.col("std_cat_uf") > 0,
            (F.col("valor_liquido") - F.col("media_cat_uf")) / F.col("std_cat_uf"))
        .otherwise(F.lit(0.0)))
    # Classifica anomalia pelo valor absoluto do Z-Score
    .withColumn("is_anomalia", F.abs(F.col("z_score")) > F.lit(ZSCORE_THRESHOLD))
    .withColumn("nivel_anomalia",
        F.when(F.abs(F.col("z_score")) > 5, "CRITICO")
        .when(F.abs(F.col("z_score")) > 3,  "ALTO")
        .otherwise("NORMAL"))
)
save_ouro(df_zscore, "gold_despesas_anomalias")
print(f"Anomalias detectadas: {df_zscore.filter('is_anomalia').count()}")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_ranking_fornecedores

# Ranking de fornecedores mais pagos com flags de suspeicao
# Flag PF alto valor: pessoa fisica (CPF) que recebeu mais de R$ 50.000 no total
# Flag monopolio: fornecedor que atende exclusivamente um unico deputado
# Score de suspeicao: soma dos flags (0 = sem flags, 2 = ambos os flags ativos)
df_rank_forn = (
    df_ceap
    .groupBy("cnpj_cpf_fornecedor", "nome_fornecedor", "is_pessoa_fisica")
    .agg(
        F.sum("valor_liquido").alias("total_recebido"),
        F.count("id_despesa").alias("total_notas"),
        F.countDistinct("id_deputado").alias("total_deputados"),
        F.countDistinct("sigla_partido").alias("total_partidos"),
    )
    .withColumn("media_por_nota", F.round(F.col("total_recebido") / F.col("total_notas"), 2))
    # Pessoa fisica com total acima de R$ 50.000 pode indicar relacao suspeita
    .withColumn("flag_pf_alto_valor",
        F.col("is_pessoa_fisica") & (F.col("total_recebido") > 50000))
    # Fornecedor que atende apenas um deputado pode indicar vinculo exclusivo
    .withColumn("flag_monopolio", F.col("total_deputados") == 1)
    # Score agregado: cada flag ativo incrementa o score em 1
    .withColumn("score_suspeicao",
        F.col("flag_pf_alto_valor").cast("int") + F.col("flag_monopolio").cast("int"))
    .orderBy(F.desc("total_recebido"))
)
save_ouro(df_rank_forn, "gold_ranking_fornecedores")
print(f"gold_ranking_fornecedores: {df_rank_forn.count()} fornecedores")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_relatorio_mensal_gasto_partido

# Relatorio mensal com os top 10 partidos por gasto da CEAP em cada mes
# Usa RANK() particionado por ano e mes para selecionar os 10 maiores gastos
df_mensal = (
    df_ceap
    .groupBy("sigla_partido", "ano", "mes")
    .agg(
        F.sum("valor_liquido").alias("total_gasto"),
        F.count("id_despesa").alias("total_notas"),
        F.countDistinct("id_deputado").alias("total_deputados"),
        F.avg("valor_liquido").alias("media_por_nota"),
    )
    .withColumn("rank_mes",
        F.rank().over(Window.partitionBy("ano", "mes").orderBy(F.desc("total_gasto"))))
    .filter(F.col("rank_mes") <= 10)
    .orderBy("ano", "mes", "rank_mes")
)
save_ouro(df_mensal, "gold_relatorio_mensal_gasto_partido")
print("CEAP -- 4 tabelas Ouro geradas")

# COMMAND ----------

# MAGIC %md ## 4 — Monitor de Presenca e Absenteismo

# COMMAND ----------

# Carrega dados de votacoes e eventos para calcular o score de engajamento
df_votos    = read_bronze("votacoes_votos")
df_votacoes = read_bronze("votacoes_lista")
df_ev       = read_prata("fato_eventos")

# Totais usados para normalizar os percentuais de presenca e participacao
total_votacoes = max(df_votacoes.count(), 1)
total_eventos  = max(df_ev.count(), 1)

# COMMAND ----------

# DBTITLE 1,Tabela: gold_monitor_engajamento

# Score de engajamento composto por dois indicadores normalizados:
# - Presenca em eventos: percentual de eventos em que o deputado esteve presente (peso 40%)
# - Participacao em votacoes: percentual de votacoes em que o deputado votou (peso 60%)
# O percentil classifica o deputado em relacao aos demais (ALTO, MEDIO, BAIXO, CRITICO)

df_pres_dep = (
    read_bronze("eventos_presenca")
    .groupBy(F.col("id").alias("id_deputado"))
    .agg(F.count("_evento_id").alias("eventos_presentes"))
)

df_vot_dep = (
    df_votos
    .groupBy("id_deputado")
    .agg(
        F.count("_votacao_id").alias("total_votacoes_participadas"),
        # Conta ausencias usando o campo tipoVoto da Bronze
        F.sum(F.when(F.col("tipoVoto") == "Ausente", 1).otherwise(0)).alias("ausencias_votacao"),
    )
)

df_eng = (
    df_dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf")
    .join(df_pres_dep, "id_deputado", "left")
    .join(df_vot_dep,  "id_deputado", "left")
    .fillna(0)
    # Percentual de presenca em eventos (em relacao ao total de eventos)
    .withColumn("perc_presenca_eventos",
        F.round(F.col("eventos_presentes") / F.lit(total_eventos) * 100, 2))
    # Percentual de participacao em votacoes (em relacao ao total de votacoes)
    .withColumn("perc_participacao_votacoes",
        F.round(F.col("total_votacoes_participadas") / F.lit(total_votacoes) * 100, 2))
    # Score composto: presenca (40%) + votacoes (60%)
    .withColumn("score_engajamento",
        F.round(
            F.col("perc_presenca_eventos") / 100 * 0.4 +
            F.col("perc_participacao_votacoes") / 100 * 0.6,
            4))
    # Percentil em relacao aos demais deputados (Window sem particao = ranking global)
    .withColumn("percentil_engajamento",
        F.round(F.percent_rank().over(
            Window.partitionBy(F.lit(1)).orderBy("score_engajamento")) * 100, 1))
    .withColumn("nivel_engajamento",
        F.when(F.col("percentil_engajamento") >= 75, "ALTO")
        .when(F.col("percentil_engajamento") >= 50, "MEDIO")
        .when(F.col("percentil_engajamento") >= 25, "BAIXO")
        .otherwise("CRITICO"))
    .orderBy(F.desc("score_engajamento"))
)
save_ouro(df_eng, "gold_monitor_engajamento")
print(f"gold_monitor_engajamento: {df_eng.count()} deputados")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_relatorio_mensal_engajamento_deputado

# Serie temporal mensal de engajamento por deputado
# Calcula taxa de presenca em votacoes e percentil em relacao a media do mes
# Permite identificar quedas de engajamento apos eventos criticos
df_serie = (
    df_votos
    .join(df_votacoes.select(
            F.col("id").alias("_votacao_id"),
            F.to_date("data").alias("data_votacao")), "_votacao_id", "left")
    .filter(F.col("data_votacao").isNotNull())
    .groupBy("id_deputado", F.year("data_votacao").alias("ano"), F.month("data_votacao").alias("mes"))
    .agg(
        F.count("_votacao_id").alias("votacoes_mes"),
        F.sum(F.when(F.col("tipoVoto") == "Ausente", 1).otherwise(0)).alias("ausencias_mes"),
    )
    # Taxa de presenca: (votacoes - ausencias) / total votacoes do mes
    .withColumn("taxa_presenca_pct",
        F.round((F.col("votacoes_mes") - F.col("ausencias_mes")) / F.col("votacoes_mes") * 100, 2))
    .join(df_dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf"), "id_deputado", "left")
    # Percentil do deputado em relacao a media do mesmo mes
    .withColumn("percentil_mes",
        F.round(F.percent_rank().over(
            Window.partitionBy("ano", "mes").orderBy("taxa_presenca_pct")) * 100, 1))
)
save_ouro(df_serie, "gold_relatorio_mensal_engajamento_deputado")
print("Presenca -- 2 tabelas Ouro geradas")

# COMMAND ----------

# MAGIC %md ## 5 — Correlacao Frentes x Votacoes

# COMMAND ----------

# DBTITLE 1,Tabela: gold_coesao_votacao_frentes

# Mede se deputados de uma mesma frente votam de forma mais alinhada do que seus colegas de partido
# Metodologia:
# 1. Para cada frente e cada votacao, conta votos Sim e Nao dos membros da frente
# 2. Identifica o voto majoritario da frente em cada votacao
# 3. Calcula o share do voto majoritario (proporcao de membros que votaram com a maioria)
# 4. Media do share por frente = indice de coesao (quanto mais alto, mais alinhada a frente)

df_membro = read_prata("fato_frente_membro")

# Window para identificar o voto mais votado por frente e votacao
w_vot = Window.partitionBy("id_frente", "_votacao_id").orderBy(F.desc("n_voto"))

df_coesao = (
    df_votos
    # Considera apenas votos validos (Sim ou Nao), excluindo ausencias e abstencoes
    .filter(F.col("tipoVoto").isin(["Sim", "Nao"]))
    # Restringe aos deputados que sao membros de alguma frente
    .join(df_membro.select("id_deputado", "id_frente"), "id_deputado")
    # Conta votos por tipo para cada frente e votacao
    .groupBy("id_frente", "_votacao_id", "tipoVoto")
    .agg(F.count("id_deputado").alias("n_voto"))
    # Calcula total de votos validos por frente e votacao
    .withColumn("total_votacao",
        F.sum("n_voto").over(Window.partitionBy("id_frente", "_votacao_id")))
    # Share do voto: proporcao de membros que votaram de determinada forma
    .withColumn("share_voto", F.col("n_voto") / F.col("total_votacao"))
    # Seleciona apenas o voto majoritario (maior share) por frente e votacao
    .withColumn("rn", F.row_number().over(w_vot))
    .filter(F.col("rn") == 1)
    # Media do share do voto majoritario por frente = indice de coesao
    .groupBy("id_frente")
    .agg(F.avg("share_voto").alias("coesao_media"))
    .withColumn("coesao_media", F.round(F.col("coesao_media"), 4))
    .join(read_prata("dim_frente").select("id_frente", "titulo"), "id_frente", "left")
    .orderBy(F.desc("coesao_media"))
)
save_ouro(df_coesao, "gold_coesao_votacao_frentes")
print("Correlacao Frentes x Votacoes -- 1 tabela Ouro gerada")
