# Databricks notebook source
# MAGIC %md
# MAGIC # 21 - Ouro: Atlas das Frentes e Calendario de Eventos
# MAGIC Todos os resultados sao tabelas Delta na camada Ouro.
# MAGIC
# MAGIC Entregaveis gerados neste notebook:
# MAGIC - gold_frentes_membros: visao completa de frentes com membros, partido e UF
# MAGIC - gold_diversidade_partidaria_frentes: indice de Herfindahl por frente
# MAGIC - gold_deputados_multi_frentes: deputados com maior participacao em frentes
# MAGIC - gold_sobreposicao_frentes: pares de frentes com membros em comum
# MAGIC - gold_evolucao_frentes_legislatura: quantidade de frentes por legislatura
# MAGIC - gold_calendario_eventos: eventos com dimensoes de orgao e data
# MAGIC - gold_presenca_por_deputado_tipo_evento: taxa de presenca por deputado
# MAGIC - gold_densidade_eventos_semanal: volume de eventos por semana
# MAGIC - gold_eventos_futuros: eventos ja agendados com data futura

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# Carrega as dimensoes e fatos da camada Prata necessarios para este notebook
df_frente  = read_prata("dim_frente")
df_membro  = read_prata("fato_frente_membro")
df_deputado = read_prata("dim_deputado")

# COMMAND ----------

# MAGIC %md ## 1 — Atlas das Frentes Parlamentares

# COMMAND ----------

# DBTITLE 1,Tabela: gold_frentes_membros

# Visao desnormalizada de frentes com todos os membros, partido, UF e legislatura
# Une fato_frente_membro com dim_frente e dim_deputado para enriquecer os dados
df_gold_frentes = (
    df_membro
    .join(df_frente.select("id_frente", "titulo", "id_legislatura", "tema_resumo"), "id_frente", "left")
    .join(df_deputado.select("id_deputado", "sigla_uf"), "id_deputado", "left")
    .select("id_frente", "titulo", "id_legislatura", "tema_resumo",
            "id_deputado", "nome_deputado", "sigla_partido", "sigla_uf", "titulo_membro")
)
save_ouro(df_gold_frentes, "gold_frentes_membros")
print(f"gold_frentes_membros: {df_gold_frentes.count()} registros")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_diversidade_partidaria_frentes (Indice de Herfindahl)

# O Indice de Herfindahl-Hirschman (HHI) mede concentracao partidaria nas frentes
# HHI = soma dos quadrados das participacoes de cada partido na frente
# Diversidade = 1 - HHI (0 = monopolio de um partido, ~1 = maxima diversidade)
# Decisao: metrica consagrada em economia, interpretavel e defensavel

w_fr = Window.partitionBy("id_frente")

df_hhi = (
    df_membro
    # Conta membros por frente e partido
    .groupBy("id_frente", "sigla_partido")
    .agg(F.count("id_deputado").alias("n_partido"))
    # Calcula participacao relativa de cada partido na frente
    .withColumn("total_frente", F.sum("n_partido").over(w_fr))
    .withColumn("share",        F.col("n_partido") / F.col("total_frente"))
    # Quadrado da participacao para compor o HHI
    .withColumn("share2",       F.col("share") * F.col("share"))
    # Agrega o HHI e calcula o indice de diversidade por frente
    .groupBy("id_frente")
    .agg(
        F.sum("share2").alias("hhi"),
        F.count("sigla_partido").alias("n_partidos"),
        F.sum("n_partido").alias("total_membros"),
    )
    .withColumn("indice_diversidade", F.round(1 - F.col("hhi"), 4))
    .join(df_frente.select("id_frente", "titulo", "id_legislatura"), "id_frente", "left")
    .orderBy(F.desc("indice_diversidade"))
)
save_ouro(df_hhi, "gold_diversidade_partidaria_frentes")
print(f"gold_diversidade_partidaria_frentes: {df_hhi.count()} registros")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_deputados_multi_frentes

# Identifica deputados que participam de mais frentes e seus temas de interesse
# O campo lista_frentes agrega os titulos de todas as frentes do deputado
df_multi = (
    df_membro
    .join(df_frente.select("id_frente", "titulo"), "id_frente", "left")
    .groupBy("id_deputado", "nome_deputado", "sigla_partido", "sigla_uf")
    .agg(
        F.count("id_frente").alias("total_frentes"),
        F.collect_set("titulo").alias("lista_frentes"),
    )
    .orderBy(F.desc("total_frentes"))
)
save_ouro(df_multi, "gold_deputados_multi_frentes")
print(f"gold_deputados_multi_frentes: {df_multi.count()} registros")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_sobreposicao_frentes

# Identifica pares de frentes que compartilham membros em comum
# Util para detectar deputados em frentes ideologicamente opostas
# O self-join com filtro frente_a < frente_b evita pares duplicados (A,B) e (B,A)
df_membro_titulo = df_membro.join(
    df_frente.select("id_frente", "titulo"), "id_frente", "left"
)

df_sob = (
    df_membro_titulo.select("id_deputado", F.col("id_frente").alias("frente_a"), F.col("titulo").alias("titulo_a"))
    .join(
        df_membro_titulo.select("id_deputado", F.col("id_frente").alias("frente_b"), F.col("titulo").alias("titulo_b")),
        "id_deputado"
    )
    .filter(F.col("frente_a") < F.col("frente_b"))
    .groupBy("frente_a", "frente_b", "titulo_a", "titulo_b")
    .agg(F.count("id_deputado").alias("membros_em_comum"))
    # Filtra pares com pelo menos 3 membros em comum para reduzir ruido
    .filter(F.col("membros_em_comum") > 2)
    .orderBy(F.desc("membros_em_comum"))
)
save_ouro(df_sob, "gold_sobreposicao_frentes")
print(f"gold_sobreposicao_frentes: {df_sob.count()} registros")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_evolucao_frentes_legislatura

# Conta o numero de frentes por legislatura para analisar a evolucao tematica
df_evolucao = (
    df_frente
    .groupBy("id_legislatura")
    .agg(F.count("id_frente").alias("total_frentes"))
    .orderBy("id_legislatura")
)
save_ouro(df_evolucao, "gold_evolucao_frentes_legislatura")
print(f"Atlas das Frentes -- 5 tabelas Ouro geradas")

# COMMAND ----------

# MAGIC %md ## 2 — Calendario Analitico de Eventos

# COMMAND ----------

# Carrega dimensoes necessarias para o calendario de eventos
df_ev   = read_prata("fato_eventos")
df_org  = read_prata("dim_orgao")
df_data = read_prata("dim_data")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_calendario_eventos

# Visao consolidada de eventos com dimensoes de orgao, tipo e data
# Permite analises de densidade, comparativos eleitorais e calendario futuro
df_cal = (
    df_ev
    .join(df_org,  "id_orgao", "left")
    .join(df_data, df_ev["data_evento"] == df_data["data_evento"], "left")
    .select(
        "id_evento", "descricao", "situacao", "local",
        "id_orgao", "nome_orgao", "sigla_orgao", "desc_tipo_evento",
        df_ev["data_evento"], "dt_inicio", "dt_fim",
        "ano", "mes", "semana_ano", "trimestre", "is_fds", "urlRegistro",
    )
)
save_ouro(df_cal, "gold_calendario_eventos")
print(f"gold_calendario_eventos: {df_cal.count()} registros")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_presenca_por_deputado_tipo_evento

# Taxa de presenca de cada deputado por tipo de evento ao longo do ano
# O ano e derivado da data do evento via join com fato_eventos
try:
    df_pres = read_bronze("eventos_presenca")
    df_dep  = read_prata("dim_deputado")

    df_pres_gold = (
        df_pres
        .join(
            df_ev.select("id_evento", "desc_tipo_evento", "data_evento"),
            df_pres["_evento_id"] == df_ev["id_evento"], "left"
        )
        .withColumn("ano", F.year("data_evento"))
        .groupBy(F.col("id").alias("id_deputado"), "desc_tipo_evento", "ano")
        .agg(F.count("_evento_id").alias("total_presencas"))
        .join(df_dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf"), "id_deputado", "left")
        .orderBy(F.desc("total_presencas"))
    )
    save_ouro(df_pres_gold, "gold_presenca_por_deputado_tipo_evento")
    print(f"gold_presenca_por_deputado_tipo_evento: {df_pres_gold.count()} registros")
except Exception as e:
    print(f"Presenca nao disponivel: {e}")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_densidade_eventos_semanal

# Densidade de eventos por semana do ano para identificar periodos sem atividade
# O campo sem_atividade sinaliza semanas com zero eventos legislativos
df_dens = (
    df_cal
    .groupBy("ano", "semana_ano")
    .agg(F.count("id_evento").alias("total_eventos"))
    .withColumn("sem_atividade", F.col("total_eventos") == 0)
    .orderBy("ano", "semana_ano")
)
save_ouro(df_dens, "gold_densidade_eventos_semanal")
print(f"gold_densidade_eventos_semanal: {df_dens.count()} registros")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_eventos_futuros

# Eventos ja agendados com data de inicio futura e nao cancelados
# Serve como calendario publico de eventos legislativos proximos
df_futuros = (
    df_cal
    .filter(F.col("dt_inicio") > F.current_timestamp())
    .filter(F.col("situacao") != "Cancelada")
    .orderBy("dt_inicio")
)
save_ouro(df_futuros, "gold_eventos_futuros")
print(f"Calendario -- 4 tabelas Ouro geradas | Eventos futuros: {df_futuros.count()}")
