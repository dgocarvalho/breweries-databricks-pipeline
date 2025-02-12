# Databricks notebook source
# Setup da leitura de dados para as camadas
from pyspark.sql import functions as F

# Leitura da camada Bronze (dados brutos)
df_bronze = spark.read.json('/mnt/datalake/bronze/breweries')  # Altere o caminho conforme necessário
display(df_bronze)  # Exibe os dados da camada Bronze

# COMMAND ----------

df_silver = spark.read.parquet("/mnt/datalake/silver/breweries")
display(df_silver)

# COMMAND ----------


import matplotlib.pyplot as plt

# Leitura da camada Gold
df_gold = spark.read.parquet("/mnt/datalake/gold/breweries")
display(df_gold)  # Exibe os dados da camada Gold

# COMMAND ----------

import matplotlib.pyplot as plt

df_gold_pandas = df_gold.toPandas()

# Agrupar os dados por estado e tipo, somando as cervejarias
df_gold_grouped = df_gold_pandas.groupby(['state', 'brewery_type'])['brewery_count'].sum().unstack()

# Ordenar os estados pelo número total de cervejarias (do maior para o menor)
total_breweries_per_state = df_gold_grouped.sum(axis=1)
df_gold_grouped = df_gold_grouped.loc[total_breweries_per_state.sort_values(ascending=False).index]

# Criar o gráfico de barras empilhadas
ax = df_gold_grouped.plot(kind='bar', stacked=True, figsize=(12, 6))

# Personalizar o gráfico
plt.title('Número de Cervejarias por Estado e Tipo')
plt.xlabel('Estado')
plt.ylabel('Número de Cervejarias')
plt.xticks(rotation=45)
plt.tight_layout()  # Ajusta o layout para não cortar labels

# Exibir o gráfico
plt.show()


