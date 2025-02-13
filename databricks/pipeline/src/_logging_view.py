# Databricks notebook source
#with open('/dbfs/tmp/pipeline_logs.log', 'w'):
#    pass  # Apenas abre e fecha o arquivo, limpando o conteúdo

# COMMAND ----------

import os

log_file_path = "/dbfs/tmp/pipeline_logs.log"  # Caminho do arquivo de log

# Verifique se o arquivo de log existe
if os.path.exists(log_file_path):
    # Abra e leia o arquivo de log
    with open(log_file_path, 'r') as file:
        log_content = file.read()
    # Exiba o conteúdo do log
    print(log_content)
else:
    print(f"O arquivo de log '{log_file_path}' não foi encontrado.")

