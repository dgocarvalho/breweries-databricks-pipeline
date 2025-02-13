# Databricks notebook source
import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logging():
    log_file_path="/dbfs/tmp/pipeline_logs.log"

    # Criar o diretório, se não existir
    log_dir = os.path.dirname(log_file_path)
    os.makedirs(log_dir, exist_ok=True)

    # Criar um logger
    logger = logging.getLogger('BronzeLayer')
    logger.setLevel(logging.INFO)

    # Criar um handler 
    handler = logging.FileHandler(log_file_path)
    handler.setLevel(logging.INFO)

    # Formato do log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Adicionar handlers ao logger
    logger.addHandler(handler)

    return logger


# COMMAND ----------

import logging
from pyspark.sql import SparkSession
import requests
import json

# Função principal da camada Bronze
def main():
    logger = setup_logging()

    try:
        logger.info("Processo da camada Bronze iniciado.")

        api_url = "https://api.openbrewerydb.org/breweries"
        bronze_path = "/mnt/datalake/bronze/breweries"

        # Busca dados da API
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        # Cria SparkSession
        spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

        # Converte dados para DataFrame e salva em JSON
        raw_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
        raw_df.coalesce(1).write.mode("overwrite").json(bronze_path)
        
        logger.info(f"Dados da camada Bronze salvos em: {bronze_path}")
        return f"Dados brutos salvos em {bronze_path}"
    
    except Exception as e:
        logger.error(f"Erro ao obter dados na camada Bronze: {e}")
        raise

if __name__ == "__main__":
    result = main()
    dbutils.notebook.exit(result)

