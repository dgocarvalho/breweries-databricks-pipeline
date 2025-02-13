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
    logger = logging.getLogger('GoldLayer')
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
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp

# Função principal da camada Bronze
def main():
    logger = setup_logging()

    try:
        logger.info("Processo da camada Gold iniciado.")

        # Ler dados da camada Prata
        df_silver = spark.read.parquet("/mnt/datalake/silver/breweries")
        gold_path = "/mnt/datalake/gold/breweries"

        # Agregação: quantidade de cervejarias por tipo e estado
        df_gold = (
            df_silver.groupBy("state", "brewery_type")
            .count()
            .withColumnRenamed("count", "brewery_count")
        )

        # Salvar no formato Parquet
        df_gold.write.format("parquet").mode("overwrite").save(gold_path)

        logger.info("Dados agregados e salvos na camada Ouro!")
        return "Processo da camada Gold concluído com sucesso!"
    
    except Exception as e:
        logger.error(f"Erro ao transformar dados na camada Silver: {e}")
        raise

if __name__ == "__main__":
    result = main()
    dbutils.notebook.exit(result)
