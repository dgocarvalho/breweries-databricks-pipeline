# Databricks notebook source
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

def setup_logging():

    log_file_path="/dbfs/tmp/pipeline_logs.log"

    # Criar o diretório, se não existir
    log_dir = os.path.dirname(log_file_path)
    dbutils.fs.mkdirs(log_dir.replace("/dbfs", "dbfs:")) 
    
    # Remover handlers antigos para evitar duplicação
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Criar um logger
    logger = logging.getLogger("pipeline_logs")
    logger.setLevel(logging.INFO)

    # Criar um handler que rotaciona o log diariamente
    handler = TimedRotatingFileHandler(log_file_path, when="midnight", interval=1, backupCount=7)
    handler.suffix = "%Y-%m-%d"  # Nomeia arquivos com a data (ex: pipeline_logs_2025-02-12.log)

    # Formato do log
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    handler.setFormatter(formatter)

    # Adicionar handlers ao logger
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))  # Exibir logs no console

    return logger


# COMMAND ----------

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp

# Função principal da camada Bronze
def main():
    logger = setup_logging()

    # Executa o notebook de configuração
    logger.info("Processo Silver iniciado.")

    # Inicializar SparkSession
    spark = SparkSession.builder.appName("Camada Silver").getOrCreate()

    # Caminhos para os dados
    bronze_path = "/mnt/datalake/bronze/breweries"
    silver_path = "/mnt/datalake/silver/breweries"

    # Ler dados brutos da camada Bronze
    try:
        df_bronze = spark.read.format("json").load(bronze_path)
        logger.info("Dados carregados da camada Bronze com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao carregar os dados da camada Bronze: {e}")
        raise

    try:
        # Monitorar campos críticos
        critical_fields = ["brewery_type", "city", "country", "name", "state", "website_url", "address_1", "phone"]
        for field in critical_fields:
            null_count = df_bronze.filter(col(field).isNull()).count()
            if null_count > 0:
                logger.warning(f"Campo {field} possui {null_count} valores nulos.")

        # Criar uma coluna indicando a completude dos campos críticos
        df_bronze = df_bronze.withColumn(
            "missing_critical_fields",
            when(
                col("brewery_type").isNull() | col("city").isNull() | col("country").isNull() |
                col("name").isNull() | col("state").isNull() | col("website_url").isNull() |
                col("address_1").isNull() | col("phone").isNull(),
                lit(True)
            ).otherwise(lit(False))
        )

        # Limpeza e transformação dos dados
        df_silver = (
            df_bronze
                .filter(col("state").isNotNull())  # Remover entradas sem estado
                .withColumn("brewery_type", when(col("brewery_type").isNull(), lit("unknown")).otherwise(col("brewery_type")))
                .withColumn("ingestion_date", current_timestamp())  # Substituir nulos por "unknown"
        )

        # Salvar os dados transformados
        df_silver.write.format("parquet").mode("overwrite").partitionBy("state").save(silver_path)
        logger.info(f"Dados da camada Silver salvos em: {silver_path}")

    except Exception as e:
        logger.error(f"Erro ao transformar dados na camada Silver: {e}")
        raise

    return "Processo concluído com sucesso!"

if __name__ == "__main__":
    result = main()
    dbutils.notebook.exit(result)

