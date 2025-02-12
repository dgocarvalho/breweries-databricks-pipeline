Este repositório contém um pipeline ETL desenvolvido no **Databricks**, seguindo a estrutura **Delta Lake** com as camadas **Bronze, Silver e Gold**.

## Estrutura do Repositório
`databricks/pipeline/src` - Contém os notebooks do pipeline  

## Tecnologias Utilizadas
- Databricks Community Edition
- Apache Spark
- PySpark
- Matplotlib (para gráficos)
- Delta Lake

## Pipeline de Dados
- **Bronze Layer**: Ingestão de dados brutos
- **Silver Layer**: Limpeza e transformação inicial
- **Gold Layer**: Dados agregados e prontos para análise

## Como Executar
1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/databricks-pipeline.git
