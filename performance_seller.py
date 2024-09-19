from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG, its parameters, and schedule
dag = DAG(
    'performance_seller',
    default_args=default_args,
    description='A DAG to update performance seller every day',
    schedule_interval='0 5 * * *',  # This line is updated
    start_date=days_ago(1),
    catchup=False,
)

# Define the BigQuery query to be executed
bq_query = """
CREATE OR REPLACE TABLE `RBBR_PROD.PERFORMANCE_SELLER` AS (
WITH DB_FATURAMENTO AS (
SELECT 
    CASE WHEN CLIENTE_SELLOUT IS NOT NULL THEN CLIENTE_SELLOUT
    WHEN FAT.NM_EXECUTIVO_CONTAS = 'VENDAS E-COMMERCE' THEN 'E-COMMERCE'
    WHEN FAT.NM_EXECUTIVO_CONTAS = 'FEIRA' THEN 'FEIRA'
    WHEN GRP_CLIENTE != '' THEN GRP_CLIENTE
    ELSE NM_FANTASIA END AS CLIENTE
  , DATE_TRUNC(DATE (DT_EMISSAO), MONTH) AS DATA_REF
  , STATUS
  , ROUND(SUM(GMV), 2) AS FATURAMENTO
  , SUM(SI) AS UNIDADES_VENDIDAS
  , COUNT(DISTINCT COD_NFE) AS QUANTIDADE_VENDAS
  , SUM(VL_DESCONTO) AS VL_DESCONTO
  FROM `RBBR_MAIN.TX_FATURAMENTO` FAT
  LEFT JOIN `RBBR_PROD.INFO_DEPARA_FATURAMENTO_SELLOUT` SELL
  ON FAT.COD_CLIENTE = SELL.COD_CLIENTE
    LEFT JOIN `RBBR_MAIN.DM_CHURN` DMC
  ON IF(FAT.GRP_CLIENTE != '', FAT.GRP_CLIENTE, FAT.NM_RAZAO_SOCIAL) = DMC.NM_CLIENTE


  WHERE CAST(COD_CFOP AS STRING) NOT IN ('5910','1411','5914','7102','1914','5927','5411','1910','1202',  '6556','5551','6910','5949','5905','3102','5917','1918','2411','2202','6917','3556','6949') 
        AND CAT_CLIENTE != 'INTERCOMPANY' 
        AND NM_RAZAO_SOCIAL NOT LIKE '%RBBR%'
        AND NM_RAZAO_SOCIAL NOT LIKE '%WALLY%'
        AND NM_RAZAO_SOCIAL NOT LIKE '%M&S%'
        AND CAT_SITUACAO_NFE = 'EMITIDA' 
        AND CAT_COND_FATURAMENTO != 'SEM_DEBITO'
        AND CAT_MOVIMENTO_NFE = 'SAIDA'
  GROUP BY 1,2, 3
),

MELHOR_MES AS (
   SELECT  
      CLIENTE
    , MES
      FROM ( SELECT 
        CLIENTE,
        EXTRACT(MONTH FROM DATA_REF) AS MES,
        SUM(FATURAMENTO) AS FATURAMENTO_TOTAL,
        ROW_NUMBER() OVER (PARTITION BY CLIENTE ORDER BY SUM(FATURAMENTO) DESC) AS RN
    FROM DB_FATURAMENTO
      GROUP BY 1,2)
      WHERE RN = 1
),

SELLOUT AS (
  SELECT
      DATA_REF
    , CASE
        WHEN GMV_SELLOUT IS NULL THEN 'NÃO'
        ELSE 'SIM'
      END AS ATIVO
    , NOME_SELLER AS CLIENTE
    , SUM(GMV_SELLOUT) AS FATURAMENTO_SELLOUT
    , SUM(SI_SELLOUT) AS UNIDADES_VENDIDAS_SELLOUT
    FROM `RBBR_MAIN.DM_SELLOUT_MENSAL`
    GROUP BY 1,2,3
)

SELECT 
      CLIENTE
    , DATA_REF
    , IFNULL(STATUS, 'SEM STATUS') AS STATUS
    , MES AS MELHOR_MES
    , FATURAMENTO
    , UNIDADES_VENDIDAS
    , QUANTIDADE_VENDAS
    , VL_DESCONTO
    , IFNULL(FATURAMENTO_SELLOUT, 0) AS FATURAMENTO_SELLOUT
    , IFNULL(UNIDADES_VENDIDAS, 0) AS UNID_VENDIDAS_SELLOUT
    , IFNULL(ATIVO, 'NÃO') AS SELLOUT
  FROM DB_FATURAMENTO DB_FAT
  
  FULL JOIN MELHOR_MES MM USING (CLIENTE)

  FULL JOIN SELLOUT  USING (DATA_REF,CLIENTE))
  
"""

# Define the BigQueryOperator task
performance_seller = BigQueryExecuteQueryOperator(
    task_id='performance_seller',
    sql=bq_query,
    use_legacy_sql=False,
    dag=dag,
)

# Set the task sequence
performance_seller