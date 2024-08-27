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
    'vendor_performance',
    default_args=default_args,
    description='A DAG to update vendors performance every day',
    schedule_interval='0 5 * * *',  # This line is updated
    start_date=days_ago(1),
    catchup=False,
)

# Define the BigQuery query to be executed
bq_query = """
DELETE FROM `RBBR_PROD.VENDOR_PERFORMANCE` 
WHERE DATA_REF >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'),INTERVAL 7 DAY);

INSERT INTO RBBR_PROD.VENDOR_PERFORMANCE 
WITH
UNICO_VENDEDOR AS
(
  SELECT
    IF(GRP_CLIENTE !='', GRP_CLIENTE, NM_RAZAO_SOCIAL) AS CLIENTE,
    CASE
      WHEN COUNT(DISTINCT NM_EXECUTIVO_CONTAS) = 1 THEN 'SIM'
      ELSE 'NAO'
      END AS UNICO_VENDEDOR
  FROM `RBBR_MAIN.TX_FATURAMENTO`
    GROUP BY 1
),
CLIENTE_ATIVO AS
(
  SELECT DISTINCT
    NM_EXECUTIVO_CONTAS,
    'SIM' AS CLI_ATIVO,
    CLIENTE
  FROM (
        SELECT
          NM_EXECUTIVO_CONTAS,
          DT_EMISSAO,
          IF(GRP_CLIENTE !='', GRP_CLIENTE, NM_RAZAO_SOCIAL) AS CLIENTE,
          ROW_NUMBER() OVER (PARTITION BY IF(GRP_CLIENTE !='', GRP_CLIENTE, NM_RAZAO_SOCIAL) ORDER BY DT_EMISSAO) AS CLIENTE_ATIVO
       FROM `RBBR_MAIN.TX_FATURAMENTO`
       )
        WHERE CLIENTE_ATIVO <= 3
        AND NM_EXECUTIVO_CONTAS != 'VENDAS E-COMMERCE'
)
  SELECT
    IF(
      DG.VENDEDOR IN ('AMANDA LARISSA DO COUTO CAMPOS', 'MAYARA AZEVEDO'),
      'OUTROS',
      DG.VENDEDOR) AS VENDEDOR,
    DG.CLIENTE,
    DG.DATA_REF,
    DG.COD_UF,
    DG.FATURAMENTO,
    DG.UNIDADES_VENDIDAS,
    DG.QUANTIDADE_VENDAS,
    DMC.STATUS,
    UV.UNICO_VENDEDOR,
    IFNULL (CA.CLI_ATIVO,'NAO') AS CARTEIRA_CLIENTE,
    CURRENT_DATE AS UPDATED_DT
  FROM (
        SELECT
          TXF.NM_EXECUTIVO_CONTAS AS VENDEDOR,
          IF(GRP_CLIENTE !='', GRP_CLIENTE, NM_RAZAO_SOCIAL) AS CLIENTE,
          DATE (DT_EMISSAO) AS DATA_REF,
          TXF.COD_UF,
          ROUND(SUM(GMV), 2) AS FATURAMENTO,
          SUM(SI) AS UNIDADES_VENDIDAS,
          COUNT(DISTINCT COD_NFE) AS QUANTIDADE_VENDAS,
        FROM RBBR_MAIN.TX_FATURAMENTO TXF
          WHERE TXF.NM_EXECUTIVO_CONTAS != 'VENDAS E-COMMERCE'
          AND TXF.CAT_CLIENTE != 'VENDA INTERNA'
          AND DATE (DT_EMISSAO) >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'),INTERVAL 7 DAY)
          AND CAST(COD_CFOP AS STRING) NOT IN ('5910','1411','5914','7102','1914','5927','5411','1910','1202','6556','5551','6910','5949','5905','3102','5917','1918','2411','2202','6917','3556','6949')
          AND CAT_CLIENTE != 'INTERCOMPANY'
          AND NM_RAZAO_SOCIAL NOT LIKE '%RBBR%'
          AND NM_RAZAO_SOCIAL NOT LIKE '%WALLY%'
          AND NM_RAZAO_SOCIAL NOT LIKE '%M&S%'
          AND CAT_SITUACAO_NFE = 'EMITIDA'
          AND CAT_COND_FATURAMENTO != 'SEM_DEBITO'
          AND CAT_MOVIMENTO_NFE = 'SAIDA'
            GROUP BY 1,2,3,4
        ) AS DG
LEFT JOIN RBBR_MAIN.DM_CHURN DMC
  ON DG.CLIENTE = DMC.NM_CLIENTE
LEFT JOIN UNICO_VENDEDOR UV
  ON DG.CLIENTE = UV.CLIENTE
LEFT JOIN CLIENTE_ATIVO CA
  ON DG.CLIENTE = CA.CLIENTE
  AND DG.VENDEDOR = CA.NM_EXECUTIVO_CONTAS;
"""

# Define the BigQueryOperator task
vendor_performance = BigQueryExecuteQueryOperator(
    task_id='vendor_performance',
    sql=bq_query,
    use_legacy_sql=False,
    dag=dag,
)

# Set the task sequence
vendor_performance