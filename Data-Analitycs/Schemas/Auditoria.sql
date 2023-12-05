
Welcome to BigQuery! This script will walk you through the 
process of initializing your .bigqueryrc configuration file.

First, we need to set up your credentials if they do not 
already exist.

Setting project_id possible-willow-403216 as the default.

BigQuery configuration complete! Type "bq" to get started.

{
  "creationTime": "1700179338861",
  "etag": "zdxBz+SDUfMpa4bPqRfcVw==",
  "id": "possible-willow-403216:Modelo_Esperanza_De_Vida.Auditoria",
  "kind": "bigquery#table",
  "lastModifiedTime": "1701386674271",
  "location": "US",
  "numActiveLogicalBytes": "1983",
  "numActivePhysicalBytes": "689606",
  "numBytes": "1983",
  "numLongTermBytes": "0",
  "numLongTermLogicalBytes": "0",
  "numLongTermPhysicalBytes": "0",
  "numRows": "17",
  "numTimeTravelPhysicalBytes": "685728",
  "numTotalLogicalBytes": "1983",
  "numTotalPhysicalBytes": "689606",
  "schema": {
    "fields": [
      {
        "mode": "REQUIRED",
        "name": "IdAuditoria",
        "type": "INTEGER"
      },
      {
        "mode": "REQUIRED",
        "name": "NroEjecucion",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "Entorno",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "Proceso",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "Tabla",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "RegLeidos",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "RegInsertados",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "RegActualizados",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "RegEliminados",
        "type": "INTEGER"
      },
      {
        "defaultValueExpression": "CURRENT_DATE()",
        "mode": "NULLABLE",
        "name": "Fecha",
        "type": "DATE"
      },
      {
        "defaultValueExpression": "CURRENT_TIME('UTC-3')",
        "mode": "NULLABLE",
        "name": "Hora",
        "type": "TIME"
      }
    ]
  },
  "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/possible-willow-403216/datasets/Modelo_Esperanza_De_Vida/tables/Auditoria",
  "tableReference": {
    "datasetId": "Modelo_Esperanza_De_Vida",
    "projectId": "possible-willow-403216",
    "tableId": "Auditoria"
  },
  "type": "TABLE"
}
