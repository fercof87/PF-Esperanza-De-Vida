{
  "creationTime": "1701024258234",
  "etag": "F42khG6QeucLGh9t6YHUHQ==",
  "id": "possible-willow-403216:Modelo_Esperanza_De_Vida.Comparacion_Periodo_Anterior_Indicadores_x5",
  "kind": "bigquery#table",
  "lastModifiedTime": "1701343737237",
  "location": "US",
  "numActiveLogicalBytes": "0",
  "numBytes": "0",
  "numLongTermBytes": "0",
  "numLongTermLogicalBytes": "0",
  "numRows": "0",
  "numTotalLogicalBytes": "0",
  "schema": {
    "fields": [
      {
        "mode": "NULLABLE",
        "name": "IdPais",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "IdIndicador",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "Indicador",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "PeriodoActualInicio",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "PeriodoActualFin",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "PeriodoAnteriorInicio",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "PeriodoAnteriorFin",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "PromedioValorActual",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "PromedioValorAnterior",
        "type": "FLOAT"
      }
    ]
  },
  "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/possible-willow-403216/datasets/Modelo_Esperanza_De_Vida/tables/Comparacion_Periodo_Anterior_Indicadores_x5",
  "tableReference": {
    "datasetId": "Modelo_Esperanza_De_Vida",
    "projectId": "possible-willow-403216",
    "tableId": "Comparacion_Periodo_Anterior_Indicadores_x5"
  },
  "type": "VIEW",
  "view": {
    "query": "WITH PeriodData AS (\r\n  SELECT\r\n    IdPais,\r\n    IdIndicador,\r\n    Anio,\r\n    Valor,\r\n    ROW_NUMBER() OVER (PARTITION BY IdPais, IdIndicador ORDER BY Anio DESC) AS RowNum\r\n  FROM\r\n    `possible-willow-403216.Modelo_Esperanza_De_Vida.DatosIndicador`\r\n)\r\n\r\nSELECT\r\n  pd.IdPais,\r\n  pd.IdIndicador,\r\n  i.Descripcion as Indicador,\r\n  MIN(IF(pd.RowNum <= 5, pd.Anio, NULL)) AS PeriodoActualInicio,\r\n  MAX(IF(pd.RowNum <= 5, pd.Anio, NULL)) AS PeriodoActualFin,\r\n  MIN(IF(pd.RowNum > 5 AND pd.RowNum <= 10, pd.Anio, NULL)) AS PeriodoAnteriorInicio,\r\n  MAX(IF(pd.RowNum > 5 AND pd.RowNum <= 10, pd.Anio, NULL)) AS PeriodoAnteriorFin,\r\n  AVG(IF(pd.RowNum <= 5, pd.Valor, NULL)) AS PromedioValorActual,\r\n  AVG(IF(pd.RowNum > 5 AND pd.RowNum <= 10, pd.Valor, NULL)) AS PromedioValorAnterior\r\nFROM\r\n  PeriodData as pd\r\nJOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Indicador` as i\r\n  ON pd.IdIndicador = i.IdIndicador\r\nWHERE\r\n  pd.RowNum <= 10  -- Considera solo los \u00faltimos 10 a\u00f1os\r\nGROUP BY\r\n  pd.IdPais,\r\n  pd.IdIndicador, \r\n  i.Descripcion;\r\n",
    "useLegacySql": false
  }
}
