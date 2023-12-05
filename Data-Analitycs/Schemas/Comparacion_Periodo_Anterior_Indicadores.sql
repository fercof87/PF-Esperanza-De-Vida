{
  "creationTime": "1701023364972",
  "etag": "XC8rg1iUrgMjTlLbXsMjiQ==",
  "id": "possible-willow-403216:Modelo_Esperanza_De_Vida.Comparacion_Periodo_Anterior_Indicadores",
  "kind": "bigquery#table",
  "lastModifiedTime": "1701343723464",
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
        "name": "AnioActual",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "ValorActual",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "AnioAnterior",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "ValorAnterior",
        "type": "FLOAT"
      }
    ]
  },
  "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/possible-willow-403216/datasets/Modelo_Esperanza_De_Vida/tables/Comparacion_Periodo_Anterior_Indicadores",
  "tableReference": {
    "datasetId": "Modelo_Esperanza_De_Vida",
    "projectId": "possible-willow-403216",
    "tableId": "Comparacion_Periodo_Anterior_Indicadores"
  },
  "type": "VIEW",
  "view": {
    "query": "WITH RankedData AS (\r\n  SELECT\r\n    IdPais,\r\n    IdIndicador,\r\n    Anio,\r\n    Valor,\r\n    ROW_NUMBER() OVER (PARTITION BY IdPais, IdIndicador ORDER BY Anio DESC) AS RowNum\r\n  FROM\r\n    `possible-willow-403216.Modelo_Esperanza_De_Vida.DatosIndicador`\r\n)\r\n\r\nSELECT\r\n  rd.IdPais,\r\n  rd.IdIndicador,\r\n  i.Descripcion as Indicador,\r\n  MAX(IF(rd.RowNum = 1, rd.Anio, NULL))   AS AnioActual,\r\n  MAX(IF(rd.RowNum = 1, rd.Valor, NULL))  AS ValorActual,\r\n  MAX(IF(rd.RowNum = 2, rd.Anio, NULL))   AS AnioAnterior,\r\n  MAX(IF(rd.RowNum = 2, rd.Valor, NULL))  AS ValorAnterior\r\nFROM\r\n  RankedData as rd\r\nJOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Indicador` as i\r\n  ON rd.IdIndicador = i.IdIndicador\r\nWHERE\r\n  rd.RowNum IN (1, 2)\r\nGROUP BY\r\n  rd.IdPais,\r\n  rd.IdIndicador, \r\n  i.Descripcion;",
    "useLegacySql": false
  }
}
