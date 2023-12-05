{
  "creationTime": "1700863604851",
  "etag": "3C6othrkZW+1N8QlmM64/g==",
  "id": "possible-willow-403216:Modelo_Esperanza_De_Vida.Join-Indicador-Categoria-Datos",
  "kind": "bigquery#table",
  "lastModifiedTime": "1701343753162",
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
        "name": "Anio",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "Valor",
        "type": "FLOAT"
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
        "name": "Categoria",
        "type": "STRING"
      }
    ]
  },
  "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/possible-willow-403216/datasets/Modelo_Esperanza_De_Vida/tables/Join-Indicador-Categoria-Datos",
  "tableReference": {
    "datasetId": "Modelo_Esperanza_De_Vida",
    "projectId": "possible-willow-403216",
    "tableId": "Join-Indicador-Categoria-Datos"
  },
  "type": "VIEW",
  "view": {
    "query": "SELECT d.IdPais, d.Anio, d.Valor, d.IdIndicador, i.Descripcion as Indicador, c.Categoria\r\nFROM `possible-willow-403216.Modelo_Esperanza_De_Vida.DatosIndicador` as d\r\nJOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Indicador` as i\r\n  ON d.IdIndicador = i.IdIndicador\r\nJOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.IndicadorCategoria` as x\r\n  ON i.IdIndicador = x.IdIndicador\r\nJOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Categoria` as c\r\n  ON x.IdCategoria = c.IdCategoria;\r\n",
    "useLegacySql": false
  }
}
