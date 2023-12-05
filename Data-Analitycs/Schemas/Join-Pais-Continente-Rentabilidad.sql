{
  "creationTime": "1700863278466",
  "etag": "CjgF7Sqzwhby4FWn0yNr5A==",
  "id": "possible-willow-403216:Modelo_Esperanza_De_Vida.Join-Pais-Continente-Rentabilidad",
  "kind": "bigquery#table",
  "lastModifiedTime": "1700863278724",
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
        "name": "Pais",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "CodPais",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "Region",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "Continente",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "IndicadorRentabilidad",
        "type": "STRING"
      }
    ]
  },
  "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/possible-willow-403216/datasets/Modelo_Esperanza_De_Vida/tables/Join-Pais-Continente-Rentabilidad",
  "tableReference": {
    "datasetId": "Modelo_Esperanza_De_Vida",
    "projectId": "possible-willow-403216",
    "tableId": "Join-Pais-Continente-Rentabilidad"
  },
  "type": "VIEW",
  "view": {
    "query": "SELECT  p.IdPais, p.Pais, p.CodPais, p.Region, c.ContinenteEng as Continente, i.Descripcion as IndicadorRentabilidad\r\nFROM `possible-willow-403216.Modelo_Esperanza_De_Vida.Pais` as p\r\nJOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Continente` as c\r\n  ON p.IdContinente = c.IdContinente\r\nJOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.IndicadorRentabilidad` as i\r\n  ON p.IdIndicadorRentabilidad = i.IdIndicadorRentabilidad;",
    "useLegacySql": false
  }
}
