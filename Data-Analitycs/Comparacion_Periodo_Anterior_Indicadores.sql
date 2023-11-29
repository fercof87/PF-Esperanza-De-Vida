WITH RankedData AS (
  SELECT
    IdPais,
    IdIndicador,
    Anio,
    Valor,
    ROW_NUMBER() OVER (PARTITION BY IdPais, IdIndicador ORDER BY Anio DESC) AS RowNum
  FROM
    `possible-willow-403216.Modelo_Esperanza_De_Vida.DatosIndicador`
)

SELECT
  rd.IdPais,
  rd.IdIndicador,
  i.Descripcion as Indicador,
  MAX(IF(rd.RowNum = 1, rd.Anio, NULL))   AS AnioActual,
  MAX(IF(rd.RowNum = 1, rd.Valor, NULL))  AS ValorActual,
  MAX(IF(rd.RowNum = 2, rd.Anio, NULL))   AS AnioAnterior,
  MAX(IF(rd.RowNum = 2, rd.Valor, NULL))  AS ValorAnterior
FROM
  RankedData as rd
JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Indicador` as i
  ON rd.IdIndicador = i.IdIndicador
WHERE
  rd.RowNum IN (1, 2)
GROUP BY
  rd.IdPais,
  rd.IdIndicador, 
  i.Descripcion;