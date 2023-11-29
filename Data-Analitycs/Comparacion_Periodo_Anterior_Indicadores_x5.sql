WITH PeriodData AS (
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
  pd.IdPais,
  pd.IdIndicador,
  i.Descripcion as Indicador,
  MIN(IF(pd.RowNum <= 5, pd.Anio, NULL)) AS PeriodoActualInicio,
  MAX(IF(pd.RowNum <= 5, pd.Anio, NULL)) AS PeriodoActualFin,
  MIN(IF(pd.RowNum > 5 AND pd.RowNum <= 10, pd.Anio, NULL)) AS PeriodoAnteriorInicio,
  MAX(IF(pd.RowNum > 5 AND pd.RowNum <= 10, pd.Anio, NULL)) AS PeriodoAnteriorFin,
  AVG(IF(pd.RowNum <= 5, pd.Valor, NULL)) AS PromedioValorActual,
  AVG(IF(pd.RowNum > 5 AND pd.RowNum <= 10, pd.Valor, NULL)) AS PromedioValorAnterior
FROM
  PeriodData as pd
JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Indicador` as i
  ON pd.IdIndicador = i.IdIndicador
WHERE
  pd.RowNum <= 10  -- Considera solo los últimos 10 años
GROUP BY
  pd.IdPais,
  pd.IdIndicador, 
  i.Descripcion;
