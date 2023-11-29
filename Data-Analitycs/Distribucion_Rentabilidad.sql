SELECT
  p.IdIndicadorRentabilidad,
  ir.Descripcion,
  COUNT(*) AS Cantidad
FROM
  `possible-willow-403216.Modelo_Esperanza_De_Vida.Pais` p
JOIN
  `possible-willow-403216.Modelo_Esperanza_De_Vida.IndicadorRentabilidad` ir
ON
  p.IdIndicadorRentabilidad = ir.IdIndicadorRentabilidad
GROUP BY
  p.IdIndicadorRentabilidad, ir.Descripcion