SELECT d.IdPais, d.Anio, d.Valor, d.IdIndicador, i.Descripcion as Indicador, c.Categoria
FROM `possible-willow-403216.Modelo_Esperanza_De_Vida.DatosIndicador` as d
JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Indicador` as i
  ON d.IdIndicador = i.IdIndicador
JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.IndicadorCategoria` as x
  ON i.IdIndicador = x.IdIndicador
JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Categoria` as c
  ON x.IdCategoria = c.IdCategoria;
