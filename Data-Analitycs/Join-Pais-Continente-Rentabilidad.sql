SELECT  p.IdPais, p.Pais, p.CodPais, p.Region, c.ContinenteEng as Continente, i.Descripcion as IndicadorRentabilidad
FROM `possible-willow-403216.Modelo_Esperanza_De_Vida.Pais` as p
JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Continente` as c
  ON p.IdContinente = c.IdContinente
JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.IndicadorRentabilidad` as i
  ON p.IdIndicadorRentabilidad = i.IdIndicadorRentabilidad;