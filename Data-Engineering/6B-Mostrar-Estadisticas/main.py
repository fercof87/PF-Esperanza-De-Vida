from google.cloud import bigquery

def mostrar_estadisticas(request):
    # Configurar el cliente de BigQuery
    bq_client = bigquery.Client()

    # Consulta para recuperar los datos de la tabla Auditoria
    query = """
    SELECT IdAuditoria, NroEjecucion, Entorno, Proceso, Tabla, RegLeidos, RegInsertados,
           RegActualizados, RegEliminados, Fecha, Hora
    FROM Modelo_Esperanza_De_Vida.Auditoria
    ORDER BY IdAuditoria DESC, NroEjecucion DESC
    LIMIT 50
    """

    # Ejecutar la consulta
    query_job = bq_client.query(query)
    results = query_job.result()

    # Verificar si hay resultados antes de imprimir
    if results.total_rows > 0:
        #cabecera
        print("*------------------------------------------------------------------------------------------------------------------*")
        print("*                                                                                                                  *")
        print("*                                               DATOS DE AUDITORIA                                                *")
        print("*                                                                                                                  *")
        print("*------------------------------------------------------------------------------------------------------------------*")
        
        # Imprimir los resultados en un formato tabular
        print("{:<15} {:<15} {:<30} {:<30} {:<25} {:<15} {:<15} {:<20} {:<20} {:<15} {:<15}".format(
            "IdAuditoria", "NroEjecucion", "Entorno", "Proceso", "Tabla", "RegLeidos",
            "RegInsertados", "RegActualizados", "RegEliminados", "Fecha", "Hora"))

        for row in results:
            # Ajusta los nombres de las columnas según los resultados reales de la consulta
            print("{:<15} {:<15} {:<30} {:<30} {:<25} {:<15} {:<15} {:<20} {:<20} {:<15} {:<15}".format(
                row.IdAuditoria if row.IdAuditoria is not None else '',
                row.NroEjecucion if row.NroEjecucion is not None else '',
                row.Entorno if row.Entorno is not None else '',
                row.Proceso if row.Proceso is not None else '',
                row.Tabla if row.Tabla is not None else '',
                row.RegLeidos if row.RegLeidos is not None else '',
                row.RegInsertados if row.RegInsertados is not None else '',
                row.RegActualizados if row.RegActualizados is not None else '',
                row.RegEliminados if row.RegEliminados is not None else '',
                row.Fecha.strftime('%Y-%m-%d') if row.Fecha is not None else '',
                row.Hora.strftime('%H:%M:%S') if row.Hora is not None else ''
            ))
    else:
        print("No se encontraron resultados.")
    
    return 'Fin del Proceso ETL'