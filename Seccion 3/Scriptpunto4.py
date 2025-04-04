import boto3
import json
import time
import logging

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Inicio del handler de API Lambda")
    
    try:
        # Inicialización de clientes de Athena
        athena = boto3.client('athena')
        
        # Parámetros de configuración (actualizar según tu entorno)
        database = "nombre_de_tu_database"  # Nombre de la base de datos en Athena
        athena_output = "s3://ruta-de-salida-athena/"  # Ruta S3 para resultados temporales de Athena

        # Consulta SQL que agrupa la información de ventas por cliente y por día,
        # incluyendo la relación con proveedores
        query = """
        SELECT 
             pp.proveedor_id,
             c.id AS cliente_id, 
             c.nombre, 
             c.apellido, 
             v.fecha, 
             COUNT(*) AS cantidad_transacciones, 
             SUM(v.monto) AS monto_total
        FROM ventas v
        JOIN clientes c ON v.cliente_id = c.id
        JOIN productos_proveedor pp ON v.producto_id = pp.producto_id
        GROUP BY pp.proveedor_id, c.id, c.nombre, c.apellido, v.fecha
        """
        logger.info("Iniciando ejecución de la consulta en Athena")
        
        # Inicia la consulta en Athena
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': athena_output}
        )
        execution_id = response['QueryExecutionId']
        logger.info("Consulta iniciada, ID: %s", execution_id)
        
        # Polling para esperar a que la consulta se complete
        state = 'RUNNING'
        while state in ['RUNNING', 'QUEUED']:
            response_status = athena.get_query_execution(QueryExecutionId=execution_id)
            state = response_status['QueryExecution']['Status']['State']
            logger.info("Estado de la consulta: %s", state)
            if state in ['FAILED', 'CANCELLED']:
                error_message = response_status['QueryExecution']['Status'].get('StateChangeReason', 'Consulta fallida')
                logger.error("Error en la consulta: %s", error_message)
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "La consulta en Athena falló", "details": error_message})
                }
            time.sleep(1)
        
        logger.info("Consulta completada, obteniendo resultados")
        result_response = athena.get_query_results(QueryExecutionId=execution_id)
        rows = result_response['ResultSet']['Rows']
        
        # Validar si se obtuvieron datos
        if not rows or len(rows) < 2:
            logger.warning("No se encontraron datos en la consulta")
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "No se encontraron resultados."})
            }
        
        # La primera fila contiene los encabezados
        headers = [col.get('VarCharValue', '') for col in rows[0]['Data']]
        logger.info("Encabezados obtenidos: %s", headers)
        
        data = []
        for row in rows[1:]:
            row_data = {}
            for header, col in zip(headers, row['Data']):
                row_data[header] = col.get('VarCharValue', '')
            data.append(row_data)
        
        # Agrupamos los datos por proveedor
        grouped_data = {}
        for row in data:
            provider_id = row.get('proveedor_id')
            if provider_id not in grouped_data:
                grouped_data[provider_id] = []
            grouped_data[provider_id].append(row)
        
        logger.info("Datos agrupados por proveedor: %s", list(grouped_data.keys()))
        
        response_data = {
            "total_proveedores": len(grouped_data),
            "data": grouped_data
        }
        
        logger.info("Respuesta generada exitosamente")
        return {
            "statusCode": 200,
            "body": json.dumps(response_data)
        }
    
    except Exception as e:
        logger.exception("Excepción durante la ejecución de la función Lambda")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error interno del servidor", "details": str(e)})
        }
