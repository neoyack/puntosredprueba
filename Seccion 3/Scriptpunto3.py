import boto3
import csv
import io
import time
import datetime

def lambda_handler(event, context):
    # Inicializamos clientes de Athena y S3
    athena = boto3.client('athena')
    s3 = boto3.client('s3')
    
    # Parámetros de configuración
    database = "nombre_de_tu_database"  # Reemplaza con el nombre de la base de datos en Athena
    athena_output = "s3://ruta-de-salida-athena/"  # Ruta S3 para resultados temporales de Athena
    s3_bucket = "nombre-del-bucket-curado"  # Bucket de destino para los archivos CSV

    # Consulta SQL que agrupa la información:
    # Se unen ventas, clientes y productos_proveedor para obtener la relación entre proveedor y las ventas
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

    # Ejecutamos la consulta en Athena
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': athena_output}
    )
    execution_id = response['QueryExecutionId']

    # Esperamos a que la consulta se complete (polling)
    state = 'RUNNING'
    while state in ['RUNNING', 'QUEUED']:
        response_status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = response_status['QueryExecution']['Status']['State']
        if state in ['FAILED', 'CANCELLED']:
            raise Exception("La consulta en Athena falló o fue cancelada")
        time.sleep(1)

    # Obtenemos los resultados
    result_response = athena.get_query_results(QueryExecutionId=execution_id)
    rows = result_response['ResultSet']['Rows']

    # La primera fila corresponde a los encabezados
    headers = [col.get('VarCharValue', '') for col in rows[0]['Data']]

    data = []
    for row in rows[1:]:
        row_data = {}
        # Se asume que el número de columnas coincide con los encabezados
        for header, col in zip(headers, row['Data']):
            row_data[header] = col.get('VarCharValue', '')
        data.append(row_data)

    # Agrupamos los datos por proveedor
    provider_data = {}
    for row in data:
        provider_id = row.get('proveedor_id')
        if provider_id not in provider_data:
            provider_data[provider_id] = []
        provider_data[provider_id].append(row)

    # Fecha de generación del archivo (por ejemplo, la fecha actual)
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    # Para cada proveedor, se genera un archivo CSV y se sube a S3
    for provider_id, rows in provider_data.items():
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
        
        # Definimos la clave (key) del objeto en S3; se puede ajustar el path según se requiera
        key = f"proveedor_{provider_id}/ventas_{today_str}.csv"
        
        s3.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())

    return {
        "statusCode": 200,
        "body": f"Archivos CSV generados para {len(provider_data)} proveedores."
    }
