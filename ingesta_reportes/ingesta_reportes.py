# Ingesta de tabla de Reportes
# -----------------------------
#   - Conectarse a DynamoDB
#   - Escanear toda la tabla por páginas
#   - Convertir cada página en un DataFrame de pandas
#   - Guardar cada DataFrame como archivo JSON
#   - Subir los JSON resultantes a un bucket de S3

import boto3
import os
import sys
import pandas as pd
from loguru import logger
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector
import copy

# Configuración de logging
logger.add("logs_output/ingesta_reportes.log")
def critical(message): logger.critical(f"ingesta-reportes - {message}")
def info(message): logger.info(f"ingesta-reportes - {message}")
def error(message): logger.error(f"ingesta-reportes - {message}")
def exit_program(early_exit=False):
    if early_exit:
        critical("Saliendo del programa debido a un error crítico.")
        sys.exit(1)
    else:
        info("Ejecución finalizada exitosamente.")

# Variables de entorno
table_name = os.getenv('TABLE_NAME')
bucket_name = os.getenv('BUCKET_NAME')

if not table_name or not bucket_name:
    critical("Faltan variables de entorno TABLE_NAME o BUCKET_NAME.")
    exit_program(True)

# Conexión a DynamoDB y S3
try:
    dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    info("Conexión a DynamoDB exitosa.")
except Exception as e:
    critical(f"Error conectando a DynamoDB: {e}")
    exit_program(True)

try:
    s3_client = boto3.client('s3')
    info("Conexión a S3 exitosa.")
except Exception as e:
    critical(f"Error conectando a S3: {e}")
    exit_program(True)

# Configuración del paginador y transformador
paginator = dynamodb_client.get_paginator('scan')
service_model = dynamodb_client._service_model.operation_model('Scan')
transformer = TransformationInjector(deserializer=TypeDeserializer())

# Parámetros para escaneo de la tabla
operation_parameters = {'TableName': table_name}
page_index = 0

# Procesamiento por páginas
for page in paginator.paginate(**operation_parameters):
    # Guardar la clave de la última página (si existe)
    original_last_evaluated_key = ""
    if 'LastEvaluatedKey' in page:
        original_last_evaluated_key = copy.copy(page['LastEvaluatedKey'])

    # Transformar la respuesta DynamoDB a un formato legible
    transformer.inject_attribute_value_output(page, service_model)
    if original_last_evaluated_key:
        page['LastEvaluatedKey'] = original_last_evaluated_key

    # Convertir los ítems a DataFrame
    items = page.get('Items', [])
    if not items:
        warning(f"No se encontraron ítems en la página {page_index}.")
        continue

    reportes_df = pd.DataFrame.from_records(items)

    # Guardar como archivo JSON
    local_file = f"reportes_pagina_{page_index}.json"
    reportes_df.to_json(local_file, orient='records', lines=True)
    info(f"Página {page_index} procesada y guardada como JSON.")

    # Subir a S3
    s3_file_path = f"reportes/reportes_pagina_{page_index}.json"
    try:
        s3_client.upload_file(local_file, bucket_name, s3_file_path)
        info(f"Archivo {local_file} subido al bucket {bucket_name} en {s3_file_path}.")
    except Exception as e:
        error(f"Error subiendo {local_file} a S3: {e}")

    page_index += 1

info(f"Se procesaron {page_index} páginas en total.")
exit_program(False)