import copy
import pandas as pd
import boto3
import os
import sys
from loguru import logger
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector

# Configuración de logging
logs_file = "logs_output/ingesta_inventarios.log"
id = 'ingesta-inventarios'
logger.add(logs_file)

def critical(message):
    logger.critical(f"{id} - {message}")
def info(message):
    logger.info(f'{id} - {message}')
def error(message):
    logger.error(f'{id} - {message}')
def warning(message):
    logger.warning(f'{id} - {message}')

def exit_program(early_exit=False):
    if early_exit:
        warning('Saliendo del programa antes de la ejecución debido a un error previo.')
        sys.exit(1)
    else:
        info('Programa terminado exitosamente.')

# Parámetros de AWS
table_name = os.environ.get('TABLE_NAME')
bucket_name = os.environ.get('BUCKET_NAME')

if not table_name:
    critical('No se encontró el nombre de la tabla.')
    exit_program(True)
if not bucket_name:
    critical('No se encontró el nombre del bucket de S3.')
    exit_program(True)

# Conexión a DynamoDB y S3
try:
    s3 = boto3.client('s3')
    info('Conexión a S3 exitosa.')
except Exception as e:
    critical(f'No fue posible conectarse a S3. Excepción: {e}')
    exit_program(True)
    
try:
    client = boto3.client('dynamodb', region_name='us-east-1')
    info('Conexión a DynamoDB exitosa')
except Exception as e:
    critical(f'No fue posible conectarse a DynamoDB. Excepción: {e}')
    exit_program(True)

# Herramientas de paginación y transformación
paginator = client.get_paginator('scan')
service_model = client._service_model.operation_model('Scan')
trans = TransformationInjector(deserializer = TypeDeserializer())

operation_parameters = {
    'TableName': table_name,
}
i = 0   # Inicializar contador

# Iterar a través de las páginas
for page in paginator.paginate(**operation_parameters):
    # Guardar LastEvaluatedKey
    original_last_evaluated_key = ""
    if 'LastEvaluatedKey' in page:
        original_last_evaluated_key = copy.copy(page['LastEvaluatedKey'])
    print(original_last_evaluated_key)

    # Transformar
    trans.inject_attribute_value_output(page, service_model)
    if original_last_evaluated_key:
        page['LastEvaluatedKey'] = original_last_evaluated_key  # Resetear al original

    # Obtener los ítems de la página
    items = page['Items']

    # Convertir a DataFrame de pandas
    inventory_df = pd.DataFrame.from_records(items)
    
    # Agregar o modificar columnas si es necesario
    inventory_df['last_update'] = pd.to_datetime(inventory_df['last_update'])
    
    # Guardar como archivo JSON
    inventory_file = f'inventory_{i}.json'
    inventory_df.to_json(inventory_file, orient='records', lines=True)

    # Subir a S3
    s3_inventory_file = f'inventory/inventory_{i}.json'
    try:
        s3.upload_file(inventory_file, bucket_name, s3_inventory_file)
        info(f'Se subió la página {i} correctamente a S3.')
    except Exception as e:
        error(f'No se pudo subir la página {i} a S3. Excepción: {str(e)}')

    i += 1
    print("Página procesada No ", i)

info(f'Se procesaron {i} páginas.')
exit_program(False)