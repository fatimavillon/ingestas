import copy
import pandas as pd
import boto3
import os
import sys
from loguru import logger
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.environ.get('AWS_REGION')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN')
table_name = os.environ.get('TABLE_NAME')
bucket_name = os.environ.get('BUCKET_NAME')

logs_file = f"/logs_output/{table_name}_log.log"
id = table_name
logger.add(logs_file)

def critical(message):
    logger.critical(f"{id} - {message}")
def info(message):
    logger.info(f"{id} - {message}")
def error(message):
    logger.error(f"{id} - {message}")
def warning(message):
    logger.warning(f"{id} - {message}")
def exit_program(early_exit=False):
    if early_exit:
        warning('Saliendo del programa antes de la ejecución debido a un error previo.')
        sys.exit(1)
    else:
        info('Programa terminado exitosamente.')

if not all([AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN]):
    critical('Faltan credenciales de AWS en las variables de entorno.')
    exit_program(True)
if not table_name:
    critical('No se encontró el nombre de la tabla.')
    exit_program(True)
if not bucket_name:
    critical('No se encontró el nombre del bucket de S3.')
    exit_program(True)

try:
    s3 = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )
    info('Conexión a S3 exitosa.')
except Exception as e:
    critical(f'No fue posible conectarse a S3. Excepción: {e}')
    exit_program(True)

try:
    client = boto3.client(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )
    info('Conexión a DynamoDB exitosa.')
except Exception as e:
    critical(f'No fue posible conectarse a DynamoDB. Excepción: {e}')
    exit_program(True)

paginator = client.get_paginator('scan')
service_model = client._service_model.operation_model('Scan')
trans = TransformationInjector(deserializer=TypeDeserializer())

operation_parameters = {
    'TableName': table_name,
}
i = 0

for page in paginator.paginate(**operation_parameters):
    original_last_evaluated_key = ""
    if 'LastEvaluatedKey' in page:
        original_last_evaluated_key = copy.copy(page['LastEvaluatedKey'])

    trans.inject_attribute_value_output(page, service_model)
    if original_last_evaluated_key:
        page['LastEvaluatedKey'] = original_last_evaluated_key

    items = page['Items']

    products = pd.DataFrame.from_records(items)
    products['created_at'] = pd.to_datetime(products['created_at'])

    if 'data' in products.columns:
        product_data = pd.json_normalize(products['data']).join(products['product_id'])
        products.drop(columns=['data'], inplace=True)
    else:
        product_data = pd.DataFrame()

    product_file = f"{table_name}-data.json"
    products.to_json(product_file, orient='records', lines=True, force_ascii=False)

    s3_products_path = f"{table_name}/{table_name}-data-{i}.json"
    try:
        s3.upload_file(product_file, bucket_name, s3_products_path)
        info(f'Subido: {s3_products_path}')
    except Exception as e:
        error(f'Error al subir productos a S3. Excepción: {str(e)}')

    i += 1

info(f'Proceso completado. Páginas procesadas: {i}')
exit_program(False)
