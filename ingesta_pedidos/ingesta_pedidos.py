import copy
import pandas as pd
import boto3
import os
import sys
from loguru import logger
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector

# Configuración de logs
logs_file = "logs_output/ingesta_pedidos.log"
logger.remove(0)
logger.add(logs_file, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

def exit_program(early_exit=False):
    if early_exit:
        logger.warning("Saliendo del programa antes de la ejecución debido a un error previo.")
        sys.exit(1)
    else:
        logger.info("Programa terminado exitosamente.")

# Variables de entorno
table_name = os.environ.get("TABLE_NAME")
bucket_name = os.environ.get("BUCKET_NAME")

if not table_name:
    logger.critical("No se encontró el nombre de la tabla.")
    exit_program(True)
if not bucket_name:
    logger.critical("No se encontró el nombre del bucket de S3.")
    exit_program(True)

# Conectar a S3 y DynamoDB
try:
    s3 = boto3.client("s3")
    logger.info("Conexión a S3 exitosa.")
except Exception as e:
    logger.critical(f"No fue posible conectarse a S3. Excepción: {e}")
    exit_program(True)

try:
    client = boto3.client("dynamodb", region_name="us-east-1")
    logger.info("Conexión a DynamoDB exitosa.")
except Exception as e:
    logger.critical(f"No fue posible conectarse a DynamoDB. Excepción: {e}")
    exit_program(True)

# Configurar paginador y transformador
paginator = client.get_paginator("scan")
service_model = client._service_model.operation_model("Scan")
trans = TransformationInjector(deserializer=TypeDeserializer())

operation_parameters = {
    "TableName": table_name,
}

# Proceso de ingestión
i = 0
for page in paginator.paginate(**operation_parameters):
    original_last_evaluated_key = ""
    if "LastEvaluatedKey" in page:
        original_last_evaluated_key = copy.copy(page["LastEvaluatedKey"])

    # Transformar datos DynamoDB a formato Python
    trans.inject_attribute_value_output(page, service_model)
    if original_last_evaluated_key:
        page["LastEvaluatedKey"] = original_last_evaluated_key

    # Convertir items a DataFrame
    items = page["Items"]
    pedidos = pd.DataFrame.from_records(items)

    # Procesar columnas específicas (personaliza según tu esquema)
    pedidos["created_at"] = pd.to_datetime(pedidos["created_at"], errors="coerce")

    # Guardar datos en archivo JSON
    pedidos_file = f"pedidos_{i}.json"
    pedidos.to_json(pedidos_file, orient="records", lines=True)

    # Subir archivo JSON a S3
    s3_pedidos_file = f"pedidos/pedidos_{i}.json"
    try:
        s3.upload_file(pedidos_file, bucket_name, s3_pedidos_file)
        logger.info(f"Página {i} subida exitosamente a S3: {s3_pedidos_file}")
    except Exception as e:
        logger.error(f"No se pudo subir la página {i} a S3. Excepción: {e}")

    i += 1

exit_program()
