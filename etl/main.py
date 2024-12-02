import json
import boto3
import pymysql
import re
import os
import sys
import csv
from dotenv import load_dotenv
from loguru import logger
import time

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
S3_OUTPUT_LOCATION = "s3://logs123123/"

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

logs_file = "/logs_output/etl_log.log"
logger.add(logs_file)
id = "ETL_Process"

athena = boto3.client(
    "athena",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)

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

def execute_athena_query(query):
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "catalogo"},
            ResultConfiguration={"OutputLocation": S3_OUTPUT_LOCATION},
        )
        query_execution_id = response["QueryExecutionId"]
        info(f"Consulta iniciada con ID: {query_execution_id}")
        return query_execution_id
    except Exception as e:
        error(f"Error ejecutando la consulta en Athena: {e}")
        exit_program(True)

def wait_for_query_to_complete(query_execution_id, max_retries=10, wait_time=5):
    for attempt in range(max_retries):
        try:
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                info(f"Consulta con ID {query_execution_id} completada exitosamente.")
                return True
            elif state in ["FAILED", "CANCELLED"]:
                error(f"Consulta fallida o cancelada. Estado: {state}")
                return False
        except Exception as e:
            error(f"Error verificando el estado de la consulta: {e}")
        time.sleep(wait_time)
    error("La consulta en Athena excedió el tiempo de espera.")
    return False

def get_query_results_from_s3(query_execution_id):
    try:
        file_key = f"{query_execution_id}.csv"
        info(f"Buscando archivo en S3: {file_key}")
        s3 = boto3.client("s3", region_name=AWS_REGION)
        response = s3.get_object(Bucket="logs123123", Key=file_key)
        content = response['Body'].read().decode('utf-8')
        rows = list(csv.DictReader(content.splitlines()))
        info(f"Resultados obtenidos: {len(rows)} filas.")
        return rows
    except Exception as e:
        error(f"Error obteniendo resultados desde S3: {e}")
        exit_program(True)

def load_to_mysql(data, table_name):
    if not data:
        warning(f"No hay datos para insertar en la tabla {table_name}.")
        return
    info(f"Iniciando la carga en la tabla {table_name}. Datos: {data}")
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT
        )
        with connection.cursor() as cursor:
            for record in data:
                try:
                    columns = ", ".join(record.keys())
                    placeholders = ", ".join(["%s"] * len(record))
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    info(f"Ejecutando query: {sql} con valores: {tuple(record.values())}")
                    cursor.execute(sql, tuple(record.values()))
                except Exception as e:
                    error(f"Error insertando el registro {record} en {table_name}: {e}")
            connection.commit()
        info(f"Datos cargados exitosamente en la tabla {table_name}.")
    except Exception as e:
        error(f"Error general cargando datos en MySQL: {e}")
        exit_program(True)
    finally:
        connection.close()

def safely_convert_to_json(data_str):
    try:
        corrected_str = re.sub(r'(\w+)=([^,}\]]+)', r'"\1": "\2"', data_str)
        corrected_str = corrected_str.replace("'", '"')
        return json.loads(corrected_str)
    except Exception as e:
        error(f"Error convirtiendo a JSON: {data_str} - {e}")
        return {}

def transform_reports(data):
    transformed_data = []
    for record in data:
        tenant_id = record["tenant_id"]
        report_id = record["report_id"]
        data_json = safely_convert_to_json(record["data"])
        transformed_data.append({
            "tenant_id": tenant_id,
            "report_id": report_id,
            "total_sales": data_json.get("total_sales", 0),
            "total_items": data_json.get("total_items", 0),
        })
    return transformed_data

def transform_billing(data):
    transformed_data = []
    for record in data:
        payment_json = safely_convert_to_json(record["payment_details"])
        transformed_data.append({
            "invoice_id": record["invoice_id"],
            "tenant_id": record["tenant_id"],
            "order_id": record["order_id"],
            "method": payment_json.get("method", ""),
            "amount": payment_json.get("amount", 0),
            "status": record["status"],
        })
    return transformed_data

def transform_inventory(data):
    return [
        {
            "product_id": record["product_id"],
            "tenant_id": record["tenant_id"],
            "stock_available": float(record["stock_available"]),
            "last_update": record["last_update"],
        }
        for record in data
    ]

def transform_productos(data):
    return [
        {
            "product_id": record["product_id"],
            "tenant_id": record["tenant_id"],
            "name": record["name"],
            "price": float(record["price"]),
            "description": record["description"],
        }
        for record in data
    ]
def transform_order(data):
    orders = []
    order_products_set = set()
    order_products = []

    for record in data:
        orders.append({
            "order_id": record["order_id"],
            "tenant_id": record["tenant_id"],
            "user_id": record["user_id"],
            "status": record["status"],
        })
        items_json = safely_convert_to_json(record["items"])
        info(f"Procesando 'items' para la orden {record['order_id']}: {items_json}")
        if isinstance(items_json, list):
            for item in items_json:
                product_id = item.get("product_id", None)
                price = item.get("price", None)
                if not product_id or price is None:
                    warning(f"Producto inválido encontrado en 'items': {item}")
                    continue
                order_products_set.add((record["order_id"], product_id))
        else:
            warning(f"Formato inesperado en 'items' para la orden {record['order_id']}: {record['items']}")

    for order_id, product_id in order_products_set:
        order_products.append({
            "order_id": order_id,
            "product_id": product_id,
        })

    info(f"Datos generados para 'OrderProductos': {order_products}")
    return orders, order_products

def etl_process():
    queries = {
        "Reports": 'SELECT * FROM "AwsDataCatalog"."catalogo"."api-reportes-dev"',
        "Billing": 'SELECT * FROM "AwsDataCatalog"."catalogo"."billingservice-dev"',
        "Inventory": 'SELECT * FROM "AwsDataCatalog"."catalogo"."inventoryservice-dev"',
        "Order": 'SELECT * FROM "AwsDataCatalog"."catalogo"."orderservice-dev"',
        "Productos": 'SELECT * FROM "AwsDataCatalog"."catalogo"."productservice-dev"'
    }
    for table_name, query in queries.items():
        try:
            logger.info(f"Procesando tabla: {table_name}")
            query_execution_id = execute_athena_query(query)
            if not wait_for_query_to_complete(query_execution_id):
                logger.warning(f"Consulta para {table_name} no completada.")
                continue
            data = get_query_results_from_s3(query_execution_id)
            if not data:
                logger.warning(f"No se encontraron datos para la tabla {table_name}.")
                continue
            if table_name == "Reports":
                transformed_data = transform_reports(data)
                load_to_mysql(transformed_data, "Reports")
            elif table_name == "Billing":
                transformed_data = transform_billing(data)
                load_to_mysql(transformed_data, "Billing")
            elif table_name == "Inventory":
                transformed_data = transform_inventory(data)
                load_to_mysql(transformed_data, "Inventory")
            elif table_name == "Order":
                orders, order_products = transform_order(data)
                load_to_mysql(orders, "Orders")
                load_to_mysql(order_products, "OrderProductos")
            elif table_name == "Productos":
                transformed_data = transform_productos(data)
                load_to_mysql(transformed_data, "Productos")
            else:
                logger.error(f"Transformación no definida para la tabla {table_name}.")
        except Exception as e:
            logger.error(f"Error procesando la tabla {table_name}: {e}")

if __name__ == "__main__":
    etl_process()
