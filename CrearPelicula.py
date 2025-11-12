import os
import uuid
import json
import traceback

import boto3
from botocore.exceptions import ClientError

TABLE_ENV_NAME = "TABLE_NAME"

def _log_info(datos: dict):
    # Log estándar INFO
    print(json.dumps({"tipo": "INFO", "log_datos": datos}, ensure_ascii=False))

def _log_error(datos: dict):
    # Log estándar ERROR
    print(json.dumps({"tipo": "ERROR", "log_datos": datos}, ensure_ascii=False))

def _parse_event_body(event):
    """
    Soporta:
      - event["body"] como dict
      - event["body"] como string JSON
      - event plano con keys en top-level (fallback)
    """
    if isinstance(event, dict) and "body" in event:
        body = event["body"]
        if isinstance(body, str):
            body = json.loads(body) if body.strip() else {}
        elif not isinstance(body, dict):
            raise ValueError("El campo 'body' debe ser dict o string JSON.")
        return body
    # fallback: permitir que vengan en top-level
    return event if isinstance(event, dict) else {}

def lambda_handler(event, context):
    # Log de entrada en formato estándar
    _log_info({"evento_entrada": event})

    try:
        # Validaciones iniciales
        nombre_tabla = os.environ.get(TABLE_ENV_NAME)
        if not nombre_tabla:
            raise RuntimeError(f"Variable de entorno '{TABLE_ENV_NAME}' no definida.")

        body = _parse_event_body(event)

        if "tenant_id" not in body:
            raise KeyError("Falta 'tenant_id' en el body.")
        if "pelicula_datos" not in body:
            raise KeyError("Falta 'pelicula_datos' en el body.")

        tenant_id = body["tenant_id"]
        pelicula_datos = body["pelicula_datos"]

        # Generar UUID
        uuidv4 = str(uuid.uuid4())

        # Item a insertar
        pelicula = {
            "tenant_id": tenant_id,
            "uuid": uuidv4,
            "pelicula_datos": pelicula_datos
        }

        # DynamoDB put
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(nombre_tabla)

        response = table.put_item(Item=pelicula)

        # Log de salida correcto (INFO)
        _log_info({
            "operacion": "crear_pelicula",
            "estado": "ok",
            "tabla": nombre_tabla,
            "tenant_id": tenant_id,
            "uuid": uuidv4,
            "http_status": response.get("ResponseMetadata", {}).get("HTTPStatusCode", None)
        })

        # Respuesta (si usas API Gateway HTTP API, suele requerir body string)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "pelicula": pelicula,
                "response": {
                    "http_status": response.get("ResponseMetadata", {}).get("HTTPStatusCode", None)
                }
            }, ensure_ascii=False)
        }

    except (ClientError, ValueError, KeyError, RuntimeError) as e:
        # Errores esperables (validación / AWS)
        err = {
            "operacion": "crear_pelicula",
            "estado": "error",
            "mensaje": str(e),
            "tipo_error": e.__class__.__name__,
            "traceback": traceback.format_exc(limit=3),
            "tabla": os.environ.get(TABLE_ENV_NAME),
        }
        # incluir datos de entrada útiles (anonimizar si corresponde)
        try:
            body_dbg = _parse_event_body(event)
        except Exception:
            body_dbg = {}
        err["entrada_relevante"] = {
            "tenant_id": body_dbg.get("tenant_id"),
            # Evita imprimir data sensible completa; aquí solo la clave presente
            "tiene_pelicula_datos": "pelicula_datos" in body_dbg
        }

        _log_error(err)

        status = 400 if isinstance(e, (ValueError, KeyError, RuntimeError)) else 500
        return {
            "statusCode": status,
            "body": json.dumps({
                "error": {
                    "mensaje": str(e),
                    "tipo_error": e.__class__.__name__
                }
            }, ensure_ascii=False)
        }

    except Exception as e:
        # Errores no controlados
        err = {
            "operacion": "crear_pelicula",
            "estado": "error",
            "mensaje": str(e),
            "tipo_error": e.__class__.__name__,
            "traceback": traceback.format_exc(limit=5),
            "tabla": os.environ.get(TABLE_ENV_NAME),
        }
        _log_error(err)

        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": {
                    "mensaje": "Error interno inesperado.",
                    "tipo_error": e.__class__.__name__
                }
            }, ensure_ascii=False)
        }
