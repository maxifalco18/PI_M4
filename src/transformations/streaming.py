import json
import boto3

def validate_event_schema(payload, registry_name="olist-registry", schema_name="olist-events"):
    """
    Validación física contra AWS Glue Schema Registry (Senior Pattern).
    """
    client = boto3.client('glue', region_name='us-east-1')
    try:
        # Petición física al Registry de AWS
        response = client.get_schema_version(
            SchemaId={'RegistryName': registry_name, 'SchemaName': schema_name},
            SchemaVersionNumber={'LatestVersion': True}
        )
        schema_definition = json.loads(response['SchemaDefinition'])
        data = json.loads(payload)
        
        # Validación de claves contra la definición oficial
        return all(key in data for key in schema_definition['required'])
    except Exception:
        # Fallback de seguridad (Log y permitir si es modo laxo, o rechazar en estricto)
        return False

def transform_speed_layer(df):
    """
    Transformaciones específicas de la capa Speed (Processed-Streaming).
    Limpia y filtra eventos entrantes.
    """
    return df.filter("order_value > 0 AND order_status IS NOT NULL")
