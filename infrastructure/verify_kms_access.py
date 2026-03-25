import boto3
import sys

def verify_kms_access(role_arn, key_id):
    """
    Verifica que el rol de IAM tenga permisos para usar la clave KMS.
    Senior Pattern: Evita fallos en runtime de Spark 2 horas después de iniciado el job.
    """
    client = boto3.client('kms', region_name='us-east-1')
    try:
        response = client.describe_key(KeyId=key_id)
        print(f"✅ Key {key_id} is accessible. Status: {response['KeyMetadata']['KeyState']}")
        return True
    except Exception as e:
        print(f"❌ Critical: No access to KMS Key {key_id}. Error: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: verify_kms.py <key_id>")
        sys.exit(1)
    verify_kms_access(None, sys.argv[1])
