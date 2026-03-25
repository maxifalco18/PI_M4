import boto3

def verify_lake_formation():
    """
    HITO 1: Validación de Registro en Lake Formation.
    """
    lf = boto3.client('lakeformation', region_name='us-east-1')
    try:
        resources = lf.list_resources()
        print("--- Registered LF Resources ---")
        for res in resources.get('ResourceInfoList', []):
            print(f"📍 Resource: {res['ResourceArn']}")
        return True
    except Exception as e:
        print(f"❌ Error listing LF resources: {str(e)}")
        return False

if __name__ == "__main__":
    verify_lake_formation()
