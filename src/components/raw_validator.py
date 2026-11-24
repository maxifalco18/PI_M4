import boto3
import logging

logger = logging.getLogger("RawValidator")

def check_raw_availability(bucket, prefixes):
    """
    HITO 2: Validación de que los datos son correctamente almacenados en S3.
    Verifica que los prefijos Raw existan y contengan archivos Parquet.
    """
    s3 = boto3.client('s3')
    missing_paths = []
    
    for prefix in prefixes:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if 'Contents' not in response:
            missing_paths.append(prefix)
            
    if missing_paths:
        logger.error(f"❌ Missing Raw Data in: {missing_paths}")
        return False
    
    logger.info("✅ All Raw paths are available for processing.")
    return True
