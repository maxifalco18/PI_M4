import base64
import subprocess
import time

def deploy():
    print("Reading docker-compose.yaml...")
    with open('docker-compose.yaml', 'rb') as f:
        b64_content = base64.b64encode(f.read()).decode('utf-8')

    print("Uploading file via base64 ssh execution...")
    upload_cmd = f"ssh -i pi-m4-ec2-key.pem -o StrictHostKeyChecking=no ubuntu@ec2-100-53-184-100.compute-1.amazonaws.com \"echo {b64_content} | base64 -d > /home/ubuntu/docker-compose.yaml\""
    r = subprocess.run(upload_cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"Deploy failed during upload: {r.stderr}")
        return

    print("Setting up airflow directories and running docker compose...")
    setup_cmd = """ssh -i pi-m4-ec2-key.pem -o StrictHostKeyChecking=no ubuntu@ec2-100-53-184-100.compute-1.amazonaws.com "mkdir -p /home/ubuntu/airflow/dags /home/ubuntu/airflow/logs /home/ubuntu/airflow/plugins && mv /home/ubuntu/docker-compose.yaml /home/ubuntu/airflow/ && cd /home/ubuntu/airflow && echo 'AIRFLOW_UID=50000' > .env && sudo docker compose up -d" """
    r2 = subprocess.run(setup_cmd, shell=True, capture_output=True, text=True)
    if r2.returncode != 0:
        print(f"Deploy failed during setup: {r2.stderr}")
        return
        
    print("Deployment successful!")
    print(r2.stdout)

if __name__ == "__main__":
    deploy()
