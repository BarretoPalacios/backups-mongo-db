import os
import boto3
import subprocess
from datetime import datetime
from botocore.config import Config
from dotenv import load_dotenv

# Cargar variables desde el archivo .env
load_dotenv()

# --- CONFIGURACIÓN DESDE ENTORNO ---
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

MONGO_CONTAINER = os.getenv("MONGO_CONTAINER")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
MONGO_AUTH_DB = os.getenv("MONGO_AUTH_DB", "admin")

# Configuración de R2
s3 = boto3.client(
    service_name="s3",
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="auto"
)

def ejecutar_backup_docker():
    ahora = datetime.now()
    fecha_hora = ahora.strftime("%d_%m_%Y-%H-%M")
    nombre_archivo = f"backup_transjer_{fecha_hora}.gz"
    ruta_local = os.path.join(os.getcwd(), nombre_archivo)

    print(f"Extrayendo backup desde el contenedor: {MONGO_CONTAINER}...")

    try:
        comando = (
            f"docker exec {MONGO_CONTAINER} mongodump "
            f"--username {MONGO_USER} "
            f"--password {MONGO_PASS} "
            f"--authenticationDatabase {MONGO_AUTH_DB} "
            f"--db={MONGO_DB_NAME} "
            f"--archive --gzip"
        )
        
        with open(ruta_local, "wb") as f:
            subprocess.run(comando, shell=True, check=True, stdout=f)
        
        print(f"📦 Backup extraído y comprimido: {nombre_archivo}")

        # Subida a R2
        print(f"☁️ Subiendo a Cloudflare R2...")
        s3.upload_file(ruta_local, R2_BUCKET_NAME, nombre_archivo)
        print(f"✅ Backup en la nube: {nombre_archivo}")

        # Limpieza
        os.remove(ruta_local)
        print(f"🧹 Limpieza local completada.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Error de Docker: Asegúrate de que el contenedor '{MONGO_CONTAINER}' esté encendido.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    ejecutar_backup_docker()