import os
import boto3
import subprocess
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

MONGO_CONTAINER = os.getenv("MONGO_CONTAINER")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

TEMP_DB = MONGO_DB_NAME + "_temp"

s3 = boto3.client(
    service_name="s3",
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="auto"
)

def obtener_ultimo_backup():

    print("🔎 Buscando último backup en R2...")

    objetos = s3.list_objects_v2(Bucket=R2_BUCKET_NAME)

    if "Contents" not in objetos:
        raise Exception("No hay backups en el bucket")

    ultimo = sorted(objetos["Contents"], key=lambda x: x["LastModified"], reverse=True)[0]

    print(f"✅ Último backup encontrado: {ultimo['Key']}")

    return ultimo["Key"]

def ejecutar_restore():

    archivo = obtener_ultimo_backup()
    ruta_local = os.path.join(os.getcwd(), archivo)

    print("☁️ Descargando backup...")
    s3.download_file(R2_BUCKET_NAME, archivo, ruta_local)

    print("🧪 Restaurando en BD TEMPORAL...")

    comando_temp = f"""
    docker exec -i {MONGO_CONTAINER} mongorestore \
    --gzip \
    --archive \
    --nsFrom="{MONGO_DB_NAME}.*" \
    --nsTo="{TEMP_DB}.*" \
    --drop
    """

    with open(ruta_local, "rb") as f:
        subprocess.run(comando_temp, shell=True, stdin=f, check=True)

    print("✅ Restore temporal OK")

    print("🔎 Validando datos en BD temporal...")

    validar = f'docker exec {MONGO_CONTAINER} mongosh --quiet --eval "db.getSiblingDB(\'{TEMP_DB}\').getCollectionNames().length"'

    result = subprocess.check_output(validar, shell=True)
    colecciones = int(result.decode().strip())

    if colecciones == 0:
        print("❌ BD temporal vacía. Abortando producción.")
        return

    print(f"✅ BD temporal tiene {colecciones} colecciones")

    print("🚀 Restaurando en PRODUCCIÓN...")

    comando_prod = f"""
    docker exec -i {MONGO_CONTAINER} mongorestore \
    --gzip \
    --archive \
    --drop
    """

    with open(ruta_local, "rb") as f:
        subprocess.run(comando_prod, shell=True, stdin=f, check=True)

    print("🔥 RESTORE EN PRODUCCIÓN COMPLETADO")

    os.remove(ruta_local)
    print("🧹 Archivo eliminado")

if __name__ == "__main__":
    ejecutar_restore()