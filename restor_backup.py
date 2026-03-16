import os
import re
import boto3
import subprocess
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

# Configuración
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

MONGO_CONTAINER = os.getenv("MONGO_CONTAINER")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
TEMP_DB = f"{MONGO_DB_NAME}_temp"

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
    
    # Paginación para asegurar que vemos todos los archivos si hay más de 1000
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=R2_BUCKET_NAME)

    todos_los_objetos = []
    for page in page_iterator:
        if "Contents" in page:
            todos_los_objetos.extend(page["Contents"])

    if not todos_los_objetos:
        raise Exception("❌ No hay backups en el bucket")

    # Ordenar por fecha de modificación
    ultimo = sorted(todos_los_objetos, key=lambda x: x["LastModified"], reverse=True)[0]
    print(f"✅ Último backup encontrado: {ultimo['Key']}")
    return ultimo["Key"]

def ejecutar_restore():
    archivo_key = obtener_ultimo_backup()
    
    # CORRECCIÓN 1: Usar solo el nombre del archivo para la ruta local
    # Evita errores si la Key tiene carpetas (ej: backups/db.gz)
    nombre_archivo_local = os.path.basename(archivo_key)
    ruta_local = os.path.abspath(nombre_archivo_local)

    print(f"☁️ Descargando {archivo_key}...")
    s3.download_file(R2_BUCKET_NAME, archivo_key, ruta_local)

    try:
        # --- PASO 1: RESTORE TEMPORAL ---
        print(f"🧪 Restaurando en BD TEMPORAL ({TEMP_DB})...")
        comando_temp = [
            "docker", "exec", "-i", MONGO_CONTAINER, "mongorestore",
            "--gzip", "--archive",
            f"--nsFrom={MONGO_DB_NAME}.*",
            f"--nsTo={TEMP_DB}.*",
            "--drop"
        ]

        with open(ruta_local, "rb") as f:
            subprocess.run(comando_temp, stdin=f, check=True)

        # --- PASO 2: VALIDACIÓN ---
        print("🔎 Validando datos en BD temporal...")
        # CORRECCIÓN 2: Validación más robusta para evitar ruidos de mongosh
        js_query = f"db.getSiblingDB('{TEMP_DB}').getCollectionNames().length"
        validar = ["docker", "exec", MONGO_CONTAINER, "mongosh", "--quiet", "--eval", js_query]
        
        result = subprocess.check_output(validar).decode().strip()
        
        # Extraer solo el número (por si mongosh devuelve warnings de conexión)
        match = re.search(r'(\d+)', result)
        colecciones = int(match.group(1)) if match else 0

        if colecciones == 0:
            print("❌ Error: La BD temporal no tiene colecciones. Abortando.")
            return

        print(f"✅ BD temporal validada con {colecciones} colecciones.")

        # --- PASO 3: RESTORE PRODUCCIÓN ---
        print(f"🚀 Restaurando en PRODUCCIÓN ({MONGO_DB_NAME})...")
        comando_prod = [
            "docker", "exec", "-i", MONGO_CONTAINER, "mongorestore",
            "--gzip", "--archive", "--drop"
        ]

        with open(ruta_local, "rb") as f:
            subprocess.run(comando_prod, stdin=f, check=True)

        print("🔥 RESTORE EN PRODUCCIÓN COMPLETADO CON ÉXITO")

    finally:
        # --- PASO 4: LIMPIEZA ---
        if os.path.exists(ruta_local):
            os.remove(ruta_local)
            print("🧹 Archivo local eliminado.")
        
        # Limpiar la base de datos temporal en el contenedor
        print(f"🧹 Eliminando base de datos temporal {TEMP_DB}...")
        drop_cmd = ["docker", "exec", MONGO_CONTAINER, "mongosh", "--eval", f"db.getSiblingDB('{TEMP_DB}').dropDatabase()"]
        subprocess.run(drop_cmd, capture_output=True)

if __name__ == "__main__":
    try:
        ejecutar_restore()
    except Exception as e:
        print(f"🚨 ERROR CRÍTICO: {e}")