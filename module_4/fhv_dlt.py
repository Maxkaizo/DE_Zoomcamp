import os
import gzip
import shutil
import requests
import dlt
from google.cloud import storage
from tqdm import tqdm
import time

# 🔹 Configuración de la API de GitHub
GITHUB_REPO = "DataTalksClub/nyc-tlc-data"
TAG = "fhv"
URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/tags/{TAG}"
HEADERS = {"Accept": "application/vnd.github.v3+json"}

# 🔹 Configuración de Google Cloud Storage (GCS)
GCS_BUCKET_NAME = "dataeng-448500-hw-4"
GCS_CREDENTIALS_PATH = "/home/maxkaizo/DE_Zoomcamp/module_1/terraform_gcp/terrademo/keys/my-creds.json"

# Crear cliente de almacenamiento con autenticación explícita
client = storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)

# Crear directorio temporal para almacenar los archivos antes de subirlos
TEMP_DIR = "github_files"
os.makedirs(TEMP_DIR, exist_ok=True)

# Función para verificar si el bucket existe antes de la subida
def check_bucket_exists(bucket_name):
    """Verifica si el bucket en GCS existe antes de la subida."""
    try:
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            print(f"❌ ERROR: El bucket {bucket_name} NO existe en GCS. Revisa el nombre.")
            return False
        return True
    except Exception as e:
        print(f"❌ ERROR verificando el bucket {bucket_name}: {e}")
        return False

# Función para descargar archivos desde GitHub si no existen localmente
def download_files_from_github():
    """Descarga archivos desde GitHub solo si no existen en github_files/"""
    response = requests.get(URL, headers=HEADERS)
    response.raise_for_status()
    release_data = response.json()

    files = []
    assets = release_data.get("assets", [])

    # Filtramos solo archivos de 2019 y 2020
    filtered_assets = [asset for asset in assets if "2019" in asset["name"]]# or "2020" in asset["name"]]

    if not filtered_assets:
        print("⚠ No se encontraron archivos para 2019 o 2020.")
        return []

    print(f"📥 Descargando {len(filtered_assets)} archivos (si no están en caché)...")

    for asset in tqdm(filtered_assets, desc="Verificando archivos", unit="archivo"):
        file_url = asset["browser_download_url"]
        file_name = asset["name"]
        local_path = os.path.join(TEMP_DIR, file_name)

        # Si el archivo ya existe, lo omitimos
        if os.path.exists(local_path):
            print(f"🔍 Archivo encontrado en caché: {file_name}, omitiendo descarga.")
            files.append({"name": file_name, "path": local_path})
            continue

        # Descargar el archivo
        response = requests.get(file_url, stream=True)
        total_size = int(response.headers.get("content-length", 0))
        block_size = 8192  # 8KB

        with open(local_path, "wb") as f, tqdm(
            desc=f"📄 Descargando {file_name}",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            leave=False
        ) as bar:
            for chunk in response.iter_content(chunk_size=block_size):
                f.write(chunk)
                bar.update(len(chunk))

        files.append({"name": file_name, "path": local_path})

    return files


# Función para descomprimir archivos .gz
def decompress_gz(file_path):
    """Descomprime un archivo .gz en la misma carpeta y devuelve la nueva ruta."""
    if not file_path.endswith(".gz"):
        return file_path  # Si no es .gz, no hacemos nada

    decompressed_path = file_path.replace(".gz", "")
    print(f"📂 Descomprimiendo: {file_path} → {decompressed_path}")

    with gzip.open(file_path, 'rb') as f_in, open(decompressed_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    return decompressed_path  # Retornamos la nueva ruta

# Función para subir archivos grandes con "resumable uploads" y barra de progreso
def upload_to_gcs(file_path, file_name, retries=3):
    """Sube un archivo a GCS con Resumable Upload y muestra el progreso correctamente."""
    if not os.path.exists(file_path):
        print(f"❌ ERROR: El archivo {file_path} NO existe localmente. No se puede subir.")
        return

    # Descomprimir si es .gz antes de subirlo
    new_file_path = decompress_gz(file_path)
    if new_file_path != file_path:
        file_path = new_file_path
        file_name = file_name.replace(".gz", "")  # Actualizar el nombre sin .gz

    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(file_name)

    total_size = os.path.getsize(file_path)
    print(f"📤 Preparando para subir: {file_name} ({total_size / 1024 / 1024:.2f} MB)")

    blob.chunk_size = 50 * 1024 * 1024  # Resumable Upload: Fragmentos de 50MB

    for attempt in range(retries):
        try:
            with open(file_path, "rb") as f, tqdm(
                desc=f"🚀 Subiendo {file_name}",
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                leave=True
            ) as bar:
                while chunk := f.read(blob.chunk_size):  # Leer en fragmentos
                    blob.upload_from_string(chunk, content_type="application/octet-stream")
                    bar.update(len(chunk))  # Actualizar tqdm en cada fragmento subido

            print(f"✅ Archivo subido correctamente: gs://{GCS_BUCKET_NAME}/{file_name}")
            return  # Si se sube correctamente, salimos de la función

        except Exception as e:
            print(f"⚠️ Intento {attempt+1}/{retries} fallido al subir {file_name}: {e}")
            time.sleep(5)  # Espera antes de reintentar

    print(f"❌ No se pudo subir {file_name} después de {retries} intentos.")

# Definir pipeline con DLT
def github_to_gcs_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="github_to_gcs",
        destination="duckdb",  # Para almacenar metadatos
        dataset_name="github_files"
    )

    # Verificar si el bucket existe antes de continuar
    if not check_bucket_exists(GCS_BUCKET_NAME):
        print("🚨 Abortando proceso porque el bucket no existe.")
        return

    # Descargar los archivos filtrados de GitHub (solo si no están en caché)
    files = download_files_from_github()
    if not files:
        print("❌ No hay archivos para procesar.")
        return

    # Subir archivos filtrados a GCS con barra de progreso y reintentos
    print(f"📤 Subiendo {len(files)} archivos a Google Cloud Storage...")
    for file in tqdm(files, desc="Subiendo archivos", unit="archivo"):
        upload_to_gcs(file["path"], file["name"])

    # Registrar en DLT los metadatos de los archivos procesados
    load_info = pipeline.run(files)
    print(load_info)

# Ejecutar la pipeline
if __name__ == "__main__":
    github_to_gcs_pipeline()
