import os
from pathlib import Path
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


class MinioPD2:
    def __init__(self, bucket="pd2", env_path=None):
        load_dotenv(dotenv_path=env_path)

        endpoint = (
            os.getenv("MINIO_ENDPOINT", "")
            .replace("http://", "")
            .replace("https://", "")
        )
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        print(endpoint, access_key, secret_key)

        self.client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=True
        )

        self.bucket_name = "pd2"

    def upload_files(self, paths: list[Path], folder="cityenjoyer"):
        bucket = self.bucket_name
        for p in paths:
            destination_file = folder + "/" + p.name
            try:
                self.client.stat_object(bucket, destination_file)
                print(f"El archivo {destination_file} ya existe en el servidor.")
                continue
            except S3Error as err:
                if err.code != "NoSuchKey":
                    print(f"Error detectado: {err}")
                    continue

            try:
                self.client.fput_object(bucket, destination_file, p)
                print(f"¡Éxito! {p} se subió como {destination_file}")

            except S3Error as exc:
                print(f"Error detectado: {exc}")

    def download_files(self, local_destination="./data", remote_folder="cityenjoyer"):
        bucket = self.bucket_name
        local_path = Path(local_destination)
        local_path.mkdir(parents=True, exist_ok=True)

        try:
            objects = self.client.list_objects(
                bucket, prefix=f"{remote_folder}/", recursive=True
            )

            for obj in objects:
                print(obj)
                file_name = Path(obj.object_name).name
                dest_path = local_path / file_name

                if dest_path.exists():
                    print(f"Encontrado archivo local: {dest_path}")
                    continue

                print(f"Descargando {obj.object_name}...")

                self.client.fget_object(bucket, obj.object_name, dest_path)
                print(f"¡Guardado en! {dest_path}")

        except S3Error as exc:
            print(f"Error detectado: {exc}")

    def download_file(
        self, local_destination="./data", remote_folder="cityenjoyer", file_name=None
    ):
        """
        Downloads a specific file from the Minio bucket to a local destination. If the file already exists locally, it will skip the download.

        :param self: The instance of the MinioPD2 class.
        :param local_destination: The local path where the file will be downloaded.
        :param remote_folder: The remote folder in Minio where the file is located.
        :param file_name: The name of the specific file to download.
        """
        bucket = self.bucket_name
        local_path = Path(local_destination)
        local_path.mkdir(parents=True, exist_ok=True)

        try:
            objects = self.client.list_objects(
                bucket, prefix=f"{remote_folder}/", recursive=True
            )
            obj = None
            for o in objects:
                if Path(o.object_name).name == file_name:
                    obj = o
                    break

            if obj is None:
                print(
                    f"Archivo {file_name} no encontrado en el directorio remoto {remote_folder}"
                )
                return

            print(obj)
            file_name = Path(obj.object_name).name
            dest_path = local_path / file_name

            if dest_path.exists():
                print(f"Encontrado archivo local: {dest_path}")
                return
            print(f"Descargando {obj.object_name}...")

            self.client.fget_object(bucket, obj.object_name, dest_path)
            print(f"¡Guardado en! {dest_path}")

        except S3Error as exc:
            print(f"Error detectado: {exc}")


if __name__ == "__main__":
    minio = MinioPD2()
    carpeta = Path("./data")
    rutas = list(carpeta.iterdir())
    minio.upload_files(rutas)
