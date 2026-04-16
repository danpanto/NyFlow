import subprocess
import sys
import time
import webbrowser
import os

ROOT_DIR = os.getcwd()
WEB_PAGE_DIR = os.path.join(ROOT_DIR, "web-page")


def main():
    print("🚀 Iniciando NyFlow...")

    # 1. Levantamos el servidor del Mapa (FastAPI) en el puerto 8000
    print("Levantando servidor del mapa (FastAPI) en el puerto 8000...")
    map_process = subprocess.Popen(
        ["uv", "run", "-m", "visualization.app.main"], cwd=ROOT_DIR
    )

    # 2. Levantamos el servidor en el puerto 3000
    print("Levantando servidor de la web en el puerto 3000...")
    web_process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "3000"], cwd=WEB_PAGE_DIR
    )

    time.sleep(2)

    # 4. Abrimos el navegador en la web principal
    print("🌐 Abriendo el navegador...")
    webbrowser.open("http://localhost:3000")

    try:
        print("\n✨ ¡Todo en marcha!")
        # Mantenemos el script corriendo
        map_process.wait()
        web_process.wait()
    except KeyboardInterrupt:
        print("\n🛑 Apagando servidores de NyFlow...")
        map_process.terminate()
        web_process.terminate()


if __name__ == "__main__":
    main()
