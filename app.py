import platform
import subprocess
from subprocess import Popen, PIPE
from concurrent.futures import ThreadPoolExecutor
import os
import difflib
import pandas as pd
import inquirer
from colorama import Fore, Style


def main(data, limit=-1):
    """
    Función principal para gestionar la descarga de canciones.

    Args:
        data (pd.DataFrame): Conjunto de datos que contiene información sobre
                             las canciones.
        limit (int, opcional): Límite de canciones a descargar. Valor por
                               defecto: -1 (sin límite).
    """
    global songs_folder
    songs_folder = "__songs__"

    if not (os.path.exists(songs_folder)):
        try:
            os.mkdir(songs_folder)
        except OSError as e:
            print("Error al crear el directorio:", e)

    success_uploaded = 0

    # Inicio del proceso
    print("\nStarting process")
    questions = [
        inquirer.List(
            "processing_force",
            message="¿Qué porcentaje del CPU vas a usar?",
            choices=["30%", "50%", "80%", "100%"],
        )
    ]

    processing_force = inquirer.prompt(questions)["processing_force"][:-1]
    processing_force = int(processing_force) / 100

    # Obtener el número de CPUs disponibles
    num_cpus = os.cpu_count()

    # Limitar a la mitad del número de CPUs
    max_workers = round(num_cpus * processing_force)

    print("Number of CPUs: ", num_cpus)
    print("Number of workers: ", max_workers)

    data_filtered = filter_downloaded_songs(data, '__songs__')

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        print("Recorriendo data...")
        for index, song in data_filtered.iterrows():
            futures.append(executor.submit(process_song, song))

        for future in futures:
            future.result()

            if limit != -1 and success_uploaded >= limit:
                print(f"Acabamos, alcanzamos el límite de {limit} canciones.")
                break


def process_song(song):
    """
    Procesa y descarga una canción de Spotify usando `spotdl`.

    Args:
        song (pd.Series): Datos de la canción que incluyen `spotify_id`
                          y `track`.
    """
    spotify_id = song.spotify_id
    track = song.track
    spotify_uri = f"http://open.spotify.com/track/{spotify_id}"

    if detect_os() == "Linux":
        
        p = Popen(["spotdl",spotify_uri,"--output",songs_folder,"--format","mp3"] 
                  ,stderr=PIPE)

        out, err = p.communicate()

        if p.returncode != 0:
            print("ERROR: cancion no se pudo descargar") 
            
    else:
        subprocess.run(
            [
                "./spotdl-4.2.5.exe",
                "download",
                spotify_uri,
                "--output",
                f"./{songs_folder}",
                "--format",
                "mp3",
            ]
        )

    # Ruta del archivo guardado
    archivo_local = f"{songs_folder}/{track}.mp3"
    print(
        f"Se descargó la canción {Fore.LIGHTMAGENTA_EX}{archivo_local}",
        f"{Style.RESET_ALL}",
    )


def detect_os():
    """
    Detecta y retorna el sistema operativo.

    Returns:
        str: Nombre del sistema operativo.
    """
    os_name = platform.system()
    if os_name == "Windows":
        print("El script se está ejecutando en Windows.")
    elif os_name == "Linux":
        print("El script se está ejecutando en Linux.")
    else:
        print(
            "El script se está ejecutando en un sistema operativo no",
            f"identificado: {os_name}",
        )
    return os_name


def filter_downloaded_songs(data, folder_path, threshold=0.6):
    """
    Filtra las canciones que ya han sido descargadas de un DataFrame usando
    coincidencia parcial en el nombre del artista y la canción.

    Args:
        data (pd.DataFrame): DataFrame que contiene información sobre las
                             canciones.
        folder_path (str): Ruta de la carpeta donde se guardan las canciones
                           descargadas.
        threshold (float, opcional): Umbral de similitud para considerar una
                                     canción como descargada. Valor por
                                     defecto: 0.6.

    Returns:
        pd.DataFrame: DataFrame con las canciones que aún no han sido
                      descargadas.
    """
    # Obtener lista de archivos en la carpeta de canciones
    downloaded_files = os.listdir(folder_path)

    print("Escaneando canciones...")

    # Extraer los nombres de las canciones descargadas (sin extensión)
    downloaded_tracks = [os.path.splitext(f)[0] for f in downloaded_files]

    # Función auxiliar para determinar si una canción ya está descargada
    def is_downloaded(artist, track):
        # Concatenar el nombre del artista y la canción como están en el
        # archivo mp3
        print(f"Artista: {artist}, Canción: {track}")
        search_str = f"{artist} - {track}"
        matches = difflib.get_close_matches(
            search_str, downloaded_tracks, n=1, cutoff=threshold
        )
        return len(matches) > 0

    print("Filtrando data...")

    # Filtrar el DataFrame para mantener solo las canciones que no han sido
    # descargadas
    data_filtered = data[
        ~data.apply(lambda row: is_downloaded(row["artist"], row["track"]),
                    axis=1)
    ]

    return data_filtered


if __name__ == "__main__":
    questions = [
        inquirer.List(
            "size",
            message="¿Qué parte del data set vas a subir?",
            choices=[
                "1. (   0  - 9999  ) [Isabella]",
                "2. (10000 - 19999 ) [Franklin]",
                "3. (20000 - 29999 ) [Santiago]",
                "4. (30000 - 39999 ) [Mateo]",
                "5. (40000 - 49999 ) [Sebastian]",
                "6. (50000 - 59999 ) [xxxx]",
                "7. (60000 - 69999 ) [xxxx]",
                "8. (70000 - 79999 ) [xxxx]",
                "9. (80000 - 90000 ) [xxxx]",
            ],
        )
    ]
    part = inquirer.prompt(questions)["size"][0]

    data = pd.read_csv("data.csv")

    if part == "1":
        data = data.loc[0:10000]
    elif part == "2":
        data = data.loc[10000:20000]
    elif part == "3":
        data = data.loc[20000:30000]
    elif part == "4":
        data = data.loc[30000:40000]
    elif part == "5":
        data = data.loc[40000:50000]
    elif part == "6":
        data = data.loc[50000:60000]
    elif part == "7":
        data = data.loc[60000:70000]
    elif part == "8":
        data = data.loc[70000:80000]
    elif part == "9":
        data = data.loc[80000:]

    detect_os()
    # data = data.loc[20104:]
    main(data, limit=-1)
