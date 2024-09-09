import librosa
import numpy as np
import pandas as pd
import os
import inquirer
from multiprocessing import Pool, cpu_count

"""
Variables globales:
"""
# Directorio donde se encuentran las canciones
audio_directory = '../__songs__/'
audio_files = [audio_directory + f for f in os.listdir(audio_directory)
               if f.endswith('.mp3')]

# Definir las columnas del archivo CSV
columns = ['name', 'rms', 'zcr', 'tempo', 'onset_strength'] + \
          [f'mfcc_{i}' for i in range(1, 14)] + \
          [f'spectral_contrast_{i}' for i in range(1, 8)]

# Definir csv
CSV = "audio_features.csv"
try:
    songs_csv = pd.read_csv(CSV)
except Exception:
    print("No hay canciones procesadas hasta el momento")
    songs_csv = pd.DataFrame(columns=columns)


def extract_features(audio_file):
    """
    Extrae características de una canción usando librosa.
    """
    global songs_csv
    try:
        audio_name = audio_file.replace("../__songs__/", "").replace(
            ".mp3", "")

        if songs_csv["name"].isin([audio_name]).any():
            print(f"Saltando {audio_name}: Ya está procesada")
            return

        # Cargar el archivo de audio
        y, sr = librosa.load(audio_file, sr=None)

        # Energía RMS
        rms = np.mean(librosa.feature.rms(y=y))

        # Tasa de cruces por cero
        zcr = np.mean(librosa.feature.zero_crossing_rate(y=y))

        # Tempo (Beats por Minuto)
        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)

        # Contraste espectral
        spectral_contrast = np.mean(
            librosa.feature.spectral_contrast(y=y, sr=sr), axis=1
        )

        # Fuerza de los ataques
        onset_strength = np.mean(librosa.onset.onset_strength(y=y, sr=sr))

        # Coeficientes MFCC (13)
        mfcc = np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13), axis=1)

        print(f"Se procesó la canción: {audio_name}")

        # Retornar la lista de características
        return [audio_name, rms, zcr, tempo[0], onset_strength] + \
            mfcc.tolist() + spectral_contrast.tolist()

    except Exception as e:
        print(f"Error procesando {audio_file}: {e}")
        return None


def save_features(features):
    """
    Guarda los resultados en el archivo CSV.
    """
    df_features = pd.DataFrame([features], columns=columns)
    # Guardar de manera incremental en el archivo CSV
    df_features.to_csv(CSV, mode='a', header=False,
                       index=False)


def process_song(path):
    """
    Procesa una canción para extraer sus características y guarda los
    resultados en un archivo CSV.

    Parámetros:
    -----------
    path : str
        La ruta completa al archivo de audio .mp3 que se va a procesar.

    La función realiza los siguientes pasos:
    1. Llama a `extract_features` para obtener las características de la
       canción.
    2. Si se obtienen características válidas, llama a `save_features` para
       guardar estos datos en el archivo CSV.

    El archivo CSV se actualiza de manera incremental con las características
    extraídas de cada canción.
    """
    features = extract_features(path)
    if features:
        save_features(features)


def main():
    f"""
    Punto de entrada principal del programa.
    Utiliza multiprocessing para extraer características en paralelo de
    múltiples archivos .mp3.
    Las características se guardan en un archivo CSV llamado
    {CSV}.
    """
    global songs_csv
    try:
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
        num_cpus = cpu_count()

        # Limitar a la mitad del número de CPUs
        max_workers = round(num_cpus * processing_force)

        print("Number of CPUs: ", num_cpus)
        print("Number of workers: ", max_workers, end="\n\n")

        # Escribir las cabeceras antes de procesar
        songs_csv.to_csv(CSV, mode='w', index=False)

        # Crear un pool de trabajadores para procesar múltiples archivos de
        # audio en paralelo
        with Pool(max_workers) as pool:
            pool.map(process_song, audio_files)

    except KeyboardInterrupt:
        print("\nProceso interrumpido. Progreso guardado hasta el momento.")

    except Exception as e:
        print(f"Error en la ejecución: {e}")

    finally:
        print("Proceso completado. Las características se guardaron en",
              f"{CSV}")


if __name__ == '__main__':
    main()
