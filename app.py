import platform
import subprocess
from concurrent.futures import ThreadPoolExecutor
from modules.s3_manager import upload_file_to_s3
from modules.dynamoDb_manager import upload_song_record_to_dynamodb, exists_record_dynamodb
import os
import pandas as pd
import shutil
import inquirer

def process_song(song):
    id = song.spotify_id
    track = song.track
    spotify_uri = "http://open.spotify.com/track/" + id 

    dynamoDb = "uploaded_songs"

    # Si ya fue subida pass
    if exists_record_dynamodb(dynamoDb, id):
        print(f"\"{track}\" parece ya estar subida al S3, pasando a la siguiente de la lista ")
        return 0

    subprocess.run(f"./spotdl-4.2.5.exe download {spotify_uri} --output {songs_cache_folder}/{id}")
    
    archivo_local = f"{songs_cache_folder}/{id}/{track}.mp3"
    # Nombre del archivo en S3
    nombre_archivo_s3 = id + " " + track
    # Nombre del bucket en S3
    nombre_bucket = 'checho-bucket-9089'

    # Renombra la cancion que pone automaticamente spotdl por el que aparece en el DataFrame
    if len(os.listdir(f'{songs_cache_folder}/{id}')):
        downloaded_song_title = os.listdir(f'{songs_cache_folder}/{id}')[0]
        try:
            os.rename(f"{songs_cache_folder}/{id}/{downloaded_song_title}" , archivo_local  )
        except OSError as e:
            pass    
        response = upload_file_to_s3(file_path=archivo_local , bucket_name=nombre_bucket,object_name=nombre_archivo_s3)

        if response:
            record = {
                'spotify_id': id,
                'song_s3_key': f'{id} {track}' }
            upload_song_record_to_dynamodb(table_name="uploaded_songs" , item=record)
            return 1

    else:
        print(f"Hubo un error al subir la cancion '{track}'")
        return 0
    
    shutil.rmtree(f"{songs_cache_folder}/{id}")

def main(data, limit=-1):
    global songs_cache_folder
    songs_cache_folder = "__songs__"

    if os.path.exists(songs_cache_folder):
        shutil.rmtree(songs_cache_folder)
    try:
        os.mkdir(songs_cache_folder)
    except OSError as e:
        print("Error al crear el directorio:", e)

    success_uploaded = 0


    #  Starting proccess
    print("Starting process")

    # Obtener el número de CPUs disponibles
    num_cpus = os.cpu_count()
    print("Number of CPU's: " ,num_cpus )

    # Limitar a la mitad del número de CPUs
    max_workers = num_cpus 
    print("Number of workers: " ,max_workers )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for index, song in data.iterrows():
            futures.append(executor.submit(process_song, song))
            print(index," row readed from data")

        for i, future in enumerate(futures):
            print(i,"/",len(futures))
            success_uploaded += future.result()

            if limit != -1 and success_uploaded >= limit:
                print(f"Acabamos, alcanzamos el límite de {limit} canciones.")
                break

    print(f"Canciones subidas: {success_uploaded}")

    shutil.rmtree(songs_cache_folder)

def detect_os():
    # Método 1: Usando el módulo platform
    os_name = platform.system()
    if os_name == "Windows":
        print("El script se está ejecutando en Windows.")
    elif os_name == "Linux":
        print("El script se está ejecutando en Linux.")
    else:
        print(f"El script se está ejecutando en un sistema operativo no identificado: {os_name}")
    return os_name

if __name__ == "__main__":
        
    questions = [
        inquirer.List('size',
                        message="¿Que parte del data set vas a subir?",
                        choices=[
                            '1. Parte 1 (0 - 30.000)',
                            '2. Parte 2 (30.001 - 60.000)',
                            '3. Parte 3 (60.001 - 90.001)']),]
    Part = inquirer.prompt(questions)["size"]
    Part = Part[0]
    
    data = pd.read_csv("data.csv")

    if Part == "1":
        data = data.loc[0:29999]
    if Part == "2":
        data = data.loc[30000:59999]
    if Part == "3":
        data = data.loc[60000:90000]
    
    SO = detect_os()
    main(data, limit=-1)

    shutil.rmtree("__songs__")

    

