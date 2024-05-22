
import platform
import subprocess
from modules.s3_manager import upload_file_to_s3
from modules.dynamoDb_manager import upload_song_record_to_dynamodb
import os
import pandas as pd
import numpy as np

def main(limit = 5):
    
    success_uploaded = 0
    # Establecer el límite de tamaño de campo más alto

    ## Load CSV
    data = pd.read_csv("data_Finaly.csv",)
    i = 1
    for index, song in data.iterrows():
        id = song.spotify_id
        track = song.track
        spotify_uri = "http://open.spotify.com/track/" + id 

        
        subprocess.run(f"./spotdl-4.2.5.exe download {spotify_uri} --output songs/{id}")
        
        archivo_local = f"songs/{id}/{track}.mp3"
        # Nombre del archivo en S3
        nombre_archivo_s3 = id + " " + track
        # Nombre del bucket en S3
        nombre_bucket = 'checho-bucket-9089'

        # Renombra la cancion que pone automaticamente spotdl por el que aparece en el DataFrame
        if len(os.listdir(f'songs/{id}')):
            downloaded_song_title = os.listdir(f'songs/{id}')[0]
            os.rename(f"songs/{id}/{downloaded_song_title}" , archivo_local  )
            
            response = upload_file_to_s3(file_path=archivo_local , bucket_name=nombre_bucket,object_name=nombre_archivo_s3)

            if response:
                success_uploaded += 1
                record = {
                    'spotify_id': id,
                    'song_s3_key': f'{id} {track}' }
                upload_song_record_to_dynamodb(table_name="uploaded_songs" , item=record)
        
        else:
            print(f"Hubo un error al subir la cancion '{track}'")
        i+=1
        # Ruta local del archivo de audio

        
        if limit == i  and limit != -1:
            print(f"Acabamos hijueputa, solo tienes establecido el limite de {limit}")
            break
        print("Canciones subidas: " + success_uploaded)




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

    # True si el usuario dice que estamos usando linux
    SO = detect_os()
    main(limit=-1)








