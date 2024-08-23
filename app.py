import platform
import subprocess
from concurrent.futures import ThreadPoolExecutor
import os
import pandas as pd
import shutil
import inquirer
from colorama import Fore, Style


def process_song(song):
    
    id = song.spotify_id
    track = song.track
    spotify_uri = f"http://open.spotify.com/track/{id}" 
    
    if detect_os() == "Linux":
        subprocess.run(f"spotdl {spotify_uri}  --output {songs_folder}  --format mp3", shell=True)
    else:
        subprocess.run(f"./spotdl-4.2.5.exe download {spotify_uri} --output {songs_folder}/{id} --format mp3", shell=True)
    
    # path con el que se guardo 
    archivo_local = f"{songs_folder}/{track}.mp3"
    
    print(f"se descargo la cancion {Fore.LIGHTMAGENTA_EX}{archivo_local}{Style.RESET_ALL}")
    

def main(data, limit=-1):
    '''
    Funcion principal
    '''
    global songs_folder
    songs_folder = "__songs__"
    

    if os.path.exists(songs_folder):
        shutil.rmtree(songs_folder)
    try:
        os.mkdir(songs_folder)
    except OSError as e:
        print("Error al crear el directorio:", e)

    success_uploaded = 0


    #  Starting proccess
    print("Starting process")
    questions = [
        inquirer.List('procesingForce',
                        message="¿Que porcentaje del CPU vas a usar?",
                        choices=[
                            '30%',
                            '50%',
                            '80%',
                            '100%']),]
    
    ProcessingForce = inquirer.prompt(questions)["procesingForce"][:-1]
    ProcessingForce = int(ProcessingForce) / 100

    # Obtener el número de CPUs disponibles
    num_cpus = os.cpu_count()
    
    # Limitar a la mitad del número de CPUs
    max_workers = round(num_cpus * ProcessingForce) 

    print("Number of CPU's: " ,num_cpus )
    print("Number of workers: " ,max_workers )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        print("Recorriendo data...")
        for index, song in data.iterrows():
            futures.append(executor.submit(process_song, song))
        
        for future in futures: 
            future.result()

            
            if limit != -1 and success_uploaded >= limit:
                print(f"Acabamos, alcanzamos el límite de {limit} canciones.")
                break






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
                            '1. (   0  - 9999  ) [Isabella]',
                            '2. (10000 - 19999 ) [Franklin]',
                            '3. (20000 - 29999 ) [Santiago]',
                            '4. (30000 - 39999 ) [Mateo]',
                            '5. (40000 - 49999 ) [Sebastian]',
                            '6. (50000 - 59999 ) [xxxx]',
                            '7. (60000 - 69999 ) [xxxx]',
                            '8. (70000 - 79999 ) [xxxx]',
                            '9. (80000 - 90000 ) [xxxx]']
                            )]
    Part = inquirer.prompt(questions)["size"]
    Part = Part[0]
    
    data = pd.read_csv("data.csv")

    if Part == "1":
        data = data.loc[0:9999]
    if Part == "2":
        data = data.loc[10000:19999]
    if Part == "3":
        data = data.loc[20000:29999]
    if Part == "4":
        data = data.loc[30000:39999]
    if Part == "5":
        data = data.loc[40000:49999]
    if Part == "6":
        data = data.loc[50000:59999]
    if Part == "7":
        data = data.loc[60000:69999]
    if Part == "8":
        data = data.loc[70000:79999]
    if Part == "9":
        data = data.loc[80000:90000]
    
    SO = detect_os()


    main(data, limit=-1)

    shutil.rmtree("__songs__")






# Hola llevame a tu proyecto
