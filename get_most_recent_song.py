import os
import pandas
from colorama import Fore,Style

def get_song_position(data, song):
    """
    Obtiene la posición de una canción en un DataFrame basado en el valor de
    'track'.

    Args:
        data (pd.DataFrame): DataFrame que contiene información sobre las
                             canciones.
        song (str): Nombre de la canción a buscar en el DataFrame.

    Returns:
        pd.DataFrame: Fila(s) del DataFrame donde se encuentra la canción.
    """
    song_position = data.loc[data["track"] == song]
    
    print(f"Id: {song_position}")
    return song_position



def get_latest_file(path):
    """
    Imprime el último archivo guardado en una carpeta dada.
    Args:
        path (str): Ruta de la carpeta.
    """

    # Lista todos los archivos en el directorio especificado
    files = os.listdir(path)

    # Filtra solo los archivos (excluye directorios)
    files = [f for f in files if os.path.isfile(os.path.join(path, f))]

    # Ordena los archivos por fecha de modificación (último primero)
    files.sort(key=lambda x: os.path.getmtime(os.path.join(path, x)), reverse=True)

    # Retorna el primer archivo de la lista (el más reciente)
    return files[0] if files else None


if __name__ == "__main__":
    folder = "__songs__"
    data = pandas.read_csv("data.csv")
    song = get_latest_file(folder)
    print("=============-------------------------------------================")
    print(f"{Fore.CYAN}  La ultima cancion descargada fue {song} {Style.RESET_ALL}")
    print("\tId: " + get_song_position(data,song)["track"])
    print("=============-------------------------------------================")
    
