import os 


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
    files.sort(key=lambda x: os.path.getmtime(os.path.join(path, x)),
               reverse=True)

    # Retorna el primer archivo de la lista (el más reciente)
    return files[0] if files else None  



if  __name__ == "__main__":
    folder = "__songs__"
    print("=============-------------------------------------================")
    print("  La ultima cancion descargada fue " + get_latest_file(folder))
    print("=============-------------------------------------================")
