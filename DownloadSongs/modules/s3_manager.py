import boto3
import boto3.exceptions
from colorama import Fore, Style, init



def upload_file_to_s3(file_path, bucket_name, object_name):
    """
    Sube un archivo al bucket de S3 especificado.

    Args:
        file_path (str): Ruta local del archivo a subir.
        bucket_name (str): Nombre del bucket de S3.
        object_name (str): Nombre que se le dará al objeto en S3.

    Returns:
        bool: True si el archivo se subió correctamente, False en caso contrario.
    """

    # Inicializar el cliente de S3
    s3_client = boto3.client('s3', region_name='us-east-1')

    try:
        # Subir el archivo al bucket de S3
        s3_client.upload_file(file_path, bucket_name, object_name)
        s3_client.close()

        print(f"\n\nCancion {Fore.MAGENTA}{object_name}{Style.RESET_ALL} subida exitosamente a S3.\n\n")
        
        return True
    except Exception as e:        
        s3_client.close()

        print(f"Error al subir el archivo a S3: {e}")
        return False
    



def contar_elementos_en_bucket(bucket_name):
    
    s3_client = boto3.client('s3', region_name='us-east-1')
    # Inicializar el contador de objetos
    contador = 0
    
    # Usar el paginador para listar todos los objetos en el bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        # Verificar si la página contiene objetos
        if 'Contents' in page:
            # Contar los objetos en la página actual
            contador += len(page['Contents'])
    
    return contador

# Ejemplo de uso
if __name__ == "__main__":
    # Ruta local del archivo de audio
    audio_file_path = "song/Swans - Will We Survive.mp3"

    # Nombre del bucket de S3
    s3_bucket_name = "checho-bucket-9089"

    # Nombre que se le dará al objeto en S3
    s3_object_name = "Test_audio.mp3"

    # Subir el archivo al bucket de S3
    
    # Nombre del bucket que quieres contar los elementos
    bucket_name = 'checho-bucket-9089'
    
    # Llamar a la función y mostrar el resultado
    total_elementos = contar_elementos_en_bucket(bucket_name)
    print(f"El bucket {bucket_name} contiene {total_elementos} elementos.")
    upload_file_to_s3(audio_file_path, s3_bucket_name, s3_object_name)
