import boto3
import csv

import sys
import os



def main():
    limit = 5
    s3_client = boto3.client('s3')
    # Establecer el límite de tamaño de campo más alto
    csv.field_size_limit(5000000)
    with open("data_Finaly.csv" , "r", encoding="utf-8") as file:

        data = csv.DictReader(file) 
        i = 1
        for song in data:
            id = song["spotify_id"]
            track = song["track"]
            spotify_uri = "http://open.spotify.com/track/" + id 
            os.system("python -m spotdl " + spotify_uri + " --output songs/"  + id +"\n")
            
            i+=1

            # Ruta local del archivo de audio
            archivo_local = 'songs/'+ id +"/"+ track

            # Nombre del archivo en S3
            nombre_archivo_s3 = id + " " + track

            # Nombre del bucket en S3
            nombre_bucket = 'checho-bucket-9089'

            try:
                with open(archivo_local, 'rb') as fd:
                    response = s3_client.put_object(
                        Bucket=nombre_bucket,
                        Key=nombre_archivo_s3,
                        Body=fd
                    )
                    print(f"Archivo {nombre_archivo_s3} subido exitosamente a {nombre_bucket}")
            except FileNotFoundError:
                print("Archivo no encontrado")
            except Exception as e:
                print(f"Error al subir el archivo: {str(e)}")

            if limit == i  and limit != -1:
                print("Acabamos hijueputa, solo tienes establecido el limite de " + limit)
                break



if __name__ == "__main__":

    try:
        with open("song/Swans - Will We Survive.mp3", 'rb') as fd:
            response = boto3.client("s3").put_object(
                Bucket="checho-bucket-9089",
                Key="Swans - Will We Survive.mp3",
                Body=fd
            )
            print(f"Archivo  subido exitosamente a checho bucket")
    except FileNotFoundError:
        print("Archivo no encontrado")
    except Exception as e:
        print(f"Error al subir el archivo: {str(e)}")








