import boto3
from botocore.exceptions import ClientError
from colorama import Style, Fore
dynamodb = boto3.resource('dynamodb')

def upload_song_record_to_dynamodb(table_name, item):
    """
    Sube un registro a una tabla de DynamoDB especificada.

    Args:
        table_name (str): Nombre de la tabla de DynamoDB.
        item (dict): El registro que se va a subir, como un diccionario.
        EJ: record = {
            'spotify_id': '3Vt8m8f1ixaiEIbUaOdnN7z',
            'song_s3_key': '3Vt8m8f1ixaiEIbUaOdnN7z - Cancion de ejemplo. feat maesedev'
            }
        song_name(str): El nombre llave de la cancion subido a S3 
        
    Returns:
        bool: True si el registro se subió correctamente, False en caso contrario.
    """
    # Inicializar el cliente de DynamoDB

    # Obtener la tabla
    table = dynamodb.Table(table_name)

    try:
        # Poner el registro en la tabla
        table.put_item(Item=item)
        print(f"\n\nRegistro {Fore.LIGHTBLUE_EX}{item}{Style.RESET_ALL} subido exitosamente a DynamoDB\n\n")
        return True
    except ClientError as e:
        print(f"Error al subir el registro a DynamoDB: {e.response['Error']['Message']}")
        return False

def remove_record_from_dynamodb(table_name, song_id):
    """
    Elimina un registro a una tabla de DynamoDB especificada.

    Args:
        table_name (str): Nombre de la tabla de DynamoDB.
        song_id (str): El id del registro a eliminar
        
    Returns:
        bool: True si el registro se eliminó correctamente, False en caso contrario.
    """

    table = dynamodb.Table(table_name)
    try:
        table.delete_item(Key={
        'spotify_id': song_id,
    })
    except ClientError as e:
        print(f"Error al eliminar el registro a DynamoDB: {e.response['Error']['Message']}")
        return False
    else:
        print("Registro eliminado exitosamente de DynamoDB")
        return True   

def exists_record_dynamodb(table_name, song_id):
    table = dynamodb.Table(table_name)
    try:
        # Intenta obtener el elemento con la clave proporcionada
        response = table.get_item(
            TableName=table_name,
            Key={
                'spotify_id':song_id
            },
        )
        
        # Si hay un elemento, entonces existe
        if 'Item' in response:
            return True
        else:
            return False
    except Exception as e:
        # Maneja cualquier error que pueda ocurrir
        print("Ocurrió un error al obtener el registro en dynamoDb:", e)
        return False   

# Ejemplo de uso
if __name__ == "__main__":
    # Nombre de la tabla de DynamoDB
    dynamodb_table_name = "uploaded_songs"

    # El registro que se va a subir
    record = {
        'spotify_id': '3Vt8m8f1ixaiEIbUaOdnN7z',
        'song_s3_key' : "3Vt8m8f1ixaiEIbUaOdnN7z - feat.maesedev"
    }

    # Subir el registro a la tabla de DynamoDB
    remove_record_from_dynamodb(table_name=dynamodb_table_name , song_id="3Vt8m8f1ixaiEIbUaOdnN7z")
    upload_song_record_to_dynamodb(table_name=dynamodb_table_name, item=record)
