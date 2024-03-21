import csv
import sys

def main():
    limit = 5
    # Establecer el límite de tamaño de campo más alto
    csv.field_size_limit(500000)
    with open("./data_Finaly.csv" , "r", encoding="utf-8") as file:
        data = csv.DictReader(file) 
        i = 1
        for song in data:
            spotify_uri = "http://open.spotify.com/track/" + song["spotify_id"] 
            print( i , spotify_uri ) 
            i+=1

if __name__ == "__main__":
    main()
