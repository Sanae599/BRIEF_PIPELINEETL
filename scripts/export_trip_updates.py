import os
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
import requests

load_dotenv()  # charge .env
URL = os.getenv("GTFS_RT_TU_URL")

#Téléchargement et parser le flux
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(requests.get(URL, timeout=30).content)

#S'assurer que le dossier exports existe
os.makedirs("exports", exist_ok=True)
out_path = os.path.join("exports", "trip_updates.txt")

#Écrire le contenu texte des TripUpdates dans le fichier
with open(out_path, "w", encoding="utf-8") as f:
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            f.write(str(entity.trip_update))
            f.write("\n")  # séparation entre les entités

print(f"Export ok : {out_path}")
