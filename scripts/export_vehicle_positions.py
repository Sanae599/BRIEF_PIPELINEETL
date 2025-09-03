import os
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
import requests

load_dotenv()
URL = os.getenv("GTFS_RT_VP_URL")

#Téléchargement et parsing du flux
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(requests.get(URL, timeout=30).content)

#S'assurer que le dossier exports existe
os.makedirs("exports", exist_ok=True)
out_path = os.path.join("exports", "vehicle_positions.txt")

#Écrire le contenu texte des VehiclePositions dans le fichier
with open(out_path, "w", encoding="utf-8") as f:
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            f.write(str(entity.vehicle))
            f.write("\n")  # séparation entre les entités

print(f"Export ok : {out_path}")
