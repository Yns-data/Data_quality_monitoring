# type: ignore
from datetime import datetime 
import requests
import json
import re
import os
# Date à tester
date = datetime(2024, 4, 20, 14, 30)
date_str = date.strftime("%Y-%m-%d-%H")
base_url = os.getenv("API_BASE_URL", "https://api-prod-487999225005.europe-west10.run.app")
# Requête pour obtenir le nombre de visiteurs
response = requests.get(f"{base_url}/visitors?date={date_str}")

# Afficher le résultat en JSON formaté
print("Nombre de visiteurs :")
print(json.dumps(response.json(), indent=2))