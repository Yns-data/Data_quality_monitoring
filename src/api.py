from fastapi import FastAPI
from datetime import datetime
from src.sensor import VisitorGenerator

app = FastAPI()
generator = VisitorGenerator(min_visitors=50, max_visitors=500)

@app.get("/visitors/{date}")
async def get_visitors(date: str):
    """
    Route GET pour obtenir le nombre de visiteurs pour une date donnée.
    Format de la date attendu: YYYY-MM-DD-HH
    """
    try:
        # Convertir la chaîne de date en objet datetime
        date_obj = datetime.strptime(date, "%Y-%m-%d-%H")
        
        # Générer le nombre de visiteurs
        visitors = generator.get_visitors(date_obj)
        
        return {
            "date": date,
            "visitors": visitors
        }
    except ValueError:
        return {
            "error": "Format de date invalide. Utilisez le format YYYY-MM-DD-HH"
        }