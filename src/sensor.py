import hashlib
from datetime import datetime
import random

class VisitorGenerator:
    def __init__(self, min_visitors=10, max_visitors=1000):
        """
        Initialise le générateur de visiteurs.
        
        Args:
            min_visitors (int): Nombre minimum de visiteurs
            max_visitors (int): Nombre maximum de visiteurs
        """
        self.min_visitors = min_visitors
        self.max_visitors = max_visitors

    def get_visitors(self, date: datetime) -> int:
        """
        Génère un nombre constant de visiteurs pour une date et heure données.
        
        Args:
            date (datetime): La date et l'heure pour lesquelles générer le nombre de visiteurs
            
        Returns:
            int: Nombre de visiteurs pour cette date/heure
        """
        # Créer une chaîne unique basée sur la date et l'heure
        date_str = date.strftime("%Y-%m-%d-%H")
        
        # Utiliser le hachage pour générer un nombre
        hash_object = hashlib.md5(date_str.encode())
        hash_number = int(hash_object.hexdigest(), 16)
        
        # Normaliser le nombre entre min_visitors et max_visitors
        range_size = self.max_visitors - self.min_visitors
        visitors = self.min_visitors + (hash_number % range_size)
        
        return visitors

    def generate_null_values(self, date: datetime) -> int | None:
        """
        Génère aléatoirement soit une valeur nulle, soit une valeur aléatoire.
        
        Args:
            date (datetime): La date et l'heure pour lesquelles générer le nombre de visiteurs
            
        Returns:
            int | None: Soit une valeur aléatoire entre min_visitors et max_visitors, soit None
        """
        # Choisir aléatoirement entre une valeur nulle et une valeur aléatoire
        if random.choice([True, False]):
            return None
        else:
            return random.randint(self.min_visitors, self.max_visitors)