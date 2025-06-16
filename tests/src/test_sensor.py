import pytest
from datetime import datetime
from src.sensor import VisitorGenerator

class TestVisitorGenerator:
    @pytest.fixture
    def generator(self):
        """Instance de base pour les tests"""
        return VisitorGenerator(min_visitors=50, max_visitors=500)

    def test_get_visitors(self, generator):
        """Test de la méthode get_visitors"""
        date = datetime(2024, 3, 20, 14, 30)
        visitors = generator.get_visitors(date)
        
        # Vérifie que la valeur est dans les bornes
        assert 50 <= visitors <= 500
        
        # Vérifie que la même date donne toujours la même valeur
        assert visitors == generator.get_visitors(date)

    def test_generate_null_values(self, generator):
        """Test de la méthode generate_null_values"""
        date = datetime(2024, 3, 20, 14, 30)
        
        # Génère 10 valeurs
        values = [generator.generate_null_values(date) for _ in range(10)]
        
        # Vérifie qu'on a des None
        assert None in values
        
        # Vérifie que les valeurs non nulles sont dans les bornes
        for value in values:
            if value is not None:
                assert 50 <= value <= 500