import pytest
from datetime import datetime
from src.sensor import VisitorGenerator

class TestVisitorGenerator:
    @pytest.fixture
    def generator(self):
        """Instance de base pour les tests"""
        return VisitorGenerator(
            min_visitors=50, max_visitors=500,
            min_articles=10, max_articles=100,
            min_revenue=100, max_revenue=1000,
            min_pages_viewed=1, max_pages_viewed=20
        )

    def test_get_visitors(self, generator):
        """Test de la méthode get_visitors"""
        date = datetime(2024, 3, 20, 14, 30)
        visitors = generator.get_visitors(date)
        
        # Vérifie que la valeur est dans les bornes
        assert 50 <= visitors <= 500
        
        # Vérifie que la même date donne toujours la même valeur
        assert visitors == generator.get_visitors(date)

    @pytest.mark.parametrize("test_date,expected_revenue", [
        (datetime(2024, 3, 20, 14, 30), 674),
        (datetime(2024, 3, 21, 15, 45), 239),
        (datetime(2024, 3, 22, 16, 15), 986),
        (datetime(2024, 3, 23, 17, 30), 208)
    ])
    def test_get_revenue(self, generator, test_date, expected_revenue):
        """Test de la méthode get_revenue"""
        # Calculer le revenu pour la date paramétrée
        revenue = generator.get_revenue(test_date)

        # Vérifie que la valeur est dans les bornes
        assert 100 <= revenue <= 1000

        # Vérifie que la valeur correspond à celle attendue
        assert revenue == expected_revenue
    @pytest.mark.parametrize("test_date,expected_pages", [
        (datetime(2024, 3, 20, 14, 30), 1),
        (datetime(2024, 3, 21, 15, 45), 14),
        (datetime(2024, 3, 22, 16, 15), 18),
        (datetime(2024, 3, 23, 17, 30), 19)
    ])
    def test_get_pages_viewed(self, generator, test_date, expected_pages):
        """Test de la méthode get_pages_viewed"""
        pages = generator.get_pages_viewed(test_date)
        
        # Vérifie que la valeur est dans les bornes
        assert 1 <= pages <= 20
        
        # Vérifie que la valeur correspond à celle attendue
        assert pages == expected_pages
    @pytest.mark.parametrize("test_date,expected_articles", [
        (datetime(2024, 3, 20, 14, 30), 1),
        (datetime(2024, 3, 21, 15, 45), 14),
        (datetime(2024, 3, 22, 16, 15), 18),
        (datetime(2024, 3, 23, 17, 30), 19)
    ])
    def test_get_article(self, generator, test_date, expected_articles):
        """Test de la méthode get_article"""
        articles = generator.get_article(test_date)
        
        # Vérifie que la valeur est dans les bornes
        assert 10 <= articles <= 100
        
        # Vérifie que la même date donne toujours la même valeur
        assert articles == expected_articles

    def test_get_articles_by_category(self, generator):
        """Test de la méthode get_articles_by_category"""
        date = datetime(2024, 3, 20, 14, 30)
        expected_distribution = {'food': 30,
                                 'wear': 7,
                                 'electronics': 5,
                                 'books': 2,
                                 'sports': 0,
                                 'toys': 0,
                                 'home': 0,
                                 'garden': 0,
                                 'beauty': 0,
                                 'automotive': 0}
        distribution = generator.get_articles_by_category(date)
        
        # Vérifie qu'on a toutes les catégories
        assert set(expected_distribution.keys()) == set(generator.categories)
        
        # Vérifie que la somme correspond au total
        total = generator.get_article(date)
        assert sum(distribution.values()) == total

        assert sum(expected_distribution.values()) == total
        
        # Vérifie que la même date donne toujours la même distribution
        assert distribution == expected_distribution