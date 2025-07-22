import hashlib
from datetime import datetime
import random

# Typing helpers
from typing import List, Dict



class MetricsGenerator:
    def __init__(self,
                 min_visitors: int = 10, max_visitors: int = 1000,
                 min_articles: int = 1, max_articles: int = 1000,
                 min_pages_viewed: int = 1, max_pages_viewed: int = 20,
                 categories: List[str] = ["food", 
                                          "wear", 
                                          "electronics", 
                                          "books", 
                                          "sports", 
                                          "toys", 
                                          "home", 
                                          "garden", 
                                          "beauty", 
                                          "automotive"],
                cities : list[str]=["Paris",
                                    "Marseille",
                                    "Lyon",
                                    "Toulouse", 
                                    "Nice",
                                    "Nantes",
                                    "Strasbourg",
                                    "Montpellier",
                                    "Bordeaux",
                                    "Lille"]):
        """
        Initialize the metrics generator.
        
        Args:
            min_visitors: Minimum number of visitors
            max_visitors: Maximum number of visitors
            min_articles: Minimum revenue from articles
            max_articles: Maximum revenue from articles
            min_pages_viewed: Minimum number of pages viewed
            max_pages_viewed: Maximum number of pages viewed
            categories: List of article categories
            cities: List of cities
        """
        self.min_visitors = min_visitors
        self.max_visitors = max_visitors
        self.min_articles = min_articles
        self.max_articles = max_articles
        self.min_pages_viewed = min_pages_viewed
        self.max_pages_viewed = max_pages_viewed
        self.categories = categories
        self.cities = cities
    
    def get_city(self, date: datetime) ->str:
        cities = self.cities
        random.seed(int(date.timestamp()))
        return random.choice(cities)

    def get_visitors(self, date: datetime) -> int:
        """
        Generates a constant number of visitors for a given date and time.
        
        Args:
            date: The date and time for which to generate visitor count
            
        Returns:
            Number of visitors for this date/time
        """
        timestamp = int(date.timestamp())
        last_3_digits = timestamp % 1000
        range_size = self.max_visitors - self.min_visitors
        visitors = self.min_visitors + (last_3_digits * range_size) // 1000
        return visitors
        

    def generate_null_values(self, date: datetime) -> int | None:
        """
        Randomly generates either a null value or a random value.
        
        Args:
            date: The date and time for which to generate visitor count
            
        Returns:
            Either a random value between min_visitors and max_visitors, or None
        """
        if random.choice([True, False]):
            return None
        else:
            return random.randint(self.min_visitors, self.max_visitors)

    def get_pages_viewed(self, date: datetime) -> int:
        """
        Generates number of pages viewed for a given date and time.

        Args:
            date: The date and time for which to generate page views

        Returns:
            Number of pages viewed for this date/time
        """
        date_str = date.strftime("%Y-%m-%d-%H-%M")
        hash_object = hashlib.md5(date_str.encode())
        hash_number = int(hash_object.hexdigest(), 16)
        range_size = self.max_pages_viewed - self.min_pages_viewed
        pages_viewed = self.min_pages_viewed + (hash_number % range_size)
        return pages_viewed

    def get_article(self, date: datetime) -> float:
        """
        Generates random revenue for a given date and time.

        Args:
            date: The date and time for which to generate revenue

        Returns:
            Revenue between 1 and 1000
        """
        date_str = date.strftime("%Y-%m-%d-%H-%M")
        hash_object = hashlib.md5(date_str.encode())
        hash_number = int(hash_object.hexdigest(), 16)
        revenue = 1 + (hash_number % 99900) / 100
        return revenue

    def get_articles_by_category(self, date: datetime) -> Dict[str, float]:
        """
        Returns revenue between 1 and 1000 for each category in a deterministic way.

        Args:
            date: Date and time for which to generate the distribution

        Returns:
            Dictionary mapping category to revenue
        """
        total_revenue = self.get_article(date)
        distribution: Dict[str, float] = {}
        date_str = date.strftime("%Y-%m-%d-%H-%M")
        ratios: Dict[str, float] = {}
        ratio_sum = 0

        for category in self.categories:
            hash_input = f"{date_str}-{category}"
            hash_number = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
            ratio = (hash_number % 100) / 100
            ratios[category] = ratio
            ratio_sum += ratio

        for category in self.categories:
            normalized_ratio = ratios[category] / ratio_sum
            distribution[category] = round(total_revenue * normalized_ratio, 2)

        return distribution
