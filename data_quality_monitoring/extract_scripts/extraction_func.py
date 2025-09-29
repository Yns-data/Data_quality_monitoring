# type: ignore
from typing import Optional
from datetime import datetime
import asyncio
import aiohttp
from aiohttp import ClientTimeout
from data_quality_monitoring.sensor_scripts.sensor import MetricsGenerator
from functools import lru_cache
from logging import Logger

# Configuration
TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 10


# Initialize metrics generator once
metrics_generator = MetricsGenerator()

@lru_cache(maxsize=100)
def format_date(date: datetime) -> str:
    """Cache date formatting for better performance with repeated dates."""
    return date.strftime("%Y-%m-%d-%H")

async def make_request(session: aiohttp.ClientSession, endpoint: str, params: dict, logger: Logger, base_url:str) -> dict:
    """Make an HTTP request with retry logic and error handling."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(f"{base_url}/{endpoint}", params=params) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Successfully fetched data from {endpoint}")
                return data
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Failed to fetch {endpoint} after {MAX_RETRIES} attempts: {str(e)}")
                raise
            await asyncio.sleep(2 ** attempt)  # Exponential backoff

async def get_dataframe_async(dates: Optional[datetime|list[datetime]],
                              logger: Logger,
                              base_url:str,
                            dico: Optional[dict] = None) -> dict:
    """Asynchronous version of get_dataframe with improved error handling and performance."""
    if dico is None:
        dico = {
            "dates": [], "visitors": [], "pages_viewed": [], "cities": [],
            "food_articles": [], "wear_articles": [], "electronics_articles": [],
            "sports_articles": [], "toys_articles": [], "home_articles": [],
            "garden_articles": [], "beauty_articles": [], "automotive_articles": []
        }

    # Validate and format dates
    if isinstance(dates, datetime):
        dates_str = [format_date(dates)]
    elif isinstance(dates, list):
        if not all(isinstance(d, datetime) for d in dates):
            raise ValueError("All elements in dates list must be datetime objects")
        dates_str = [format_date(d) for d in dates]
    else:
        raise ValueError("dates must be a datetime object or list of datetime objects")

    params = {"dates": dates_str}
    timeout = ClientTimeout(total=TIMEOUT_SECONDS)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Prepare all requests
        article_tasks = [
            make_request(session, f"articles/{cat}", params, logger, base_url)
            for cat in metrics_generator.categories
        ]
        
        other_endpoint_tasks = [
            make_request(session, endpoint, params, logger, base_url)
            for endpoint in ["visitors", "pages_viewed", "cities"]
        ]

        # Execute all requests concurrently
        all_responses = await asyncio.gather(
            *article_tasks, *other_endpoint_tasks,
            return_exceptions=True
        )
        logger.info("Successfully gathered all responses for all endpoints")

        # Process article responses
        dico_articles = {}
        for cat, response in zip(metrics_generator.categories, all_responses[:len(metrics_generator.categories)]):
            if isinstance(response, Exception):
                logger.error(f"Failed to fetch articles for category {cat}: {str(response)}")
                continue
            
            key = f"{cat}_articles"
            if key in response:
                dico_articles[key] = response[key]
                logger.info(f"Successfully processed articles for category {cat}")

        # Process other endpoint responses
        other_responses = all_responses[len(metrics_generator.categories):]
        endpoints = ["visitors", "revenue", "pages_viewed", "cities"]
        
        for endpoint, response in zip(endpoints, other_responses):
            if isinstance(response, Exception):
                logger.error(f"Failed to fetch {endpoint}: {str(response)}")
                continue
            dico_articles.update(response)
            logger.info(f"Successfully processed {endpoint} data")

        # Update main dictionary
        for key in dico:
            if key in dico_articles:
                dico[key].extend(dico_articles[key])

        logger.info("Successfully completed data processing")
        return dico

def get_dataframe(
    dates: Optional[datetime | list[datetime]],
    logger: Logger,
    base_url: str,
    dico: Optional[dict] = None
) -> dict:
    """Synchronous wrapper for backward compatibility."""
    try:
        # if a loop exists (ex: Jupyter, FastAPI)
        loop = asyncio.get_running_loop()
        # creating a new loop in a seprated thread to avoid conflicts
        new_loop = asyncio.new_event_loop()
        result = new_loop.run_until_complete(
            get_dataframe_async(dates, logger, base_url, dico)
        )
        new_loop.close()
        logger.info(f"Executed in running event loop for {dates}")
        return result

    except RuntimeError:
        # if there is no loop (standard script execution)
        try:
            result = asyncio.run(get_dataframe_async(dates, logger, base_url, dico))
            logger.info(f"Executed with asyncio.run() for {dates}")
            return result
        except Exception as e:
            logger.error(f"Error executing get_dataframe for {dates}: {e}")