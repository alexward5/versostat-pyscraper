from src.classes.SportsRefScraper import SportsRefScraper
from src.utils.df_utils import inspect_df
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def main() -> None:
    scraper = SportsRefScraper()

    url = "https://fbref.com/en/comps/9/Premier-League-Stats"
    table_index = 0

    logger.info("Scraping table %s from: %s", table_index, url)
    logger.info("-" * 80)

    df = scraper.scrape_table(url, table_index=table_index)

    inspect_df(df)


if __name__ == "__main__":
    main()
