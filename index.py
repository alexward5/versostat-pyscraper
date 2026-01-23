from src.classes.SportsRefScraper import SportsRefScraper
from src.utils.df_utils import inspect_df


def main() -> None:
    scraper = SportsRefScraper()

    url = "https://fbref.com/en/comps/9/Premier-League-Stats"
    table_index = 0

    print(f"Scraping table {table_index} from: {url}")
    print("-" * 80)

    df = scraper.scrape_table(url, table_index=table_index)

    inspect_df(df)


if __name__ == "__main__":
    main()
