import re
import json
import requests
from bs4 import BeautifulSoup
from forex_python.converter import CurrencyRates
from tld import get_tld
import tld_to_country

class CurrencyConverter:
    """
    Handles conversion of currencies to Russian Rubles (₽).
    """
    def __init__(self):
        self.converter = CurrencyRates()
        self.tld_mapping = tld_to_country.TLD_TO_COUNTRY

    def convert_currency_public(self, amount, from_currency, to_currency):
        url = f"https://open.er-api.com/v6/latest/{from_currency}"
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200 and to_currency in data["rates"]:
            rate = data["rates"][to_currency]
            return amount * rate
        else:
            print(f"Error fetching rate: {data.get('error-type', 'Unknown error')}")
            return None

    def convert_to_rubles(self, amount, domain):
        try:
            tld = get_tld(domain, as_object=True).tld.split('.')[-1]
            country_code = self.tld_mapping.get(tld, "Unknown").get("country_code", "Unknown")
            country = self.tld_mapping.get(tld, "Unknown").get("country_name", "Unknown")
            currency_code = self.tld_mapping.get(tld, "Unknown").get("currency_code", "Unknown")

            print(f"Detected Country: {country}, Currency: {currency_code}")

            return self.convert_currency_public(float(amount),currency_code,"RUB")
            #return self.converter.convert(currency_code, "RUB", float(amount))
        except Exception as e:
            print(f"Error converting currency: {e}")
            return None


class WebScraper:
    """
    Handles web scraping, data extraction, and processing.
    """
    def __init__(self, url):
        self.url = url
        self.currency_converter = CurrencyConverter()
        self.currency_symbols = r'[₹$€₽£¥₴₦₨฿₱₪₡₭₮₩₫₲₺₼₸₾₤₳₥₧₢₠₰₯]'
        self.price_pattern = rf'({self.currency_symbols})\s?(\d{{1,3}}(?:,\d{{3}})*(?:\.\d{{2}})?)'
        self.hardware_pattern = r'\b(?:\d+MB|\d+GB|\d+TB|\d+ CORE|\d+-CORE|\d+ Processor Cores|RAM)\b'

    def fetch_webpage(self):
        """Fetches webpage content using requests."""
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching URL: {e}")
            return ""

    def extract_info(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text(separator=" ")

        # Extract prices and hardware specs
        prices = re.findall(self.price_pattern, text)
        hardware_specs = re.findall(self.hardware_pattern, text)

        # Process prices and convert to RUB
        processed_prices = []
        for symbol, amount in prices:
            try:
                amount = amount.replace(",", "")
                converted_price = self.currency_converter.convert_to_rubles(amount, self.url)
                processed_prices.append({
                    "original": f"{symbol}{amount}",
                    "in_rubles": f"{converted_price:.2f} ₽" if converted_price else "Conversion Error"
                })
            except Exception as e:
                print(f"Error processing price {symbol}{amount}: {e}")
                processed_prices.append({
                    "original": f"{symbol}{amount}",
                    "in_rubles": "Conversion Error"
                })

        return {"prices": processed_prices, "hardware_specs": hardware_specs}

    def save_to_json(self, data, filename="prices_and_specs.json"):
        with open(filename, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)

    def print_results(self, extracted_info):
        """Prints the extracted results."""
        print("\nPrices (with conversion to ₽):")
        for price in extracted_info["prices"]:
            print(f"Original: {price['original']}, In RUB: {price['in_rubles']}")
        print("\nHardware Specifications:")
        print(extracted_info["hardware_specs"])


# Main execution
if __name__ == "__main__":
    # Define the target URL
    a= json.dumps('{}')
    target_url = "https://www.nameregistry.org.cn/index-11.html"

    # Initialize the WebScraper
    scraper = WebScraper(target_url)

    # Fetch and process webpage data
    html_content = scraper.fetch_webpage()
    if html_content:
        extracted_data = scraper.extract_info(html_content)

        # Save and display results
        scraper.save_to_json({"extracted_info": extracted_data})
        scraper.print_results(extracted_data)
