from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
import json
import re
import requests
from bs4 import BeautifulSoup
from forex_python.converter import CurrencyRates
from tld import get_tld
import tld_to_country
import psycopg2
from psycopg2.extras import Json

countries_population = {
    "China": {"population": 1402, "domain": ".cn"},
    "India": {"population": 1380, "domain": ".in"},
    "United States": {"population": 331, "domain": ".us"},
    "Indonesia": {"population": 273, "domain": ".id"},
    "Pakistan": {"population": 220, "domain": ".pk"},
    "Brazil": {"population": 213, "domain": ".br"},
    "Nigeria": {"population": 206, "domain": ".ng"},
    "Bangladesh": {"population": 166, "domain": ".bd"},
    "Russia": {"population": 144, "domain": ".ru"},
    "Mexico": {"population": 128, "domain": ".mx"},
    "Japan": {"population": 125, "domain": ".jp"},
    "Ethiopia": {"population": 114, "domain": ".et"},
    "Philippines": {"population": 110, "domain": ".ph"},
    "Egypt": {"population": 104, "domain": ".eg"},
    "Vietnam": {"population": 97, "domain": ".vn"},
    "DR Congo": {"population": 89, "domain": ".cd"},
    "Turkey": {"population": 84, "domain": ".tr"},
    "Iran": {"population": 83, "domain": ".ir"},
    "Thailand": {"population": 70, "domain": ".th"},
    "France": {"population": 65, "domain": ".fr"},
    "United Kingdom": {"population": 67, "domain": ".uk"},
    "Italy": {"population": 60, "domain": ".it"},
    "South Africa": {"population": 59, "domain": ".za"},
    "Tanzania": {"population": 58, "domain": ".tz"},
    "Myanmar": {"population": 54, "domain": ".mm"},
    "South Korea": {"population": 52, "domain": ".kr"},
    "Colombia": {"population": 51, "domain": ".co"},
    "Kenya": {"population": 54, "domain": ".ke"},
    "Argentina": {"population": 45, "domain": ".ar"},
    "Ukraine": {"population": 44, "domain": ".ua"},
    "Spain": {"population": 47, "domain": ".es"},
    "Uganda": {"population": 46, "domain": ".ug"},
    "Algeria": {"population": 45, "domain": ".dz"},
    "Sudan": {"population": 44, "domain": ".sd"},
    "Iraq": {"population": 41, "domain": ".iq"},
    "Poland": {"population": 38, "domain": ".pl"},
    "Canada": {"population": 38, "domain": ".ca"},
    "Morocco": {"population": 37, "domain": ".ma"},
    "Afghanistan": {"population": 38, "domain": ".af"},
    "Saudi Arabia": {"population": 35, "domain": ".sa"},
    "Uzbekistan": {"population": 34, "domain": ".uz"},
    "Malaysia": {"population": 33, "domain": ".my"},
    "Peru": {"population": 33, "domain": ".pe"},
    "Venezuela": {"population": 33, "domain": ".ve"},
    "Ghana": {"population": 32, "domain": ".gh"},
    "Nepal": {"population": 29, "domain": ".np"},
    "Yemen": {"population": 30, "domain": ".ye"},
    "Mozambique": {"population": 29, "domain": ".mz"},
    "Madagascar": {"population": 26, "domain": ".mg"},
    "Australia": {"population": 25, "domain": ".au"},
    "Cameroon": {"population": 26, "domain": ".cm"},
    "Ivory Coast": {"population": 25, "domain": ".ci"},
    "Niger": {"population": 24, "domain": ".ne"},
    "Sri Lanka": {"population": 24, "domain": ".lk"},
    "Burkina Faso": {"population": 23, "domain": ".bf"},
    "Mali": {"population": 20, "domain": ".ml"},
    "Romania": {"population": 20, "domain": ".ro"},
    "Malawi": {"population": 19, "domain": ".mw"},
    "Kazakhstan": {"population": 19, "domain": ".kz"},
    "Zambia": {"population": 19, "domain": ".zm"},
    "Chile": {"population": 19, "domain": ".cl"},
    "Guatemala": {"population": 18, "domain": ".gt"},
    "Ecuador": {"population": 18, "domain": ".ec"},
    "Netherlands": {"population": 17, "domain": ".nl"},
    "Syria": {"population": 17, "domain": ".sy"},
    "Cambodia": {"population": 16, "domain": ".kh"},
    "Senegal": {"population": 15, "domain": ".sn"},
    "Chad": {"population": 15, "domain": ".td"},
    "Somalia": {"population": 15, "domain": ".so"},
    "Zimbabwe": {"population": 15, "domain": ".zw"},
    "Guinea": {"population": 14, "domain": ".gn"},
    "Rwanda": {"population": 13, "domain": ".rw"},
    "Benin": {"population": 12, "domain": ".bj"},
    "Burundi": {"population": 12, "domain": ".bi"},
    "Tunisia": {"population": 12, "domain": ".tn"},
    "Bolivia": {"population": 11, "domain": ".bo"},
    "Belgium": {"population": 11, "domain": ".be"},
    "Haiti": {"population": 11, "domain": ".ht"},
    "Cuba": {"population": 11, "domain": ".cu"},
    "South Sudan": {"population": 10, "domain": ".ss"},
    "Dominican Republic": {"population": 10, "domain": ".do"},
    "Czech Republic": {"population": 10, "domain": ".cz"},
    "Greece": {"population": 10, "domain": ".gr"},
    "Jordan": {"population": 10, "domain": ".jo"},
    "Portugal": {"population": 10, "domain": ".pt"},
    "Azerbaijan": {"population": 10, "domain": ".az"},
    "Sweden": {"population": 10, "domain": ".se"},
    "Honduras": {"population": 9, "domain": ".hn"},
    "United Arab Emirates": {"population": 9, "domain": ".ae"},
    "Hungary": {"population": 9, "domain": ".hu"},
    "Tajikistan": {"population": 9, "domain": ".tj"},
    "Belarus": {"population": 9, "domain": ".by"},
    "Austria": {"population": 8, "domain": ".at"},
    "Papua New Guinea": {"population": 8, "domain": ".pg"},
    "Serbia": {"population": 7, "domain": ".rs"},
    "Israel": {"population": 7, "domain": ".il"},
    "Switzerland": {"population": 7, "domain": ".ch"},
    "Togo": {"population": 7, "domain": ".tg"},
    "Sierra Leone": {"population": 7, "domain": ".sl"},
    "Hong Kong": {"population": 7, "domain": ".hk"},
    "Laos": {"population": 7, "domain": ".la"},
    "Paraguay": {"population": 7, "domain": ".py"},
    "Bulgaria": {"population": 6, "domain": ".bg"},
    "Libya": {"population": 6, "domain": ".ly"},
    "Lebanon": {"population": 6, "domain": ".lb"},
    "Nicaragua": {"population": 6, "domain": ".ni"},
    "Kyrgyzstan": {"population": 6, "domain": ".kg"},
    "El Salvador": {"population": 6, "domain": ".sv"},
    "Turkmenistan": {"population": 6, "domain": ".tm"}
}
GL_FILE = "/Users/avotech/PycharmProjects/mainTest/google_links.json"
GRES_FILE = "/Users/avotech/PycharmProjects/mainTest/google_results.html"

def upsert_results(connection, data):
    try:
        with connection.cursor() as cursor:
            if 'id' in data and data['id'] is not None:
                # Update existing record
                cursor.execute("""
                    UPDATE results
                    SET title = %s, link = %s, snippet = %s, accessibility = %s, data = %s
                    WHERE id = %s
                    RETURNING id;
                """, (
                data['title'], data['link'], data['snippet'], data['accessibility'], Json(data['data']), data['id']))
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO results (title, link, snippet, accessibility, data)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """, (data['title'], data['link'], data['snippet'], data['accessibility'], Json(data['data'])))

            # Commit the transaction
            connection.commit()
            # Return the ID of the inserted or updated row
            return cursor.fetchone()[0]

    except Exception as e:
        connection.rollback()
        print(f"Error occurred: {e}")
        raise


def upsert_results(data):

    try:
        connection = psycopg2.connect(
            dbname="google_search",
            user="zorro",
            password="rjnjhsq1980",
            host="localhost",
            port=5432
        )
        with connection.cursor() as cursor:
            if 'id' in data and data['id'] is not None:
                # Update existing record
                cursor.execute("""
                    UPDATE results
                    SET title = %s, link = %s, snippet = %s, accessibility = %s, data = %s
                    WHERE id = %s
                    RETURNING id;
                """, (
                data['title'], data['link'], data['snippet'], data['accessibility'], Json(data['data']), data['id']))
            else:
                cursor.execute("""
                    INSERT INTO results (title, link, snippet, accessibility, data)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """, (data['title'], data['link'], data['snippet'], data['accessibility'], Json(data['data'])))

            connection.commit()
            return cursor.fetchone()[0]

    except Exception as e:
        connection.rollback()
        print(f"Error occurred: {e}")
        raise

class CurrencyConverter:
    def __init__(self):
        self.converter = CurrencyRates()
        self.tld_mapping = tld_to_country.TLD_TO_COUNTRY
        self.rate_cache = {}
        self.cache_duration = 3600

    def fetch_rates(self, from_currency):
        url = f"https://open.er-api.com/v6/latest/{from_currency}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if "rates" in data:
                self.rate_cache[from_currency] = {
                    "rates": data["rates"],
                    "timestamp": time.time()
                }
                return data["rates"]
        print(f"Error fetching rate: {response.json().get('error-type', 'Unknown error')}")
        return None

    def get_rates(self, from_currency):
        if (from_currency in self.rate_cache and
            time.time() - self.rate_cache[from_currency]["timestamp"] < self.cache_duration):
            return self.rate_cache[from_currency]["rates"]
        return self.fetch_rates(from_currency)

    def convert_currency_public(self, amount, from_currency, to_currency):
        rates = self.get_rates(from_currency)
        if rates and to_currency in rates:
            return amount * rates[to_currency]
        print(f"Error: Conversion rate for {to_currency} not found.")
        return None

    def convert_to_rubles(self, amount, domain):
        try:
            tld = get_tld(domain, as_object=True).tld.split('.')[-1]
            country_code = self.tld_mapping.get(tld, "Unknown").get("country_code", "Unknown")
            country = self.tld_mapping.get(tld, "Unknown").get("country_name", "Unknown")
            currency_code = self.tld_mapping.get(tld, "Unknown").get("currency_code", "Unknown")

            ##print(f"Detected Country: {country}, Currency: {currency_code}")

            convert_value = self.convert_currency_public(float(amount),currency_code,"RUB")
            return country,  convert_value
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

        prices = re.findall(self.price_pattern, text)
        hardware_specs = re.findall(self.hardware_pattern, text)

        processed_prices = []
        for symbol, amount in prices:
            try:
                amount = amount.replace(",", "")
                country, converted_price = self.currency_converter.convert_to_rubles(amount, self.url)
                processed_prices.append({
                    "original": f"{symbol}{amount}",
                    "in_rubles": f"{converted_price:.2f} ₽" if converted_price else "Conversion Error",
                    "country": country
                })
            except Exception as e:
                print(f"Error processing price {symbol}{amount}: {e}")
                processed_prices.append({
                    "original": f"{symbol}{amount}",
                    "in_rubles": f"Conversion Error{str(e)}",
                    "country": country
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


def duckduckgo_query(query, domain, max_results):
    full_query = f"{query} site:{domain}"
    url = f"https://duckduckgo.com/?q={full_query}"

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode if desired
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=" + random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.111 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.134 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]))

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.get(url)
    time.sleep(2)  # Wait for the page to load

    results = driver.find_elements(By.CSS_SELECTOR, ".result__body")

    with open("duckduckgo_results.html", "w", encoding="utf-8") as f:
        f.write("<html><head><title>DuckDuckGo Search Results</title></head><body>")
        f.write(f"<h1>Results for '{query}' in {domain}</h1>")

        pages = 0
        for idx, result in enumerate(results):
            if pages >= max_results:
                break

            title_element = result.find_element(By.CSS_SELECTOR, ".result__a")
            title = title_element.text
            link = title_element.get_attribute("href")
            snippet_element = result.find_element(By.CSS_SELECTOR, ".result__snippet")
            snippet = snippet_element.text

            try:
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[1])
                driver.get(link)
                time.sleep(1)  # Wait for the link to load

                accessibility = "Accessible"

                driver.close()
                driver.switch_to.window(driver.window_handles[0])

                f.write(f"<h2>{idx + 1}. {title}</h2>")
                f.write(f"<p><a href='{link}'>{link}</a></p>")
                f.write(f"<p>{snippet}</p>")
                f.write(f"<p><strong>Accessibility:</strong> {accessibility}</p><br>")
                pages += 1
            except Exception as e:
                print(f"Failed to load {link}: {e}")
                f.write(f"<h2>{idx + 1}. {title} (Link failed)</h2><br>")

    f.write("</body></html>")
    driver.quit()

def initialize_driver():
    """Initialize and return a new Chrome WebDriver instance with options."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=" + random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.111 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.134 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]))
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def country_link_count(country, output_file=GL_FILE):
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return sum(1 for entry in data if entry.get("country") == country)
    except FileNotFoundError:
        return 0


def google_query(query, country, domain, max_results, output_file=GL_FILE):
    full_query = f"{query} site:{domain}"
    url = f"https://www.google.com/search?q={full_query}"

    driver = initialize_driver()
    new_data = []

    try:
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            checked_urls = {entry["link"] for entry in existing_data if entry.get("checked", False)}
    except FileNotFoundError:
        existing_data = []
        checked_urls = set()

    try:
        driver.get(url)
        time.sleep(2)
        results = driver.find_elements(By.CSS_SELECTOR, ".tF2Cxc")

        for idx, result in enumerate(results):
            print(f'{country} - {idx}')
            if idx >= max_results:
                break

            try:
                title = result.find_element(By.CSS_SELECTOR, "h3").text or "No title"
                link = result.find_element(By.CSS_SELECTOR, "a").get_attribute("href") or "No link"
                snippet = result.find_element(By.CSS_SELECTOR, ".VwiC3b").text or "No snippet"

                if link not in checked_urls:
                    new_data.append({
                        "country": country,
                        "title": title,
                        "link": link,
                        "snippet": snippet,
                        "checked": False
                    })
            except Exception as e:
                print(f"Error processing result: {e}")

    finally:
        driver.quit()

    all_links = {item["link"]: item for item in existing_data + new_data}
    combined_data = list(all_links.values())

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=4)
    print(f"Appended {len(new_data)} new links to {output_file}")


def check_links(input_file=GL_FILE, output_file=GRES_FILE):
    #driver = initialize_driver()
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"No data found in {input_file}")
        return

    with open(output_file, "w", encoding="utf-8") as file:
        file.write("<html><head><title>Checked Google Links</title></head><body>")

        for entry in data:
            if entry["checked"]:
                continue
            link = entry["link"]
            title = entry["title"]
            snippet = entry["snippet"]
            print(f'{snippet} {link}')
            price_data = json.dumps('{}')
            try:
                response = requests.get(link, timeout=10)
                if response.status_code == 200:
                    time.sleep(1)  # Wait to ensure the page loads
                    entry["checked"] = True
                    accessibility = "Accessible"
                    price_data = get_data(link)
                else:
                    accessibility = "Not accessible"
            except Exception as e:
                print(f"Failed to load {link}: {e}")
                accessibility = "Not accessible"

            file.write(f"<h2>{title}</h2>")
            file.write(f"<p><a href='{link}'>{link}</a></p>")
            file.write(f"<p>{snippet}</p>")
            file.write(f"<p><strong>Accessibility:</strong> {accessibility}</p><br>")
            new_data = {
                "title": title,
                "link": link,
                "snippet": snippet,
                "accessibility": accessibility,
                "data": price_data
            }
            upsert_results(new_data)
        file.write("</body></html>")

    with open(input_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    #driver.quit()
    print(f"Results saved to {output_file} and updated {input_file}")


def prepare():
    for country, info in countries_population.items():
        links_count = country_link_count(country, GL_FILE)
        if  links_count>= 10:
            print(f"Skipping {country}: already has 10 or more links.")
            continue
        print(f'{country}')
        google_query(f'VPS hosting in {country}', country, info['domain'], 10-links_count)

def get_data(target_url):
    #target_url = "https://www.nameregistry.org.cn/index-11.html"
    scraper = WebScraper(target_url)

    # Fetch and process webpage data
    html_content = scraper.fetch_webpage()
    if html_content:
        extracted_data = scraper.extract_info(html_content)

        scraper.save_to_json({"extracted_info": extracted_data})
        scraper.print_results(extracted_data)
        return extracted_data
    else:
        return json.dumps('{}')

if __name__ == "__main__":
    check_links()