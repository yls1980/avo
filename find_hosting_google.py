from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
import json

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

def country_link_count(country, output_file="google_links.json"):
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return sum(1 for entry in data if entry.get("country") == country)
    except FileNotFoundError:
        return 0


def google_query(query, country, domain, max_results, output_file="google_links.json"):
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


def check_links(input_file="google_links.json", output_file="google_results.html"):
    driver = initialize_driver()

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

            try:
                driver.get(link)
                time.sleep(1)  # Wait to ensure the page loads
                entry["checked"] = True
                accessibility = "Accessible"
            except Exception as e:
                print(f"Failed to load {link}: {e}")
                accessibility = "Not accessible"

            file.write(f"<h2>{title}</h2>")
            file.write(f"<p><a href='{link}'>{link}</a></p>")
            file.write(f"<p>{snippet}</p>")
            file.write(f"<p><strong>Accessibility:</strong> {accessibility}</p><br>")

        file.write("</body></html>")

    with open(input_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    driver.quit()
    print(f"Results saved to {output_file} and updated {input_file}")


def prepare():
    for country, info in countries_population.items():
        links_count = country_link_count(country, "google_links.json")
        if  links_count>= 10:
            print(f"Skipping {country}: already has 10 or more links.")
            continue
        print(f'{country}')
        google_query(f'VPS hosting in {country}', country, info['domain'], 10-links_count)

if __name__ == "__main__":
    check_links()