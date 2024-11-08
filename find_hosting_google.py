import random
import time

import requests
from bs4 import BeautifulSoup
import urllib.parse

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


def google_query(query, domain, max_results):
    full_query = f"{query} site:{domain}"

    #url = f"https://www.google.com/search?q={urllib.parse.quote_plus(full_query)}"
    url = f"https://duckduckgo.com/html/?q={urllib.parse.quote_plus(full_query)}"

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.111 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.134 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "DNT": "1",  # Do Not Track Request Header
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "no-cache",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve results {url}")
        time.sleep(random.uniform(5, 10))
        return

    soup = BeautifulSoup(response.text, "html.parser")

    results = soup.select(".tF2Cxc")

    with open("google_results.html", "w", encoding="utf-8") as f:
        # Write the HTML structure
        f.write("<html><head><title>Google Search Results</title></head><body>")
        f.write("<html><head><title>Google Search Results</title></head><body>")
        f.write(f"<h1>Results for '{query}' in {domain}</h1>")

        pages = 0
        for idx, result in enumerate(results, start=1):
            if pages >= max_results:
                break
            title = result.select_one("h3").text if result.select_one("h3") else "No title"
            print(title)
            link = result.select_one("a")["href"]
            snippet = result.select_one(".VwiC3b").text if result.select_one(".VwiC3b") else "No snippet"
            try:
                link_response = requests.head(link, timeout=5)
                if link_response.status_code == 200:
                    f.write(f"<h2>{idx}. {title}</h2>")
                    f.write(f"<p><a href='{link}'>{link}</a></p>")
                    f.write(f"<p>{snippet}</p><br>")
                    pages += 1
                else:
                    accessibility = f"Not accessible (Status: {link_response.status_code})"
            except requests.RequestException:
                accessibility = "Not accessible (Request failed)"

        f.write("</body></html>")

if __name__ == "__main__":
    for country in countries_population:
        google_query(f'VPS hosting in {country}', countries_population[country]['domain'], 10)