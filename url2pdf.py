import base64
import hashlib
import json
import os
import random
import tempfile

import pdfplumber
#import pdfkit
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from PyPDF2 import PdfMerger

# Constants
DELAY = 1  # Delay between requests to avoid overloading the server
MAX_DEPTH = 3  # Maximum depth for crawling

visited_urls = set()
pdf_files = []  # List to keep track of individual PDF files


# def fetch_html(url):
#     """Fetch the HTML content from a URL."""
#     try:
#         response = requests.get(url, verify=False)  # `verify=False` to ignore SSL warnings
#         response.raise_for_status()
#         return response.text
#     except requests.RequestException as e:
#         print(f"Error fetching {url}: {e}")
#         return None

def generate_file_name(url, depth, file_extension):
    """
    Generate a unique file name based on the URL and recursion depth.
    Use a hash of the URL to make it unique and short.
    """
    url_hash = hashlib.md5(url.encode()).hexdigest()  # Create a short hash of the URL
    return f"depth_{depth}_{url_hash}{file_extension}"
def wait_for_page_load(driver):
    # Ждем, пока document.readyState не станет 'complete'
    WebDriverWait(driver, 2).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    print("Страница загружена.")



import pdfplumber

def pdf_to_html_with_links(pdf_path):
    """Convert a PDF file to HTML with links using pdfplumber."""
    try:
        html_content = ""

        # Open the PDF file with pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Extract plain text from the page
                text = page.extract_text()

                # Extract annotations (links, if available)
                annotations = page.annots or []

                # Wrap text in HTML and add links
                page_html = "<div>"
                if text:
                    page_html += "<p>" + text.replace("\n", "<br>") + "</p>"
                for annot in annotations:
                    uri = annot.get("uri", None)
                    if uri:
                        page_html += f'<a href="{uri}">Link</a><br>'
                page_html += "</div>"

                html_content += page_html

        return html_content
    except Exception as e:
        print(f"Error converting PDF to HTML: {e}")
        return None



def accept_cookies(driver):
    try:
        # Wait until the "Accept Cookies" button is present and clickable
        accept_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Accept Cookies']"))  # Adjust the XPath as necessary
        )
        accept_button.click()
        print("Cookies accepted.")
    except Exception as e:
        print(f"Could not find or click the 'Accept Cookies' button: {e}")

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import tempfile
import base64
import time

def fetch_html_with_selenium(url):
    """Fetch the fully rendered HTML content from a URL using Selenium."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run headless Chrome
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("user-agent=" + random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.111 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.134 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]))
    chrome_options.add_argument("test-type")
    prefs = {
        "download.default_directory": "/Users/iskorkin/PycharmProjects/mainTest",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--kiosk-printing")

    # Set up the WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        driver.get(url)
        wait_for_page_load(driver)
        accept_cookies(driver)
        wait_for_page_load(driver)

        print_options = {
            'paperWidth': 8.27,  # A4 size width in inches
            'paperHeight': 11.69,  # A4 size height in inches
            'marginTop': 0,
            'marginBottom': 0,
            'marginLeft': 0,
            'marginRight': 0,
            'printBackground': True,  # Include background graphics
            'preferCSSPageSize': True
        }
        result = driver.execute_cdp_cmd("Page.printToPDF", print_options)
        time.sleep(DELAY)

        try:
            pdf_filename = generate_file_name(start_url, url, ".pdf")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", mode='wb') as temp_file:
                temp_file.write(base64.b64decode(result['data']))
                temp_file_path = temp_file.name
                print(f"Page saved as PDF: {temp_file_path}")
                html_content = pdf_to_html_with_links(temp_file_path)
                pdf_files.append(temp_file_path)
        except Exception as e:
            print(f"Error saving PDF: {e}")

        with open('tmp_file.html', 'w', encoding='utf-8') as file:
            file.write(html_content)
        return html_content
    except Exception as e:
        print(f"Error fetching {url} with Selenium: {e}")
        return None
    finally:
        driver.quit()



def extract_internal_links(base_url, html):
    """Extract internal links from HTML content."""
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc == urlparse(base_url).netloc:
            links.add(full_url)
    return links


def crawl_website(start_url, depth=0):
    """Recursively crawl the website and collect HTML content."""
    if start_url in visited_urls or depth > MAX_DEPTH:
        return

    visited_urls.add(start_url)
    print(f"Visiting: {start_url} (Depth: {depth})")

    #html = fetch_html(start_url)
    html_content = fetch_html_with_selenium(start_url)
    if not html_content:
        return

    # Recursively crawl internal links
    internal_links = extract_internal_links(start_url, html_content)
    for link in internal_links:
        time.sleep(DELAY)  # Delay to avoid overloading the server
        crawl_website(link, depth + 1)



def merge_pdfs(pdf_files, output_filename):
    """Merge multiple PDF files into one."""
    try:
        merger = PdfMerger()
        for pdf in pdf_files:
            merger.append(pdf)
        merger.write(output_filename)
        merger.close()
        for pdf in pdf_files:
            if os.path.exists(pdf):
                os.remove(pdf)
        print(f"PDFs successfully merged into {output_filename}")
    except Exception as e:
        print(f"Error merging PDFs: {e}")


# Example usage
#start_url = "https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/landing-index.html"
start_url = "https://automate-dv.readthedocs.io/en/latest/"
crawl_website(start_url)
merge_pdfs(pdf_files, "combined_content.pdf")
