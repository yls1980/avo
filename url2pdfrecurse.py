import hashlib
import base64
import sys
import time
import os
import random
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from PyPDF2 import PdfMerger
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import defaultdict

class PDFCrawler:
    DELAY = 0.5  # Delay between requests to avoid overloading the server
    MAX_DEPTH = 5  # Maximum depth for crawling

    def __init__(self):
        self.visited_urls = set()
        self.pdf_files = []
        self.site_structure = {}
        self.driver = None
        self.bad_files = set()
        self.directory = ''

    def generate_file_name(self, url, depth, file_extension):
        """Generate a unique file name based on the URL and recursion depth."""
        url_hash = hashlib.md5(url.encode()).hexdigest()  # Create a short hash of the URL
        return f"depth_{depth}_{url_hash}{file_extension}"

    def wait_for_page_load(self):
        """Wait for the page to fully load by checking the document.readyState."""
        WebDriverWait(self.driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        print("Page fully loaded.")

    def accept_cookies(self):
        """Accept cookies if the dialog is present."""
        try:
            accept_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[text()='Accept Cookies']"))
            )
            accept_button.click()
            print("Cookies accepted.")
        except Exception:
            print("Could not find or click the 'Accept Cookies' button.")

    def get_driver(self):
        """Set up the WebDriver."""
        if self.driver is None:
            print("Setting up the WebDriver.")
            chrome_options = Options()
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--ignore-certificate-errors")
            chrome_options.add_argument("--disable-extensions")
            prefs = {
                "download.default_directory": r'c:\Users\iskorkin\PycharmProjects\mainTest',
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_argument('--kiosk-printing')

            #chromedriver_path = r'c:\Users\iskorkin\0\chromedriver-win64\chromedriver.exe'
            chromedriver_path = r'/Users/avotech/yarik/0/driver/chromedriver-mac-x64/chromedriver'
            service = Service(chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def fetch_pdf_with_selenium(self, url, depth):
        try:
            self.get_driver()
            print(f"Fetching PDF for {url} (Depth: {depth})")
            self.driver.get(url)
            self.wait_for_page_load()
            self.accept_cookies()
            time.sleep(random.uniform(2, 5))

            print_options = {
                'paperWidth': 8.27,  # A4 size width in inches
                'paperHeight': 11.69,  # A4 size height in inches
                'marginTop': 0,
                'marginBottom': 0,
                'marginLeft': 0,
                'marginRight': 0,
                'printBackground': True,
                'preferCSSPageSize': True
            }

            result = self.driver.execute_cdp_cmd("Page.printToPDF", print_options)
            pdf_filename = os.path.join(os.getcwd(), '00', self.generate_file_name(url, depth, ".pdf"))

            os.makedirs(os.path.dirname(pdf_filename), exist_ok=True)
            self.directory = os.path.dirname(pdf_filename)
            with open(pdf_filename, 'wb') as file:
                file.write(base64.b64decode(result['data']))

            print(f"Page saved as PDF: {pdf_filename}")
            self.pdf_files.append(pdf_filename)
            html_content = self.driver.page_source
            return pdf_filename, html_content

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"Error fetching PDF for {url}: {e}")
            print(exc_type, exc_tb.tb_lineno)
            return None, None

    def extract_internal_links(self, base_url, html):
        """Extract internal links from HTML content."""
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urljoin(base_url, href)
            if urlparse(full_url).netloc == urlparse(base_url).netloc:
                links.add(full_url)
        return links

    def crawl_website(self, start_url, depth=0):
        """Recursively crawl the website and collect PDF files and structure."""
        if start_url in self.visited_urls or depth > self.MAX_DEPTH:
            return None

        self.visited_urls.add(start_url)
        print(f"Visiting: {start_url} (Depth: {depth})")

        pdf_filename, html_content = self.fetch_pdf_with_selenium(start_url, depth)
        if not pdf_filename:
            return None

        internal_links = self.extract_internal_links(start_url, html_content)

        # Use a ThreadPoolExecutor to crawl internal links concurrently
        children = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_link = {executor.submit(self.crawl_website, link, depth + 1): link for link in internal_links}
            for future in concurrent.futures.as_completed(future_to_link):
                link = future_to_link[future]
                try:
                    child_structure = future.result()
                    if child_structure:
                        children.append(child_structure)
                except Exception as e:
                    print(f"Error crawling link {link}: {e}")

        page_structure = {
            "url": start_url,
            "pdf_file": pdf_filename,
            "children": children
        }

        self.site_structure[start_url] = page_structure
        return page_structure



    def find_large_duplicate_files(self):
        if not self.directory:
            return
        size_to_files = defaultdict(list)  # Словарь для хранения файлов по их размеру

        for root, _, files in os.walk(self.directory):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    size_to_files[file_size].append(file_path)

        for file_size, file_list in size_to_files.items():
            if len(file_list) > 10:
                self.bad_files.update(file_list)


    def merge_pdfs_with_structure(self, merger=None):
        """Merge multiple PDF files into one, respecting their hierarchical structure."""
        if merger is None:
            merger = PdfMerger()

        def merge_recursively(page_structure):
            pdf_file = page_structure.get("pdf_file")
            if pdf_file and os.path.exists(pdf_file):
                if pdf_file not in self.bad_files:
                    print(f"Merging {pdf_file}")
                    merger.append(pdf_file)

            for child in page_structure.get("children", []):
                merge_recursively(child)

        merge_recursively(self.site_structure)
        return merger

    def save_and_merge_all_pdfs(self, output_filename):
        """Merge PDFs and save them to the output filename."""
        try:
            merger = self.merge_pdfs_with_structure()
            merger.write(output_filename)
            merger.close()
            print(f"PDFs successfully merged into {output_filename}")
        except Exception as e:
            print(f"Error merging PDFs: {e}")

    def run(self, start_url):
        self.crawl_website(start_url)
        self.find_large_duplicate_files()
        self.save_and_merge_all_pdfs("combined_content.pdf")


if __name__ == "__main__":
    #start_url = "https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/landing-index.html"
    start_url = "https://automate-dv.readthedocs.io/en/latest/"
    crawler = PDFCrawler()
    crawler.run(start_url)
