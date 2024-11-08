import os.path

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from fpdf import FPDF

# To prevent overloading the server, we add a small delay between requests
DELAY = 0.5  # seconds

# Set to store visited URLs to avoid visiting the same page multiple times
visited_urls = set()


def get_text_from_url(url, base_url):
    """Fetches the page content from a URL, parses the text, and returns it along with any found internal links."""
    try:
        response = requests.get(url, verify=False, timeout=10)
        if response.status_code != 200:
            return "", []

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract all text from the webpage
        paragraphs = soup.find_all('p')
        page_text = "\n\n".join(p.get_text(separator=' ', strip=True) for p in paragraphs)

        # Find all internal links
        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urljoin(base_url, href)
            parsed_base = urlparse(base_url)
            parsed_url = urlparse(full_url)

            # Only add the link if it's within the same domain
            if parsed_url.netloc == parsed_base.netloc and full_url not in visited_urls:
                links.add(full_url)

        return page_text, links

    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return "", []


def crawl_website(start_url, max_depth=2):
    """Recursively crawl a website, fetching text and following internal links up to a maximum depth."""
    base_url = start_url
    to_visit = [(start_url, 0)]  # Stack for DFS: (URL, current depth)

    website_text = ""

    while to_visit:
        current_url, depth = to_visit.pop()

        if current_url in visited_urls or depth > max_depth:
            continue

        visited_urls.add(current_url)
        print(f"Visiting: {current_url} (Depth: {depth})")

        # Fetch the page text and internal links
        page_text, internal_links = get_text_from_url(current_url, base_url)
        website_text += page_text + "\n\n"  # Append the page's text to the overall text

        # Add internal links to the stack for further exploration
        for link in internal_links:
            to_visit.append((link, depth + 1))

        time.sleep(DELAY)  # Delay to avoid overloading the server

    return website_text


def save_text_to_pdf(text, pdf_filename):
    # Create a PDF object
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Add a Unicode-compatible font
    pdf.add_font("DejaVuSans", "", r'c:\Users\iskorkin\0\dejavu-fonts-ttf-2.37\dejavu-fonts-ttf-2.37\ttf\DejaVuSans.ttf', uni=True)
    pdf.set_font("DejaVuSans", size=12)

    # Add text to PDF
    pdf.multi_cell(0, 10, text)

    # Save PDF to file
    pdf.output(pdf_filename)


# Main function to fetch, extract, and save website text to PDF
def website_to_pdf(all_text, pdf_filename):
    if all_text:
        save_text_to_pdf(all_text, pdf_filename)
        print(f"Text saved to {pdf_filename}")

def save_text_to_file(text, filename):
    """Saves the provided text to a plain text file."""
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(text)
        print(f"Text successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving text to file: {e}")

# Example usage:
start_url = "https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/landing-index.html"
txt_filename = 'website_content.txt'
if not os.path.exists(txt_filename):
    all_text = crawl_website(start_url, max_depth=3)
    save_text_to_file(all_text, txt_filename)
else:
    with open(txt_filename, 'r', encoding='utf-8') as file:
        all_text = file.read()
pdf_filename = "website_content.pdf"
website_to_pdf(all_text, pdf_filename)