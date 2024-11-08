import requests
from bs4 import BeautifulSoup
from fpdf import FPDF

def fetch_website_text(url):
    # Send a GET request to fetch the HTML content
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to retrieve content from {url}")
        return None

def extract_text(html_content):
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()

    # Extract text from the page
    text = soup.get_text(separator='\n')

    # Clean up text by removing leading/trailing spaces
    lines = (line.strip() for line in text.splitlines())
    text = '\n'.join(line for line in lines if line)
    return text


def save_text_to_pdf(text, pdf_filename):
    # Create a PDF object
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Set font for the PDF
    pdf.set_font("Arial", size=12)

    # Add text to PDF
    pdf.multi_cell(0, 10, text)

    # Save PDF to file
    pdf.output(pdf_filename)


# Main function to fetch, extract, and save website text to PDF
def website_to_pdf(url, pdf_filename):
    html_content = fetch_website_text(url)
    if html_content:
        text = extract_text(html_content)
        save_text_to_pdf(text, pdf_filename)
        print(f"Text saved to {pdf_filename}")


# Example usage
url = "https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/landing-index.html"
pdf_filename = "website_content.pdf"
website_to_pdf(url, pdf_filename)