from PyPDF2 import PdfReader, PdfWriter
import os


def split_into_equal_parts(pdf_file, output_folder, parts=3):
    """
    Split a PDF into equal parts.

    Parameters:
        pdf_file (str): Path to the input PDF file.
        output_folder (str): Directory to save the split PDF files.
        parts (int): Number of equal parts to split the PDF into.

    Returns:
        None
    """
    try:
        # Ensure the output folder exists
        os.makedirs(output_folder, exist_ok=True)

        # Read the input PDF
        reader = PdfReader(pdf_file)
        total_pages = len(reader.pages)

        # Calculate the number of pages per part
        pages_per_part = total_pages // parts
        remainder = total_pages % parts

        # Split the PDF into parts
        start = 0
        for part in range(parts):
            writer = PdfWriter()

            # Calculate the end page for this part
            end = start + pages_per_part + (1 if part < remainder else 0)

            # Add pages to this part
            for page_num in range(start, end):
                writer.add_page(reader.pages[page_num])

            # Save the split part
            output_pdf = f"{output_folder}/split_part_{part + 1}.pdf"
            with open(output_pdf, "wb") as output_file:
                writer.write(output_file)
            print(f"Created: {output_pdf}")

            start = end  # Update the start page for the next part

    except Exception as e:
        print(f"Error while splitting PDF: {e}")


# Example usage
input_pdf = "/Users/avotech/PycharmProjects/mainTest/combined_content.pdf"
output_folder = "/Users/avotech/Documents/0/"

# Split into 3 parts
split_into_equal_parts(input_pdf, output_folder, parts=3)
