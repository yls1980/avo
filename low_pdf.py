import pikepdf
import fitz  # PyMuPDF

# Path to the original PDF
input_pdf = "/Users/avotech/PycharmProjects/mainTest/combined_content.pdf"
# Path to save the optimized PDF
output_pdf = "/Users/avotech/PycharmProjects/mainTest/combined_content_optimized.pdf"

def opt1():
    try:
        # Open the PDF
        with pikepdf.Pdf.open(input_pdf) as pdf:
            # Remove unnecessary metadata
            pdf.remove_unreferenced_resources()

            # Save the optimized PDF
            pdf.save(output_pdf)
            print(f"Optimized PDF saved at: {output_pdf}")
    except Exception as e:
        print(f"Error while optimizing PDF: {e}")


import fitz  # PyMuPDF

def compress_pdf(input_path, output_path, image_quality=75):
    """
    Compress images in a PDF file to reduce size.

    Parameters:
        input_path (str): Path to the original PDF file.
        output_path (str): Path to save the compressed PDF.
        image_quality (int): Quality of compressed images (1-100).
    """
    try:
        # Open the original PDF
        doc = fitz.open(input_path)

        # Iterate through each page
        for page_num in range(len(doc)):
            page = doc[page_num]
            images = page.get_images(full=True)

            # Process each image
            for img_index, img in enumerate(images):
                xref = img[0]  # Reference to the image object
                base_image = doc.extract_image(xref)  # Extract image
                image_bytes = base_image["image"]  # Get the image data
                image_ext = base_image["ext"]  # Get the image format (e.g., png, jpeg)

                # Compress the image
                from PIL import Image
                from io import BytesIO

                img_pil = Image.open(BytesIO(image_bytes))
                compressed_image_io = BytesIO()
                img_pil.save(
                    compressed_image_io,
                    format="JPEG",
                    quality=image_quality,  # Set compression quality
                )
                compressed_image_bytes = compressed_image_io.getvalue()

                # Replace the original image with the compressed one
                doc.update_stream(xref, compressed_image_bytes)

        # Save the compressed PDF
        doc.save(output_path)
        print(f"Compressed PDF saved at: {output_path}")

    except Exception as e:
        print(f"Error while compressing PDF: {e}")


compress_pdf(input_pdf, output_pdf, image_quality=50)
