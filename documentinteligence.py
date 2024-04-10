import os
import PyPDF2
import time
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import ContentFormat, AnalyzeDocumentRequest

# Set your Azure Document Intelligence and Storage details
document_intelligence_endpoint = "https://finnetorecognizer.cognitiveservices.azure.com/"
document_intelligence_key = "49c5fbc4b0fd42e0b7205c6bef9d121c"
storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=storageformreco;AccountKey=G3+65AzgEiQuMYGanmzGx8ndS3RCztkE4coC4NtHee8w64Q9OQhp4+gveL5cDVKORxnyYSPk9SZu+AStq+WOrQ==;EndpointSuffix=core.windows.net"
storage_account_name = "storageformreco"
storage_account_key = "G3+65AzgEiQuMYGanmzGx8ndS3RCztkE4coC4NtHee8w64Q9OQhp4+gveL5cDVKORxnyYSPk9SZu+AStq+WOrQ=="
container_name = "splitpdfs"

# Define paths
initial_pdf_path = "/Users/idanaharoni/Downloads/ocr/Azure/PDFtoJsonInventory/input_pdf/lili.pdf"
split_pdf_dir = "/Users/idanaharoni/Downloads/ocr/Azure/PDFtoJsonInventory/split_pdf_dir"  # Directory for split PDFs
extracted_md_dir = "/Users/idanaharoni/Downloads/ocr/Azure/PDFtoJsonInventory/final_output"  # Directory for Markdown output

# Ensure the directories exist
os.makedirs(split_pdf_dir, exist_ok=True)
os.makedirs(extracted_md_dir, exist_ok=True)

def split_pdf_into_pages(pdf_path, output_dir):
    start_time = time.time()  # Start timing

    with open(pdf_path, 'rb') as infile:
        reader = PyPDF2.PdfReader(infile)
        for i, page in enumerate(reader.pages):
            writer = PyPDF2.PdfWriter()
            writer.add_page(page)

            output_path = os.path.join(output_dir, f'page_{i+1}.pdf')
            with open(output_path, 'wb') as outfile:
                writer.write(outfile)

            yield output_path

    end_time = time.time()  # End timing
    print(f"Time taken to split PDF: {end_time - start_time} seconds")

def upload_pdf_to_blob(pdf_path):
    start_time = time.time()  # Start timing

    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(pdf_path))
    
    with open(pdf_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    end_time = time.time()  # End timing
    print(f"Time taken to upload {os.path.basename(pdf_path)}: {end_time - start_time} seconds")
    
    
    sas_token = generate_blob_sas(account_name=storage_account_name,
                                  container_name=container_name,
                                  blob_name=os.path.basename(pdf_path),
                                  account_key=storage_account_key,
                                  permission=BlobSasPermissions(read=True),
                                  expiry=datetime.utcnow() + timedelta(hours=1))
    
    return f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{os.path.basename(pdf_path)}?{sas_token}"

def analyze_document_and_save_markdown(blob_sas_url, output_file_path):
    document_intelligence_client = DocumentIntelligenceClient(
        endpoint=document_intelligence_endpoint, 
        credential=AzureKeyCredential(document_intelligence_key)
    )

    poller = document_intelligence_client.begin_analyze_document(
        "prebuilt-layout",
        AnalyzeDocumentRequest(url_source=blob_sas_url),  # Correct usage
        output_content_format=ContentFormat.MARKDOWN,
    )
    result = poller.result()

    # Change the file extension from .md to .txt for the output file
    output_file_path = output_file_path.replace('.md', '.txt')

    with open(output_file_path, "w", encoding="utf-8") as output_file:
        output_file.write(result.content)

if __name__ == "__main__":
    for page_path in split_pdf_into_pages(initial_pdf_path, split_pdf_dir):
        blob_sas_url = upload_pdf_to_blob(page_path)
        page_number = os.path.basename(page_path).split('_')[-1].replace('.pdf', '')
        # Specify the output file with a .txt extension instead of .md
        output_file_path = os.path.join(extracted_md_dir, f"extracted_page_{page_number}.txt")
        analyze_document_and_save_markdown(blob_sas_url, output_file_path)
        print(f"Processed and saved content for page {page_number} as TXT")
