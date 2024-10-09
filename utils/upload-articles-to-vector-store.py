import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize OpenAI API client with API key from environment variables
openai_api_key = os.getenv('OPENAI_API_KEY')

# Constants
VECTOR_STORE_ID = "vs_I09vB9pr80qOUB7W5LIRBeIo"  # Replace with your actual vector store ID
UPLOAD_URL = "https://api.openai.com/v1/files"
BATCH_URL = f"https://api.openai.com/v1/vector_stores/{VECTOR_STORE_ID}/file_batches"
HEADERS = {
    "Authorization": f"Bearer {openai_api_key}",
    "Content-Type": "application/json",
    "OpenAI-Beta": "assistants=v2"
}

def list_files(purpose=None):
    """List files in the OpenAI API."""
    try:
        url = "https://api.openai.com/v1/files"
        if purpose:
            url += f"?purpose={purpose}"
        
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code

        files = response.json()["data"]
        return files

    except Exception as e:
        print(f"An error occurred while listing the files: {str(e)}")
        return []

def delete_file(file_id):
    """Delete a file from the OpenAI API."""
    try:
        url = f"https://api.openai.com/v1/files/{file_id}"
        response = requests.delete(url, headers=HEADERS)
        response.raise_for_status()

        # Check if the deletion was successful
        if response.json().get("deleted", False):
            print(f"File with ID {file_id} was successfully deleted.")
        else:
            print(f"Failed to delete the file with ID {file_id}.")

    except Exception as e:
        print(f"An error occurred while trying to delete the file: {str(e)}")

def get_markdown_files_from_docs():
    """Get all markdown files from the 'docs' directory."""
    docs_dir = os.path.join(os.getcwd(), 'docs')
    file_paths = [os.path.join(docs_dir, f) for f in os.listdir(docs_dir) if f.endswith('.md')]
    return file_paths

def upload_files_to_openai(file_paths):
    """Upload markdown files to OpenAI API."""
    file_ids = []
    for file_path in file_paths:
        filename = os.path.basename(file_path)  # Extract just the filename
        with open(file_path, 'rb') as f:
            response = requests.post(
                UPLOAD_URL,
                headers={
                    "Authorization": f"Bearer {openai_api_key}",
                },
                files={
                    'file': (filename, f, 'text/markdown')
                },
                data={
                    'purpose': 'user_data'  # Change this if another purpose fits better
                }
            )
            if response.status_code == 200:
                file_id = response.json()['id']
                file_ids.append(file_id)
                print(f"Uploaded {filename} successfully, file_id: {file_id}")
            else:
                print(f"Failed to upload {filename}: {response.text}")
    return file_ids

def create_vector_store_file_batch(file_ids):
    """Create a vector store file batch with the uploaded files."""
    data = {
        "file_ids": file_ids,
        # Optional: "chunking_strategy": {"type": "auto"}
    }
    response = requests.post(
        BATCH_URL,
        headers=HEADERS,
        json=data
    )
    if response.status_code == 200:
        batch_id = response.json()['id']
        print(f"Vector store file batch created successfully, batch_id: {batch_id}")
    else:
        print(f"Failed to create vector store file batch: {response.text}")

def main():
    # Step 1: List and delete existing files in the vector store
    print("Listing and deleting existing files...")
    files = list_files(purpose='user_data')  # Adjust purpose if necessary

    for file in files:
        print(f"Deleting file: {file['filename']} (ID: {file['id']})")
        delete_file(file['id'])

    # Step 2: Get markdown files from the 'docs' directory
    print("Uploading new files to the vector store...")
    file_paths = get_markdown_files_from_docs()

    # Step 3: Upload new files to OpenAI
    file_ids = upload_files_to_openai(file_paths)

    # Step 4: Create a vector store file batch
    if file_ids:
        create_vector_store_file_batch(file_ids)

if __name__ == "__main__":
    main()
