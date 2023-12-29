import os
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from urllib.parse import urljoin

# List of path names
path_names = ["20230929220924_superseded"]  # Add more path names s needed

# Base URL of the directory
base_url = "http://dl.ash2txt.org/full-scrolls/Scroll1.volpkg/paths/"

# Main directory to save downloaded files
main_save_dir = "/home/sean/Desktop/new-run"

# Authentication details
auth = ('registeredusers', 'only')

# Session to persist authentication across requests
session = requests.Session()
session.auth = auth

def download_file(file_url, save_path):
    """Function to download a single file."""
    local_filename = file_url.split('/')[-1]
    file_path = os.path.join(save_path, local_filename)

    print(f"Downloading {local_filename}...")
    with session.get(file_url, stream=True) as file_response:
        file_response.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in file_response.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

def process_path(path_name):
    path_url = urljoin(base_url, f"{path_name}/")
    layers_url = urljoin(path_url, "layers/")

    save_dir = os.path.join(main_save_dir, path_name)
    os.makedirs(save_dir, exist_ok=True)
    layers_save_dir = os.path.join(save_dir, "layers")
    os.makedirs(layers_save_dir, exist_ok=True)

    # Download the mask file
    #mask_url = urljoin(path_url, f"{path_name}_mask.png")
    #download_file(mask_url, save_dir)

    # Get the list of layer files
    response = session.get(layers_url)
    response.raise_for_status()

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')
    file_urls = [urljoin(layers_url, tag['href']) for tag in soup.find_all('a') if tag['href'].endswith('.tif')]

    # Download layer files using multiple threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(lambda url: download_file(url, layers_save_dir), file_urls)

    print(f"Download completed for path: {path_name}")

# Process each path
for path_name in path_names:
    process_path(path_name)

print("All specified paths have been processed.")
