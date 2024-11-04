import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
from pathlib import Path
import argparse

def is_valid_url(url):
    """Check if url belongs to the target domain"""
    parsed = urlparse(url)
    base_domain = parsed.netloc.split('.')[-2:] 
    target_domain = urlparse(url).netloc.split('.')[-2:]
    return base_domain == target_domain

def download_pdf(url, folder='downloaded_pdfs'):
    """Download PDF file and save it to the specified folder"""
    try:
        # Create folder if it doesn't exist
        Path(folder).mkdir(parents=True, exist_ok=True)
        
        # Get filename from URL
        filename = os.path.join(folder, url.split('/')[-1])
        
        # Download file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Check if it's actually a PDF
        if 'application/pdf' in response.headers.get('content-type', '').lower():
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"Successfully downloaded: {filename}")
            return True
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
    return False

def find_pdfs(url, visited=None):
    """Recursively find all PDF links on the website"""
    if visited is None:
        visited = set()
    
    if url in visited or not is_valid_url(url):
        return
    
    visited.add(url)
    
    try:
        # Add headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all links
        for link in soup.find_all('a'):
            href = link.get('href')
            if not href:
                continue
                
            full_url = urljoin(url, href)
            
            # Download if it's a PDF
            if full_url.lower().endswith('.pdf'):
                download_pdf(full_url)
            # # Recursively visit other pages on the same domain
            # elif is_valid_url(full_url) and full_url not in visited:
            #     find_pdfs(full_url, visited)
                
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Crawl a website for PDF files')
    parser.add_argument('--url', type=str, 
                      default='https://www.cn.ca/en/supplier-portal/supplier-portal/policies-and-guidelines',
                      help='Base URL to start crawling from')
    
    args = parser.parse_args()
    print(f"Starting PDF crawler for {args.url}...")
    find_pdfs(args.url)
    print("Finished crawling for PDFs")

if __name__ == "__main__":
    main()