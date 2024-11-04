import json
from pathlib import Path
from pypdf import PdfReader
import argparse

def parse_pdf_to_json(pdf_path):
    """Convert a PDF file to a JSON format containing text content and essential metadata"""
    try:
        # Read PDF file
        reader = PdfReader(pdf_path)
        metadata = reader.metadata
        
        # Create dictionary with essential metadata only
        pdf_content = {
            "filename": Path(pdf_path).stem,
            "title": metadata.get("/Title", ""),
            "author": metadata.get("/Author", ""),
            "num_pages": len(reader.pages),
            "pages": []
        }
        
        # Extract text from each page
        for page_num, page in enumerate(reader.pages):
            pdf_content["pages"].append({
                "page_number": page_num + 1,
                "text": page.extract_text()
            })
        
        # Save to JSON file
        output_path = Path(pdf_path).parent / "json_output"
        output_path.mkdir(exist_ok=True)
        
        json_path = output_path / f"{pdf_content['filename']}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(pdf_content, f, indent=2, ensure_ascii=False)
            
        print(f"Successfully parsed: {pdf_path} -> {json_path}")
        return True
        
    except Exception as e:
        print(f"Error parsing {pdf_path}: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Convert PDF files to JSON')
    parser.add_argument('--pdf_dir', type=str, 
                      default='downloaded_pdfs',
                      help='Directory containing PDF files')
    
    args = parser.parse_args()
    pdf_dir = Path(args.pdf_dir)
    
    if not pdf_dir.exists():
        print(f"Directory not found: {pdf_dir}")
        return
    
    print(f"Starting PDF to JSON conversion in {pdf_dir}...")
    
    # Process all PDFs in the directory
    pdf_files = list(pdf_dir.glob('*.pdf'))
    
    if not pdf_files:
        print("No PDF files found in directory")
        return
    
    successful = 0
    for pdf_file in pdf_files:
        if parse_pdf_to_json(pdf_file):
            successful += 1
    
    print(f"Conversion complete. Successfully processed {successful}/{len(pdf_files)} files")

if __name__ == "__main__":
    main()