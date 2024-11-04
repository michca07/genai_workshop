import json
from pathlib import Path
from pypdf import PdfReader
from pyspark.sql.functions import col, struct, array
from langchain.text_splitter import RecursiveCharacterTextSplitter

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def parse_pdf_to_json(pdf_path, json_path):
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
        output_path = Path(json_path)
        output_path.mkdir(exist_ok=True)
        
        file_json_path = output_path / f"{pdf_content['filename']}.json"
        with open(file_json_path, 'w', encoding='utf-8') as f:
            json.dump(pdf_content, f, indent=2, ensure_ascii=False)
            
        print(f"Successfully parsed: {pdf_path} -> {file_json_path}")
        return True
        
    except Exception as e:
        print(f"Error parsing {pdf_path}: {str(e)}")
        return False
    
def chunk_json_data_to_df(
  data, 
  chunk_size=500, 
  chunk_overlap=50):
    filename = data.get('filename','')
    num_pages = data.get('num_pages','')
    all_rows = []
    for page_idx in range(data['num_pages']):
        text = data.get('pages')[page_idx].get('text','')

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
        )
        split_texts = text_splitter.split_text(text)

        rows = [
            (filename, num_pages, page_idx+1, i, text)
            for i, text in enumerate(split_texts)
        ]

        all_rows += rows

    df = spark.createDataFrame(all_rows, ["filename", "num_pages", "page_idx", "chunk_idx", "text"])

    return df