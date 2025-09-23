# pdf_extract.py
import os
import re
import json
from datetime import datetime, timezone
import pdfplumber

PDF_DIR = "pdfs"
OUT_DIR = "extracted_text"
os.makedirs(OUT_DIR, exist_ok=True)

def clean_text(text: str) -> str:
    """Normalize whitespace, remove repeated blank lines and weird control chars."""
    if not text:
        return ""
    text = text.replace("\r", "\n")
    # collapse multiple spaces
    text = re.sub(r"[ \t]+", " ", text)
    # collapse many newlines into two
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    # strip leading/trailing whitespace
    return text.strip()

def extract_pdf_text(path: str) -> (str, dict):
    """Return (cleaned_text, metadata) for a PDF file."""
    pages = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                pages.append(page_text or "")
        full_text = "\n\n".join(pages)
    except Exception as e:
        full_text = ""
        print(f"Error reading {path}: {e}")

    cleaned = clean_text(full_text)
    stat = os.stat(path)
    meta = {
        "filename": os.path.basename(path),
        "page_count": len(pages),
        "file_size_bytes": stat.st_size,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
    return cleaned, meta

def process_all():
    files = sorted([f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")])
    if not files:
        print(f"No PDFs found in '{PDF_DIR}'. Put your files there and re-run.")
        return
    for fname in files:
        path = os.path.join(PDF_DIR, fname)
        print("Processing:", fname)
        text, meta = extract_pdf_text(path)

        # write JSON (metadata + text)
        out_json = os.path.join(OUT_DIR, fname + ".json")
        with open(out_json, "w", encoding="utf-8") as j:
            json.dump({"metadata": meta, "text": text}, j, ensure_ascii=False, indent=2)

        # write plain text preview
        out_txt = os.path.join(OUT_DIR, fname + ".txt")
        with open(out_txt, "w", encoding="utf-8") as t:
            t.write(text[:200000])  # truncate very large files for preview

        print(f" -> wrote {out_json} ({meta['page_count']} pages)")

if __name__ == "__main__":
    process_all()
