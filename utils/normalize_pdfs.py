# normalize_pdfs.py
import os
import re
import json
from collections import Counter
from datetime import datetime, timezone
import pdfplumber
import dateparser

PDF_DIR = "pdfs"
OUT_DIR = "normalized"
os.makedirs(OUT_DIR, exist_ok=True)

def extract_pages_and_tables(path):
    pages = []
    tables = []
    with pdfplumber.open(path) as pdf:
        for p in pdf.pages:
            text = p.extract_text() or ""
            pages.append(text)
            # capture any small tables as CSV-like strings
            for tab in p.extract_tables():
                rows = ["\t".join([cell or "" for cell in row]) for row in tab]
                tables.append("\n".join(rows))
    return pages, tables

def detect_repeating_headers_footers(pages, head_lines=3, tail_lines=3, threshold_frac=0.5):
    n = len(pages)
    head_cnt = Counter()
    tail_cnt = Counter()
    for pg in pages:
        lines = [ln.strip() for ln in (pg or "").splitlines() if ln.strip()]
        if not lines:
            continue
        for h in lines[:head_lines]:
            head_cnt[h] += 1
        for t in lines[-tail_lines:]:
            tail_cnt[t] += 1
    head_cut = {k for k,v in head_cnt.items() if v >= max(1, int(n * threshold_frac))}
    tail_cut = {k for k,v in tail_cnt.items() if v >= max(1, int(n * threshold_frac))}
    return head_cut, tail_cut

def strip_headers_footers_from_page(page_text, head_cut, tail_cut):
    if not page_text:
        return ""
    lines = [ln for ln in page_text.splitlines()]
    # remove repeating head lines
    while lines and lines[0].strip() in head_cut:
        lines.pop(0)
    # remove repeating tail lines
    while lines and lines[-1].strip() in tail_cut:
        lines.pop(-1)
    return "\n".join(lines).strip()

def guess_title_and_date(first_page_text):
    lines = [ln.strip() for ln in (first_page_text or "").splitlines() if ln.strip()]
    title = lines[0] if lines else ""
    # try common date patterns
    date = None
    m = re.search(r'((?:\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4})|(?:\d{4}-\d{2}-\d{2})|(?:\d{1,2}/\d{1,2}/\d{2,4}))', first_page_text, re.I)
    if m:
        parsed = dateparser.parse(m.group(1))
        if parsed:
            date = parsed.date().isoformat()
    if not date:
        parsed = dateparser.parse(first_page_text[:300])
        if parsed:
            date = parsed.date().isoformat()
    return title, date

def detect_lists(text):
    """
    Detect simple bullet/numbered lists blocks. Return list of snippet examples.
    """
    bullets = re.findall(r'(^\s*[-•\u2022]\s+.+$)', text, flags=re.M)
    numbers = re.findall(r'(^\s*\d+\.\s+.+$)', text, flags=re.M)
    # return a small sample (unique)
    samples = []
    for s in (bullets + numbers):
        s2 = s.strip()
        if s2 not in samples:
            samples.append(s2)
            if len(samples) >= 10:
                break
    return samples

def normalize_pdf(path):
    pages, tables = extract_pages_and_tables(path)
    head_cut, tail_cut = detect_repeating_headers_footers(pages)
    cleaned_pages = [strip_headers_footers_from_page(p, head_cut, tail_cut) for p in pages]
    full_text = "\n\n".join([p for p in cleaned_pages if p])
    title, date = guess_title_and_date(cleaned_pages[0] if cleaned_pages else full_text)
    lists = detect_lists(full_text)
    meta = {
        "filename": os.path.basename(path),
        "page_count": len(pages),
        "title": title,
        "date": date,
        "normalized_at": datetime.now(timezone.utc).isoformat(),
        "headers_detected": list(head_cut)[:10],
        "footers_detected": list(tail_cut)[:10],
    }
    return {
        "metadata": meta,
        "normalized_text": full_text,
        "tables": tables,
        "lists": lists,
    }

def main():
    pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")])
    if not pdfs:
        print(f"No PDFs found in {PDF_DIR}")
        return
    for f in pdfs:
        p = os.path.join(PDF_DIR, f)
        print("Normalizing:", f)
        out = normalize_pdf(p)
        out_path = os.path.join(OUT_DIR, f + ".normalized.json")
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False, indent=2)
        # also write a plain text preview
        txt_path = os.path.join(OUT_DIR, f + ".normalized.txt")
        with open(txt_path, "w", encoding="utf-8") as fh:
            fh.write(out["normalized_text"][:200000])
        print(" -> wrote", out_path)

if __name__ == "__main__":
    main()
