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

BULLET_RE = re.compile(r"^\s*(?:[-•\u2022]|\(\w+\)|\d+[\.\)])\s+")
DATE_RE = re.compile(
    r"((?:\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4})|"
    r"(?:\d{4}-\d{2}-\d{2})|"
    r"(?:\d{1,2}/\d{1,2}/\d{2,4}))",
    re.I,
)

def extract_pages(path):
    pages = []
    with pdfplumber.open(path) as pdf:
        for p in pdf.pages:
            text = p.extract_text() or ""
            tables_raw = []
            for tab in (p.extract_tables() or []):
                rows = [[cell or "" for cell in row] for row in tab]
                # TSV and simple Markdown render
                tsv = "\n".join(["\t".join(r) for r in rows])
                md = "\n".join(
                    ["| " + " | ".join(r) + " |" for r in rows]
                )
                tables_raw.append(
                    {
                        "rows": rows,
                        "tsv": tsv,
                        "markdown": md,
                    }
                )
            pages.append({"text": text, "tables": tables_raw})
    return pages

def detect_repeating_headers_footers(pages, head_lines=3, tail_lines=3, threshold_frac=0.5):
    n = len(pages)
    head_cnt = Counter()
    tail_cnt = Counter()
    for pg in pages:
        lines = [ln.strip() for ln in (pg["text"] or "").splitlines() if ln.strip()]
        if not lines:
            continue
        for h in lines[:head_lines]:
            head_cnt[h] += 1
        for t in lines[-tail_lines:]:
            tail_cnt[t] += 1
    head_cut = {k for k, v in head_cnt.items() if v >= max(1, int(n * threshold_frac))}
    tail_cut = {k for k, v in tail_cnt.items() if v >= max(1, int(n * threshold_frac))}
    return head_cut, tail_cut

def strip_headers_footers_from_page(page_text, head_cut, tail_cut):
    if not page_text:
        return ""
    lines = [ln for ln in page_text.splitlines()]
    while lines and lines[0].strip() in head_cut:
        lines.pop(0)
    while lines and lines[-1].strip() in tail_cut:
        lines.pop(-1)
    return "\n".join(lines).strip()

def guess_title_and_date(first_page_text):
    lines = [ln.strip() for ln in (first_page_text or "").splitlines() if ln.strip()]
    title = lines[0] if lines else ""
    date = None
    m = DATE_RE.search(first_page_text or "")
    if m:
        parsed = dateparser.parse(m.group(1))
        if parsed:
            date = parsed.date().isoformat()
    if not date:
        parsed = dateparser.parse((first_page_text or "")[:300])
        if parsed:
            date = parsed.date().isoformat()
    return title, date

def split_paragraphs(text):
    # Split on blank lines; keep intra-paragraph newlines (e.g., wrapped bullets)
    blocks = re.split(r"\n\s*\n", text.strip()) if text.strip() else []
    return [b.strip() for b in blocks if b.strip()]

def block_to_segments(block):
    """
    Turn a block into list of segments:
    - list-item segments if most lines look like bullets
    - otherwise a single paragraph segment
    """
    lines = [ln.rstrip() for ln in block.splitlines() if ln.strip()]
    if not lines:
        return []

    bullet_mask = [bool(BULLET_RE.match(ln)) for ln in lines]
    # If at least ~40% lines look like bullets, emit as list items
    if lines and sum(bullet_mask) >= max(1, int(0.4 * len(lines))):
        segs = []
        cur = []
        for ln in lines:
            if BULLET_RE.match(ln):
                # flush previous item
                if cur:
                    segs.append({"type": "list-item", "text": " ".join(cur).strip()})
                    cur = []
                # remove bullet token
                segs.append({"type": "list-item", "text": BULLET_RE.sub("", ln).strip()})
            else:
                # continuation of previous bullet item (wrapped line)
                if segs and segs[-1]["type"] == "list-item":
                    segs[-1]["text"] = (segs[-1]["text"] + " " + ln.strip()).strip()
                else:
                    cur.append(ln.strip())
        if cur:
            segs.append({"type": "paragraph", "text": " ".join(cur).strip()})
        return [s for s in segs if s["text"]]
    else:
        return [{"type": "paragraph", "text": " ".join(lines).strip()}]

def normalize_pdf(path):
    raw_pages = extract_pages(path)
    head_cut, tail_cut = detect_repeating_headers_footers(raw_pages)

    # Clean page text and build segments
    segments = []
    cleaned_pages_text = []
    for i, page in enumerate(raw_pages, start=1):
        cleaned = strip_headers_footers_from_page(page["text"], head_cut, tail_cut)
        cleaned_pages_text.append(cleaned)

        # text -> paragraph/list-item segments
        for block in split_paragraphs(cleaned):
            for seg in block_to_segments(block):
                seg["page"] = i
                segments.append(seg)

        # tables -> table segments
        for t in page["tables"]:
            # prefer TSV for LLMs; keep markdown for preview
            segments.append(
                {
                    "type": "table",
                    "page": i,
                    "format": "tsv",
                    "text": t["tsv"],
                    "markdown_preview": t["markdown"][:5000],  # guard size
                }
            )

    # Build normalized_text and page_spans (offset mapping)
    normalized_text_parts = []
    page_spans = []
    cursor = 0
    for i, cleaned in enumerate(cleaned_pages_text, start=1):
        part = (cleaned + "\n\n") if cleaned else ""
        if part:
            start = cursor
            normalized_text_parts.append(part)
            cursor += len(part)
            page_spans.append({"page": i, "start": start, "end": cursor})

    full_text = "".join(normalized_text_parts)

    title, date = guess_title_and_date(cleaned_pages_text[0] if cleaned_pages_text else full_text)

    meta = {
        "filename": os.path.basename(path),
        "page_count": len(raw_pages),
        "title": title,
        "date": date,
        "normalized_at": datetime.now(timezone.utc).isoformat(),
        "headers_detected": list(head_cut)[:10],
        "footers_detected": list(tail_cut)[:10],
    }

    # Small list samples (optional)
    list_samples = []
    for s in segments:
        if s["type"] == "list-item":
            list_samples.append(s["text"])
            if len(list_samples) >= 10:
                break

    return {
        "metadata": meta,
        "normalized_text": full_text,   # still here for quick previews
        "segments": segments,           # <-- structured blocks with type + page
        "page_spans": page_spans,       # <-- char offset -> page mapping for full_text
        "tables": [s for s in segments if s["type"] == "table"],  # convenience
        "lists": list_samples,          # quick examples of detected bullets
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
        # Also write a plain text preview
        txt_path = os.path.join(OUT_DIR, f + ".normalized.txt")
        with open(txt_path, "w", encoding="utf-8") as fh:
            fh.write(out.get("normalized_text", "")[:200000])
        print(" -> wrote", out_path)

if __name__ == "__main__":
    main()
