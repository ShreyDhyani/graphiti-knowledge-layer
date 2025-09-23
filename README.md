## Graphiti Knowledge Layer

Brief guide to extract, normalize, and ingest PDF circulars into Graphiti.

### 1) Setup

```bash
# (optional) create/activate venv
python3 -m venv venv
source venv/bin/activate

# install deps
pip install -r requirements.txt
```

Set required env vars (example):

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your_password_here"
# LLM/embedding (pick one as your Graphiti config requires)
export GOOGLE_API_KEY="your_gemini_api_key"
# or
export OPENAI_API_KEY="your_openai_api_key"
```

Put your PDFs in `pdfs/`.

### 2) (Optional) Raw text extraction

Writes quick previews to `extracted_text/`.

```bash
python3 pdf_extract.py
```

### 3) Normalize PDFs (headers/footers stripped, metadata guessed)

Outputs `.normalized.json` and `.normalized.txt` to `normalized/`.

```bash
python3 normalize_pdfs.py
```

### 4) Map to models and (optionally) ingest into Graphiti

Creates mapped helper files in `normalized/*.mapped.(circular|clauses).json`.

- Dry run (no ingest):

```bash
python3 graphiti_ingest_mapper.py
```

- Ingest episodes into Graphiti (requires Neo4j + API keys):

```bash
python3 graphiti_ingest_mapper.py --ingest
```

### 5) Quick sanity test against Graphiti

Adds a dummy episode and runs a search.

```bash
python3 graphiti_ingestion_test.py
```

### Notes

- Source PDFs: `pdfs/`
- Extracted text: `extracted_text/`
- Normalized artifacts: `normalized/`
- You can run step 4 directly if you already have normalized JSONs.
