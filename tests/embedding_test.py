
import os, asyncio
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

async def t():
    r = await client.embeddings.create(
        model=os.getenv("OPENAI_EMBED_MODEL","text-embedding-3-small"),
        input=["hello world"]
    )
    print(len(r.data[0].embedding))
asyncio.run(t())
