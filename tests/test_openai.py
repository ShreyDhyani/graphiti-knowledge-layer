import os, openai, asyncio
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()


api_key=os.getenv("OPENAI_API_KEY")

print(f"KEY IS {api_key}")
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
async def go():
    r = await client.responses.create(model="gpt-4o-mini", input="ping")
    print(r.output_text[:50])
asyncio.run(go())

async def t():
    r = await client.embeddings.create(
        model=os.getenv("OPENAI_EMBED_MODEL","text-embedding-3-small"),
        input=["hello world"]
    )
    print(len(r.data[0].embedding))
asyncio.run(t())