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