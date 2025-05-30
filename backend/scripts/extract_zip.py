import aiofiles
import zipfile
import os
import asyncio

async def extract_zip(zip_path, extract_to):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, zipfile.ZipFile(zip_path, 'r').extractall, extract_to)