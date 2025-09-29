import aiohttp
import aiofiles
import asyncio
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin

class AsyncSpimexParser:
    def __init__(self):
        self.base_url = "https://spimex.com"
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def get_links(self):
        pattern = r'^/upload/reports/oil_xls/oil_xls_2023\d+\.xls\?r=\d+$'
        links = []
        n = 1
        
        while True:
            async with self.session.get(
                f'https://spimex.com/markets/oil_products/trades/results/?page=page-{n}'
            ) as response:
                soup = BeautifulSoup(await response.text(), 'html.parser')
                found_new = False
                
                for i in range(1, 11):
                    selector = f'#comp_d609bce6ada86eff0b6f7e49e6bae904 > div.accordeon-inner > div:nth-child({i}) > div > div.accordeon-inner__header > a'
                    element = soup.select_one(selector)
                    
                    if element:
                        link = element.get('href')
                        if re.match(pattern, link):
                            return links
                        links.append(link)
                        found_new = True
                
                if not found_new:
                    break
                n += 1
        return links

    async def download_file(self, url, filename):
        filepath = os.path.join('files', filename)
        async with self.session.get(url) as response:
            async with aiofiles.open(filepath, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
        return filename

    async def download_all_files(self, links):
        os.makedirs('files', exist_ok=True)
        tasks = []
        for i, relative_url in enumerate(links, 1):
            full_url = urljoin(self.base_url, relative_url)
            filename = relative_url.split('/')[-1].split('?')[0]
            task = self.download_file(full_url, filename)
            tasks.append(task)
            print(f"Добавлена задача на скачивание {i}/{len(links)}: {filename}")
        
        downloaded = await asyncio.gather(*tasks, return_exceptions=True)
        successful = [f for f in downloaded if not isinstance(f, Exception)]
        print(f"Успешно скачано файлов: {len(successful)} из {len(links)}")
        return successful