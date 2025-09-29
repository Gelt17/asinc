import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import re

def download_parser():
    def download_spimex_reports(links, base_url="https://spimex.com"):
        """
        Скачивает все отчеты с Spimex в папку files
        """
        os.makedirs('files', exist_ok=True)
        
        downloaded_files = []
        
        for i, relative_url in enumerate(links, 1):
            try:
                full_url = urljoin(base_url, relative_url)
                
                filename = relative_url.split('/')[-1].split('?')[0]
                filepath = os.path.join('files', filename)
                
                print(f"Скачивание {i}/{len(links)}: {filename}")
                

                response = requests.get(full_url, stream=True, timeout=30)
                response.raise_for_status()  
                
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                downloaded_files.append(filename)
                print(f"✅ Успешно: {filename}")
                
            except Exception as e:
                print(f"❌ Ошибка при скачивании {relative_url}: {e}")
        
        return downloaded_files


    pattern = r'^/upload/reports/oil_xls/oil_xls_2023\d+\.xls\?r=\d+$'
    n = 1
    links = []
    while n != 0:
        response = requests.get(f'https://spimex.com/markets/oil_products/trades/results/?page=page-{n}')
        soup = BeautifulSoup(response.text, 'html.parser')
        for i in range(1, 11):
            element = soup.select_one(f'#comp_d609bce6ada86eff0b6f7e49e6bae904 > div.accordeon-inner > div:nth-child({i}) > div > div.accordeon-inner__header > a')
            if element:
                link = element.get('href')
                if re.match(pattern, link):
                    n = -1
                    break
                print(link)
                links.append(link)
        n += 1

            

    print("Начинаем скачивание файлов...")
    downloaded = download_spimex_reports(links)
    print(f"\nСкачано файлов: {len(downloaded)} из {len(links)}")
    print("Файлы сохранены в папке 'files'")