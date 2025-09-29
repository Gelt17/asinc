import asyncio
import time
from parser import AsyncSpimexParser
from database import AsyncDatabase
from addsting import add_string, extract_date_from_filename
from models import SpimexTradingResults
import os

async def main():
    start_time = time.time()
    
    try:
        # Скачиваем файлы
        async with AsyncSpimexParser() as parser:
            links = await parser.get_links()
            print(f"Найдено ссылок: {len(links)}")
            files = await parser.download_all_files(links)
        
        # Инициализируем БД
        db = AsyncDatabase()
        await db.init()
        
        # Парсим и загружаем в БД
        total_records = 0
        
        async with db.get_session() as session:
            for filename in files:
                try:
                    file_data = add_string(f"files/{filename}")
                    
                    for i in range(len(file_data["Код Инструмента"])):
                        record = SpimexTradingResults(
                            exchange_product_id = file_data["Код Инструмента"][i],
                exchange_product_name = file_data["Наименование Инструмента"][i],
                oil_id = file_data["Код Инструмента"][i][:4],
                delivery_basis_id = file_data["Код Инструмента"][i][4:7],
                delivery_basis_name = file_data["Базис поставки"][i],
                delivery_type_id = file_data["Код Инструмента"][i][-1],
                volume = file_data["Объем Договоров в единицах измерения"][i],
                total = file_data["Обьем Договоров, руб."][i],
                count = file_data["Количество Договоров, шт."][i],
                date = extract_date_from_filename(filename)
                        )
                        session.add(record)
                        total_records += 1
                    
                    await session.commit()
                    print(f"✅ Обработан файл: {filename}")
                    
                except Exception as e:
                    print(f"❌ Ошибка при обработке файла {filename}: {e}")
                    await session.rollback()
        
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения: {execution_time:.2f} секунд")
        print(f"Добавлено записей: {total_records}")
        
        return execution_time, total_records
    
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return 0, 0

if __name__ == "__main__":
    execution_time, total_records = asyncio.run(main())