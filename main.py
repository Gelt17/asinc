import parser
from database import create_database, get_session
import os
from addsting import add_string, extract_date_from_filename
from models import SpimexTradingResults
import time



def add_db(files):
    t = 0
    session = get_session()
    for i in files:
        add_s = add_string(f"files/{i}")
        for j in range(len(add_s["Код Инструмента"])):
            s = SpimexTradingResults(
                exchange_product_id = add_s["Код Инструмента"][j],
                exchange_product_name = add_s["Наименование Инструмента"][j],
                oil_id = add_s["Код Инструмента"][j][:4],
                delivery_basis_id = add_s["Код Инструмента"][j][4:7],
                delivery_basis_name = add_s["Базис поставки"][j],
                delivery_type_id = add_s["Код Инструмента"][j][-1],
                volume = add_s["Объем Договоров в единицах измерения"][j],
                total = add_s["Обьем Договоров, руб."][j],
                count = add_s["Количество Договоров, шт."][j],
                date = extract_date_from_filename(i)
            )
            
            session.add(s)
            t += 1
        session.commit()
    print(f"Сделано {t} записей") 
        
if __name__ == "__main__":
    start_time = time.time()
    parser.download_parser()
    engine = create_database()
    files = os.listdir('files')
    add_db(files)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения программы: {execution_time:.2f} секунд")