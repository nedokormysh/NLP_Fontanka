import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import logging
from datetime import datetime, date, timedelta
from tqdm.asyncio import tqdm
import random
import aioconsole
from typing import Optional, NoReturn, List, Dict
import csv
import psycopg2
from psycopg2.extras import execute_values
import psycopg2
from sqlalchemy import create_engine


# создаём логгер
logger = logging.getLogger(__name__)

# Настраиваем вывод логов в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# Настраиваем вывод логов в файл
file_handler = logging.FileHandler('my_log_file.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Устанавливаем уровень логирования для логгера
logger.setLevel(logging.DEBUG)

#
# def load_csv_to_postgres(file_path: str, table_name: str) -> None:
#     """
#     Выгружает данные из CSV-файла в таблицу PostgreSQL
#     :param file_path: путь к CSV-файлу
#     :param table_name: имя таблицы PostgreSQL
#     """
#     # подключаемся к базе данных
#     conn = psycopg2.connect(
#         dssn="postgres://bocuxjzf:SeOwalEg0IBjgCL6Ub7GGEMCj0PR5rI8@abul.db.elephantsql.com/bocuxjzf"
#     )
#
#     # создаем курсор для выполнения SQL-запросов
#     cur = conn.cursor()
#
#     # открываем CSV-файл и читаем данные
#     with open(file_path, 'r', encoding='utf-8') as f:
#         reader = csv.DictReader(f)
#
#         # формируем SQL-запрос для вставки данных в таблицу
#         columns = ', '.join(reader.fieldnames)
#         query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
#
#         # выполняем SQL-запрос для вставки данных в таблицу
#         execute_values(cur, query, reader)
#
#     # фиксируем изменения в базе данных
#     conn.commit()
#
#     # закрываем курсор и соединение с базой данных
#     cur.close()
#     conn.close()

def load_data_to_postgres(file_path: str) -> None:
    """
    Выгружает данные из CSV-файла в базу данных PostgreSQL
    :param file_path: путь к CSV-файлу
    """
    # создаем строку подключения к базе данных PostgreSQL
    conn_str = "postgres://bocuxjzf:SeOwalEg0IBjgCL6Ub7GGEMCj0PR5rI8@abul.db.elephantsql.com/bocuxjzf"

    # создаем движок SQLAlchemy для подключения к базе данных PostgreSQL
    engine = create_engine(conn_str)

    # загружаем данные из CSV-файла в pandas DataFrame
    df = pd.read_csv(file_path)

    # выгружаем данные из DataFrame в базу данных PostgreSQL
    df.to_sql("news", engine, if_exists="append", index=False)

def parse_date(date_string: str) -> Optional[datetime]:
    """
    Функция парсинга даты из строкового представления
    :param date_string: строка с датой
    :return: datetime представление даты
    """
    logger.debug('Парсинг даты')

    month_map = {
        'января': '01',
        'февраля': '02',
        'марта': '03',
        'апреля': '04',
        'мая': '05',
        'июня': '06',
        'июля': '07',
        'августа': '08',
        'сентября': '09',
        'октября': '10',
        'ноября': '11',
        'декабря': '12'
    }
    try:
        day, month_word, year, time = date_string.split(' ')
        year = year.replace(',', '')
        month = month_map[month_word]
        formatted_date = f"{day}.{month}.{year} {time}"
        return datetime.strptime(formatted_date, '%d.%m.%Y %H:%M')
    except Exception as e:
        print(f"Не удалось разобрать дату: {date_string}, ошибка: {e}")
        logger.error(f"Не удалось разобрать дату: {date_string}")
        return None


async def print_progress(progress: int) -> NoReturn:
    """
    Функция печати прогресса в консоль
    :param progress: текущий прогресс в процентах
    :return: None
    """
    try:
        await aioconsole.aprint(f"\rПрогресс: {progress}%", end="")
    except Exception as e:
        # logger.debug(f"Ошибка при печати прогресса: {e}")
        logger.error(f"Ошибка при печати прогресса: {e}")


async def collect_data_from_page(url: str, semaphore: asyncio.Semaphore) -> List[Dict]:
    """
    Функция сбора данных с новостной страницы
    :param url: URL страницы
    :param semaphore: семафор для ограничения количества одновременных запросов
    :return: список словарей с данными о новостях
    """
    logger.info(f"Старт сбора данных для {url}")
    async with aiohttp.ClientSession() as session:
        async with semaphore:
            try:
                async with session.get(url) as response:
                    html = await response.text()
                    tree = BeautifulSoup(html, 'html.parser')
                    articles = tree.find_all('li', {'class': 'IPafn'})
                    info = []
                    for article in articles:
                        topic = article.find_all('a', {'class': 'IPmv IPl7'})
                        subclass = article.find_all('a', {'class': 'IPef'})
                        if len(topic) == 0:
                            continue
                        topic = topic[0].get('title')
                        if 'Афиша Plus' in topic:
                            continue
                        title = subclass[0].text
                        suburl = subclass[0].get('href')
                        if 'longreads' in suburl or 'doctorpiter' in suburl or 'https' in suburl or 'vk' in suburl or 'amp' in suburl:
                            continue
                        suburl = 'https://www.fontanka.ru' + suburl
                        async with session.get(suburl) as response_inner:
                            html_inner = await response_inner.text()
                            tree_inner = BeautifulSoup(html_inner, 'html.parser')
                            check_add = tree_inner.find_all('div', {'class': 'NBzj MVzj'})
                            if len(check_add) == 0:
                                continue
                            content_ = tree_inner.find_all('div', {'class': 'CFah JFa3 JFah'})
                            content_txt = []
                            [content_txt.append(el.text) for el in content_]
                            content = ' '.join(content_txt)
                            views = int(tree_inner.find_all('span', {'class': 'CNm3 primaryOverlineMobile'})[0].text)
                            date_time = tree_inner.find_all('span', {'itemprop': 'datePublished'})[0].text
                            date_time = parse_date(date_time)
                            comments = int(tree_inner.find_all('div', {'class': 'NBzj MVzj'})[0].text)
                            row = {'url': url,
                                   'title': title,
                                   'content': content,
                                   'topic': topic,
                                   'datetime': date_time,
                                   'views': views,
                                   'comments_amount': comments}
                            info.append(row)
                    logger.info(f"Данные для {url} собраны")
                    await asyncio.sleep(random.uniform(1, 3))
                    return info
            except Exception as e:
                logger.info(f"Ошибка при сборе данных для {url}: {e}")
                logger.error(f"Ошибка при сборе данных для {url}: {e}")
                return []


async def main() -> NoReturn:
    """
    Функция выполняет сбор данных с новостных страниц и сохраняет их в CSV-файл
    """
    # пустой датафрейм, в который будем сохранять результаты
    df_main = pd.DataFrame()
    logger.info('Поехали')

    # задаём даты начала и конца обработки
    start = date(2022, 1, 1)
    finish = date(2022, 12, 31)

    # создаём список дат в диапазоне от start до finish
    dates = [start + timedelta(days=i) for i in range((finish - start).days)]

    # создаём список URL-адресов на основе списка дат
    urls = ['https://www.fontanka.ru/{}/news.html'.format(date.strftime('%Y/%m/%d')) for date in dates]

    semaphore = asyncio.Semaphore(10) # Ограничиваем количество одновременных запросов

    # создаём список тасков для асинхронного выполнения
    tasks = [asyncio.create_task(collect_data_from_page(url, semaphore)) for url in urls]

    # Создаём экземпляр tqdm с общей длиной задач
    progress_bar = tqdm(total=len(tasks))

    try:
        results = []
        total_news = 0
        for i, task in enumerate(asyncio.as_completed(tasks)):
            try:
                result = await task
                results.append(result)
                total_news += len(result)
            except aiohttp.client_exceptions.ServerDisconnectedError:
                print("Сервер отключился, пропускаем этот URL")
            progress = int(100 * (i + 1) / len(tasks))
            await print_progress(progress)  # выводим в консоль процент обработки

        print(f"\nВсего собрано новостей: {total_news}")

        df_main = pd.concat([pd.DataFrame(result) for result in results])

        # сохраняем результаты в файл
        df_main.to_csv('fontanka_full.csv', index=False)

        print('Общий результат сохранён')
    except Exception as e:
        print(f"Ошибка при выполнении программы: {e}")
        logger.error(f"Ошибка при выполнении программы: {e}")

    print('Общий результат сохранён')

    # # выгружаем данные в PostgreSQL
    # load_data_to_postgres('fontanka_full.csv')


if __name__ == '__main__':
    asyncio.run(main())