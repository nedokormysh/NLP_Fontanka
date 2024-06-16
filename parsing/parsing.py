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

# создаём логгер
logger = logging.getLogger(__name__)

# Настраиваем вывод логов в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# Настраиваем вывод логов в файл
file_handler = logging.FileHandler('my_log_file.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Устанавливаем уровень логирования для логгера
logger.setLevel(logging.DEBUG)


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
    async with (aiohttp.ClientSession() as session):
        async with semaphore:
            try:
                async with session.get(url) as response:
                    html = await response.text()
                    tree = BeautifulSoup(html, 'html.parser')
                    articles = tree.find_all('li', {'class': 'IFae9'})
                    info = []
                    for article in articles:
                        topic = article.find_all('a', {'class': 'IFhf IFkp'})
                        subclass = article.find_all('a', {'class': 'IFcb'})

                        if len(topic) == 0:
                            continue
                        topic = topic[0].get('title')

                        # не берём некоторые темы, т.к. они рекламные
                        topics_to_skip = ['Афиша Plus',
                                          'Новости компаний',
                                          'Работа',
                                          'ЖКХ',
                                          'Финляндия']
                        if any(topic in topic_to_skip for topic_to_skip in topics_to_skip):
                            continue

                        title = subclass[0].text
                        suburl = subclass[0].get('href')

                        substrings_to_check = ['longreads', 'doctorpiter', 'https', 'vk', 'amp']
                        # не будем брать лонгриды и статьи с переходом на другие сайты
                        if any(substring in suburl for substring in substrings_to_check):
                            continue

                        suburl = 'https://www.fontanka.ru' + suburl

                        async with session.get(suburl) as response_inner:
                            html_inner = await response_inner.text()
                            tree_inner = BeautifulSoup(html_inner, 'html.parser')
                            check_add = tree_inner.find_all('span',
                                                            {'class': 'JBahp primarySubtitle1AccentMobile'})
                            if len(check_add) == 0:
                                continue
                            content_ = tree_inner.find_all('div', {'class': 'B1ah I-a3 I-ah'})
                            content_txt = []
                            [content_txt.append(el.text) for el in content_]
                            content = ' '.join(content_txt)
                            views = int(tree_inner.find_all('span',
                                                            {'class': 'A7hp primaryOverlineMobile'})[0].text)
                            date_time = tree_inner.find_all('span', {'itemprop': 'datePublished'})[0].text
                            date_time = parse_date(date_time)
                            comments = int(
                                tree_inner.find_all('span',
                                                    {'class': 'JBahp primarySubtitle1AccentMobile'})[0].text)
                            row = {'url': suburl,
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
                # logger.info(f"Ошибка при сборе данных для {url}: {e}")
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
    finish = date(2024, 1, 1)

    # создаём список дат в диапазоне от start до finish
    dates = [start + timedelta(days=i) for i in range((finish - start).days)]

    # создаём список URL-адресов на основе списка дат
    urls = ['https://www.fontanka.ru/{}/news.html'.format(date.strftime('%Y/%m/%d')) for date in dates]

    semaphore = asyncio.Semaphore(10)  # Ограничиваем количество одновременных запросов

    # создаём список тасков для асинхронного выполнения
    tasks = [asyncio.create_task(collect_data_from_page(url, semaphore)) for url in urls]

    # Создаём экземпляр tqdm с общей длиной задач
    progress_bar = tqdm(total=len(tasks))

    try:

        results = []
        total_news = 0
        intermediate_df = pd.DataFrame()
        intermediate_count = 0  # подсчёт промежуточных значений
        intermediate_file_index = 0  # индексирование промежуточных значений
        for i, task in enumerate(asyncio.as_completed(tasks)):
            try:
                result = await task
                results.append(result)
                total_news += len(result)
                intermediate_df = pd.concat([intermediate_df, pd.DataFrame(result)])
                intermediate_count += len(result)
                if intermediate_count >= 1000:
                    # сохраняем промежуточный датафрейм в файл
                    intermediate_df.to_csv(f'fontanka_intermediate_{intermediate_file_index}.csv',
                                           index=False)
                    intermediate_df = pd.DataFrame()
                    intermediate_count = 0
                    intermediate_file_index += 1
            except aiohttp.client_exceptions.ServerDisconnectedError:
                print("Сервер отключился, пропускаем этот URL")
            progress = int(100 * (i + 1) / len(tasks))
            await print_progress(progress)  # выводим в консоль процент обработки

        print(f"\nВсего собрано новостей: {total_news}")

        # Сохраняем последний промежуточный результат, если там что-то есть.
        if not intermediate_df.empty:
            intermediate_df.to_csv(f'fontanka_intermediate_{intermediate_file_index}.csv', index=False)

        try:
            df_main = pd.concat([pd.DataFrame(result) for result in results])

            # сохраняем результаты в файл
            df_main.to_csv('fontanka_full.csv', index=False)
            logger.info('Общий результат сохранён')
            print('Общий результат сохранён')
        except Exception as e:
            # logger.info(f"Ошибка при сборе данных для {url}: {e}")
            logger.error(f"Не удалось сохранить общий файл csv: {e}")

        # создаём список дат, для которых были собраны данные
        collected_dates = df_main['datetime'].dt.date.unique().tolist()

        # создаём множество дат, для которых были собраны данные
        collected_dates_set = set(collected_dates)

        # создаём множество всех дат в диапазоне от start до finish
        # all_dates_set = set(date for date in dates)
        all_dates_set = set(dates)

        # находим разницу между множествами, чтобы получить список пропущенных дат
        missing_dates = all_dates_set - collected_dates_set

        # проверяем, были ли потеряны данные
        if missing_dates:
            # выводим сообщение о потерянных данных
            print(f"Потеряны данные за следующие дни: {', '.join(str(date) for date in missing_dates)}\n"
                  f"Всего потеряно {len(missing_dates)}")
        else:
            # выводим сообщение о том, что данные были собраны полностью
            logger.info("Все данные были собраны полностью, без потерь")
            print("Все данные были собраны полностью, без потерь")
    except Exception as e:
        print(f"Ошибка при выполнении прог2раммы: {e}")
        logger.error(f"Ошибка при выполнении программы: {e}")


if __name__ == '__main__':
    asyncio.run(main())
