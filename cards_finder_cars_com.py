import pymysql
from bs4 import BeautifulSoup
import requests
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()

headers = requests.utils.default_headers()
headers.update({
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'TGTG/22.2.1 Dalvik/2.1.0 (Linux; U; Android 9; SM-G955F Build/PPR1.180610.011)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive'
})

DEFAULT_HEADER = headers
SOURCE_ID = "https://www.cars.com"
PROCESS_DESC = "cards_finder_cars_com.py"

def get_card_url_list(url, site_url=SOURCE_ID, headers=DEFAULT_HEADER):
    url_list = []
    try:
        page = requests.get(url, headers=headers, timeout=10)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, "html.parser")
        listing_items = soup.find_all("div", class_="vehicle-card")
        for item in listing_items:
            link = item.find("a", class_="image-gallery-link")
            if link and link.get("href"):
                url_list.append(site_url + link["href"])
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе {url}: {e}")
    except Exception as e:
        logging.error(f"Ошибка парсинга страницы {url}: {e}")
    return url_list

def init_db_connection(con, sql_script_path):
    try:
        if sql_script_path:
            cur = con.cursor()
            with open(sql_script_path) as init_db_file:
                for sql_stmt in init_db_file.read().split(";"):
                    if sql_stmt.strip():
                        cur.execute(sql_stmt)
            con.commit()
        return 0
    except Exception as e:
        logging.error(f"Ошибка инициализации БД: {e}")
        return -1

def main():
    try:
        with open("config.json") as config_file:
            configs = json.load(config_file)
        con = pymysql.connect(**configs["audit_db"], autocommit=True)
    except Exception as e:
        logging.error(f"Ошибка подключения к БД: {e}")
        return
    
    if init_db_connection(con, configs.get("finder_init_db_script")) == -1:
        return
    
    try:
        with con.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO process_log(process_desc, user, host, connection_id)         
                    SELECT '{PROCESS_DESC}', user, host, connection_id()
                    FROM information_schema.processlist
                    WHERE id = connection_id();
                """
            )
            cur.execute("SELECT LAST_INSERT_ID();")
            process_log_id = cur.fetchone()[0]
            
            curr_year = int(time.strftime("%Y", time.gmtime()))
            page_size = 20
            
            num_ads_inserted = 0
            num_searches = 0
            num_combinations = (curr_year - 1900) * 50 * 500
            
            for year in range(curr_year, 1900, -1):
                for price_usd in range(0, 500001, 10000):
                    for page_num in range(1, 501):
                        num_searches += 1
                        group_url = f"{SOURCE_ID}/shopping/results/?list_price_max={price_usd + 9999}&list_price_min={price_usd}&maximum_distance=all&page_size={page_size}&page={page_num}&stock_type=used&year_max={year}&year_min={year}&zip=60606"
                        card_url_list = get_card_url_list(group_url)
                        if not card_url_list:
                            logging.info(f"{time.strftime('%X', time.gmtime(time.time() - start_time))}, no cards found")
                            num_searches += 500 - page_num
                            break
                        
                        cur.execute(
                            f"""
                                INSERT INTO ad_groups(price_min, page_size, year, page_num, process_log_id)
                                VALUES({price_usd}, {page_size}, {year}, {page_num}, {process_log_id});
                            """
                        )
                        cur.execute("SELECT LAST_INSERT_ID();")
                        ad_group_id = cur.fetchone()[0]
                        
                        for card_url in card_url_list:
                            try:
                                cur.execute(
                                    f"""
                                        INSERT INTO ads(source_id, card_url)
                                        SELECT '{SOURCE_ID}', '{card_url[len(SOURCE_ID):]}'
                                        WHERE NOT EXISTS (
                                            SELECT 1 FROM ads WHERE source_id='{SOURCE_ID}' AND card_url='{card_url[len(SOURCE_ID):]}'
                                        );
                                    """
                                )
                                if cur.rowcount > 0:
                                    num_ads_inserted += cur.rowcount
                                    cur.execute(
                                        f"""
                                            INSERT INTO ads_archive(ads_id, source_id, card_url, ad_group_id, process_log_id)
                                            VALUES (LAST_INSERT_ID(), '{SOURCE_ID}', '{card_url[len(SOURCE_ID):]}', {ad_group_id}, {process_log_id});
                                        """
                                    )
                            except pymysql.MySQLError as e:
                                logging.error(f"Ошибка при вставке данных в БД: {e}")
                        
                        logging.info(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, ads inserted: {num_ads_inserted}, combination #: {num_searches}, progress: {round(num_searches/num_combinations*100, 2):5}%, search url: {group_url}")
                        
                        if len(card_url_list) < page_size:
                            num_searches += 500 - page_num
                            break
        
            logging.info(f"end time (GMT): {time.strftime('%X', time.gmtime())}")
            cur.execute(
                f"""
                    UPDATE process_log 
                    SET end_date = CURRENT_TIMESTAMP 
                    WHERE process_log_id = {process_log_id};
                """
            )
    except Exception as e:
        logging.error(f"Ошибка в основном процессе: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
