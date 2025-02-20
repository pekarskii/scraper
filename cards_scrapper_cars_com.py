import pymysql
import requests
import time
import json
from bs4 import BeautifulSoup

start_time = time.time()

headers = requests.utils.default_headers()
headers.update({
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "en-US,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "TGTG/22.2.1 Dalvik/2.1.0 (Linux; U; Android 9; SM-G955F Build/PPR1.180610.011)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive"
})

DEFAULT_HEADER = headers
SOURCE_ID = "https://www.cars.com"
PROCESS_DESC = "cards_scrapper_cars_com.py"
MIN_RESCRAP_TIME = 24

def get_parsed_card(url, headers=DEFAULT_HEADER):
    card_dict = {}
    page = requests.get(url, headers=headers)
    if page.status_code != 200:
        return {}

    soup = BeautifulSoup(page.text, "html.parser")
    card = soup.find("section", class_="listing-overview")
    if not card:
        return {}

    card_dict["gallery"] = [img["src"] for img in card.find_all("img", class_="swipe-main-image")]
    
    basic_content = soup.find("div", class_="basics-content-wrapper")
    basic_section = basic_content.find("section", class_="sds-page-section basics-section")
    fancy_description_list = basic_section.find("dl", class_="fancy-description-list")
    
    dt_elements = [dt.text.strip() for dt in fancy_description_list.find_all("dt")]
    dd_elements = [dd.get_text("|", strip=True).split("|")[0] for dd in fancy_description_list.find_all("dd")]
    card_dict.update(dict(zip(map(str.lower, dt_elements), dd_elements)))

    card_dict["card_id"] = card_dict.get("stock #", card_dict.get("vin", ""))
    if not card_dict["card_id"] or card_dict["card_id"] == "-":
        return {}
    
    card_dict["title"] = card.find(class_="listing-title").text
    card_dict["price_primary"] = card.find("div", class_="price-section").find("span", class_="primary-price").text
    
    price_history = " | ".join(
        f"{row.find_all('td')[0].text}: {row.find_all('td')[2].text}"
        for row in soup.find("div", class_="price-history").find_all("tr")
    ) if soup.find("div", class_="price-history") else ""
    card_dict["price_history"] = price_history
    
    card_dict["scrap_date"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    return card_dict

def init_db_connection(con, sql_script_path):
    if sql_script_path:
        with open(sql_script_path) as f, con.cursor() as cur:
            for stmt in f.read().split(";"):
                try:
                    cur.execute(stmt)
                except:
                    return -1
    return 0

def main():
    with open("config.json") as f:
        configs = json.load(f)
    
    con = pymysql.connect(**configs["audit_db"])
    init_db_connection(con, configs.get("init_db_script"))
    
    with con.cursor() as cur:
        cur.execute(
            f"""
                INSERT INTO process_log (process_desc, user, host, connection_id)
                SELECT '{PROCESS_DESC}', user, host, connection_id()
                FROM information_schema.processlist
                WHERE id = connection_id();
            """
        )
        
        cur.execute("SELECT LAST_INSERT_ID() AS process_log_id;")
        process_log_id = cur.fetchone()[0]
        
        while True:
            cur.execute(
                f"""
                    SELECT FLOOR(RAND() * (SELECT MAX(ad_group_id) FROM ads WHERE ad_status IN (0,2)))
                    AS random_ad_group_id;
                """
            )
            random_ad_group_id = cur.fetchone()[0]
            if not random_ad_group_id:
                break
            
            cur.execute(
                f"""
                    SELECT ads_id, CONCAT(source_id, card_url) AS url
                    FROM ads
                    WHERE ad_status IN (0,2) AND ad_group_id >= {random_ad_group_id}
                    LIMIT 1;
                """
            )
            records = cur.fetchall()
            if not records:
                break
            
            for ads_id, url in records:
                ad_status = None
                try:
                    parsed_card = get_parsed_card(url)
                    card_json = json.dumps(parsed_card)
                    ad_status = 2 if parsed_card else 1
                except:
                    ad_status = -1
                    card_json = '{}'
                
                cur.execute(
                    f"""
                        UPDATE ads
                        SET ad_status = {ad_status}, change_status_date = CURRENT_TIMESTAMP,
                            change_status_process_log_id = {process_log_id}, card = '{card_json}'
                        WHERE ads_id = {ads_id};
                    """
                )
        
        cur.execute(
            f"""
                UPDATE process_log
                SET end_date = CURRENT_TIMESTAMP
                WHERE process_log_id = {process_log_id};
            """
        )

if __name__ == "__main__":
    main()
