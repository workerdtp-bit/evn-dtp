import csv
import pandas as pd
import datetime
import os
import time
import threading
import random
import re
import json
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# ==========================================
# CONFIG
# ==========================================
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

processed = 0
total = 0
lock = threading.Lock()
csv_lock = threading.Lock()

SPREADSHEET_ID = "1FVu_-BWCk_c7rjtC5ovq4wSish8U7bx3ay-KhNiYqXY"
TARGET_SHEET = "upload"

# ==========================================
# DRIVER
# ==========================================
def create_driver(driver_path):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver

# ==========================================
# SCRAPER (có retry)
# ==========================================
def scrape(driver, ma_kh):
    for _ in range(2):
        try:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            driver.get('https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien')

            input_el = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, 'idMaKhachHang'))
            )

            input_el.clear()
            input_el.send_keys(ma_kh)
            input_el.send_keys(Keys.RETURN)

            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.ID, 'idThongTinLichNgungGiamMaKhachHang')
                )
            )

            time.sleep(2)

            content = driver.find_element(
                By.ID,
                'idThongTinLichNgungGiamMaKhachHang'
            ).text.strip()

            return {
                "Ma_KH": ma_kh,
                "Thoi_gian": now,
                "Noi_dung": content
            }

        except Exception:
            time.sleep(2)

    return {
        "Ma_KH": ma_kh,
        "Thoi_gian": now,
        "Noi_dung": "Lỗi"
    }

# ==========================================
# WORKER (buffer ghi file)
# ==========================================
def worker(data, driver_path, output):
    global processed
    driver = create_driver(driver_path)
    buffer = []

    try:
        for ma_kh in data:
            res = scrape(driver, ma_kh)
            buffer.append(res)

            with lock:
                processed += 1
                print(f"📊 {processed}/{total} | {ma_kh}")

            if len(buffer) >= 5:
                write_csv(output, buffer)
                buffer = []

            time.sleep(random.uniform(1.5, 3))

        if buffer:
            write_csv(output, buffer)

    finally:
        driver.quit()

# ==========================================
# CSV
# ==========================================
def write_csv(file, rows, mode='a', header=False):
    with csv_lock:
        with open(file, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["Ma_KH", "Thoi_gian", "Noi_dung"]
            )
            if header:
                writer.writeheader()
            writer.writerows(rows)

# ==========================================
# PARSE DATA
# ==========================================
def process(input_csv):
    df = pd.read_csv(input_csv)

    rows = []

    for _, row in df.iterrows():
        text = str(row["Noi_dung"])

        kh = re.search(r"KHÁCH HÀNG:\s*(.+)", text)
        dc = re.search(r"ĐỊA CHỈ:\s*(.+)", text)

        kh = kh.group(1).strip() if kh else ""
        dc = dc.group(1).strip() if dc else ""

        blocks = re.split(r"(?=MÃ.*?LỊCH)", text, flags=re.IGNORECASE)

        for b in blocks:
            ma = re.search(r"MÃ.*LỊCH:\s*(\d+)", b)
            tg = re.search(
                r"từ (.+?) ngày (.+?) đến (.+?) ngày (.+)",
                b
            )
            lydo = re.search(r"LÝ DO.*:\s*(.+)", b)

            if ma and tg:
                rows.append([
                    row["Ma_KH"],
                    kh,
                    dc,
                    ma.group(1),
                    tg.group(2),
                    tg.group(1),
                    tg.group(4),
                    tg.group(3),
                    lydo.group(1) if lydo else ""
                ])

    df2 = pd.DataFrame(rows, columns=[
        "Ma_KH", "Khach_hang", "Dia_chi",
        "Ma_lich", "Ngay_BD", "Gio_BD",
        "Ngay_KT", "Gio_KT", "Ly_do"
    ])

    df2.to_excel("output.xlsx", index=False)
    upload_sheet(df2)

# ==========================================
# GOOGLE SHEET
# ==========================================
def upload_sheet(df):
    try:
        raw = os.getenv("GCP_JSON")
        raw = raw.replace("\\\\n", "\\n")

        info = json.loads(raw)
        info["private_key"] = info["private_key"].replace("\\n", "\n")

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = Credentials.from_service_account_info(info, scopes=scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_key(SPREADSHEET_ID)

        try:
            ws = sheet.worksheet(TARGET_SHEET)
        except WorksheetNotFound:
            ws = sheet.add_worksheet(title=TARGET_SHEET, rows="1000", cols="20")

        ws.clear()
        data = [df.columns.tolist()] + df.astype(str).values.tolist()
        ws.update(range_name="A1", values=data)

        print("✅ Upload Google Sheets OK")

    except Exception as e:
        print("❌ Sheet lỗi:", e)

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    file_input = "makh_list.csv"
    file_raw = "raw.csv"

    if not os.path.exists(file_input):
        print("❌ Thiếu file makh_list.csv")
        exit()

    with open(file_input, encoding="utf-8") as f:
        data = [r[0] for r in csv.reader(f) if r]

    global total
    total = len(data)

    driver_path = ChromeDriverManager().install()

    write_csv(file_raw, [], mode="w", header=True)

    threads = 3
    chunks = [data[i::threads] for i in range(threads)]

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker, c, driver_path, file_raw) for c in chunks]
        for f in as_completed(futures):
            f.result()

    process(file_raw)

    print("🏁 DONE")
