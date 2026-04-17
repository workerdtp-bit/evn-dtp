import csv
import pandas as pd
import datetime
import os
import time
import threading
import random
import re
import io
import sys
import json
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
# CẤU HÌNH & ENCODING
# ==========================================
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

processed = 0
total = 0
progress_lock = threading.Lock()
csv_lock = threading.Lock()

# Cấu hình file và ID
SPREADSHEET_ID = "1FVu_-BWCk_c7rjtC5ovq4wSish8U7bx3ay-KhNiYqXY"
TARGET_SHEET = "upload"
# Tên file JSON bạn đã upload lên GitHub
JSON_FILE = "responsive-task-492802-h3-0f08af796138.json"

# ==========================================
# 1. SELENIUM SETUP
# ==========================================
def create_driver(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

# ==========================================
# 2. LOGIC CÀO DỮ LIỆU
# ==========================================
def scrape_power_outage(driver, ma_kh):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        driver.get('https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien')
        input_element = WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.ID, 'idMaKhachHang')))
        driver.execute_script("document.getElementById('idThongTinLichNgungGiamMaKhachHang').innerHTML = '';")
        input_element.clear()
        input_element.send_keys(ma_kh)
        input_element.send_keys(Keys.RETURN)
        WebDriverWait(driver, 25).until(lambda d: d.find_element(By.ID, 'idThongTinLichNgungGiamMaKhachHang').text.strip() != "")
        div_content = driver.find_element(By.ID, 'idThongTinLichNgungGiamMaKhachHang').text.strip()
        return {'Ma_KH': ma_kh, 'Thoi_gian_tra_cuu': current_time, 'Ket_qua': div_content}
    except Exception as e:
        return {'Ma_KH': ma_kh, 'Thoi_gian_tra_cuu': current_time, 'Ket_qua': f"Lỗi: {str(e)}"}

def worker(ma_kh_list, thread_id, output_csv, driver_path):
    global processed
    driver = create_driver(driver_path)
    try:
        for ma_kh in ma_kh_list:
            result = scrape_power_outage(driver, ma_kh)
            with progress_lock:
                processed += 1
                print(f"📊 [{processed}/{total}] Luồng {thread_id}: {ma_kh} - DONE")
            write_to_csv(output_csv, [result])
            time.sleep(random.uniform(2, 4))
    finally:
        driver.quit()

def write_to_csv(filename, data, mode='a', header=False):
    fieldnames = ['Ma_KH', 'Thoi_gian_tra_cuu', 'Ket_qua']
    with csv_lock:
        with open(filename, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if header: writer.writeheader()
            writer.writerows(data)

# ==========================================
# 3. UPLOAD GOOGLE SHEETS (FIXED)
# ==========================================
def upload_to_sheets(dataframe):
    print(f"⏳ Đang kết nối Google Sheets bằng file {JSON_FILE}...")
    try:
        if not os.path.exists(JSON_FILE):
            print(f"❌ Lỗi: Không tìm thấy file {JSON_FILE} trong thư mục!")
            return

        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        # Nạp trực tiếp từ file JSON để tránh lỗi JWT Signature do copy-paste chuỗi
        creds = Credentials.from_service_account_file(JSON_FILE, scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        try:
            worksheet = spreadsheet.worksheet(TARGET_SHEET)
        except WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=TARGET_SHEET, rows="1000", cols="20")
        
        worksheet.clear()
        data_to_upload = [dataframe.columns.tolist()] + dataframe.astype(str).values.tolist()
        worksheet.update(range_name="A1", values=data_to_upload, value_input_option="USER_ENTERED")
        print("✅ Đã cập nhật Google Sheets thành công!")
    except Exception as e:
        print(f"❌ Lỗi Google Sheets chi tiết: {e}")

# ==========================================
# 4. XỬ LÝ DỮ LIỆU REGEX
# ==========================================
def process_and_finalize(input_csv):
    if not os.path.exists(input_csv): return
    df = pd.read_csv(input_csv)
    rows = []
    for _, row in df.iterrows():
        ma_kh = row.get('Ma_KH', '')
        time_query = row.get('Thoi_gian_tra_cuu', '')
        text = str(row.get('Ket_qua', ''))
        
        kh_match = re.search(r"KHÁCH HÀNG:\s*(.+)", text, re.IGNORECASE)
        dc_match = re.search(r"ĐỊA CHỈ:\s*(.+)", text, re.IGNORECASE)
        khach_hang = kh_match.group(1).strip() if kh_match else ""
        dia_chi = dc_match.group(1).strip() if dc_match else ""

        lich_blocks = re.split(r"(?=MÃ.*LỊCH)", text)
        for block in lich_blocks:
            ma_lich_match = re.search(r"MÃ.*LỊCH:\s*(\d+)", block, re.IGNORECASE)
            tg_match = re.search(r"THỜI GIAN:\s*từ (.+?) ngày (.+?) đến (.+?) ngày (.+)", block, re.IGNORECASE)
            ly_do_match = re.search(r"LÝ DO NGỪNG CUNG CẤP ĐIỆN:\s*(.+)", block, re.IGNORECASE)
            if ma_lich_match and tg_match:
                rows.append([
                    ma_kh, time_query, khach_hang, dia_chi, 
                    ma_lich_match.group(1), tg_match.group(2).strip(), tg_match.group(1).strip(),
                    tg_match.group(4).strip(), tg_match.group(3).strip(),
                    ly_do_match.group(1).strip() if ly_do_match else ""
                ])
    
    result_df = pd.DataFrame(rows, columns=[
        "MA_KH", "Thoi_gian_quet", "Khach_hang", "Dia_chi", "Ma_lich",
        "Ngay_BD", "Gio_BD", "Ngay_KT", "Gio_KT", "Ly_do"
    ])
    result_df.to_excel("output.xlsx", index=False)
    upload_to_sheets(result_df)

# ==========================================
# 5. MAIN
# ==========================================
if __name__ == '__main__':
    csv_raw = "datasauget.csv"
    makh_file = "makh_list.csv"

    if not os.path.exists(makh_file):
        print("❌ Không tìm thấy makh_list.csv"); exit()
        
    with open(makh_file, 'r', encoding='utf-8') as f:
        ma_kh_all = [r[0].strip() for r in csv.reader(f) if r and r[0].strip()]

    total = len(ma_kh_all)
    d_path = ChromeDriverManager().install()
    write_to_csv(csv_raw, [], mode='w', header=True)
    
    num_threads = 3
    chunks = [ma_kh_all[i::num_threads] for i in range(num_threads)]
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, chunks[i], i+1, csv_raw, d_path) for i in range(num_threads)]
        for f in as_completed(futures): f.result()

    process_and_finalize(csv_raw)
