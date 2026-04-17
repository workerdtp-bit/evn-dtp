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

# THÔNG TIN GOOGLE SHEET (Bạn có thể sửa ID này nếu đổi Sheet)
SPREADSHEET_ID = "1FVu_-BWCk_c7rjtC5ovq4wSish8U7bx3ay-KhNiYqXY"
TARGET_SHEET = "upload"

# THÔNG TIN XÁC THỰC GOOGLE
# Sử dụng biến RAW_PRIVATE_KEY với định dạng chuỗi thô (r"") để tránh lỗi ký tự đặc biệt
RAW_PRIVATE_KEY = r"""-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDlIAsWlrWSoCUi
CPdqWo2x0IBGXv1S7rFCZgTOD5wb17OfC+vA6R7cvLJ2C7EdCf96oOa7ssEJudxV
ZvpZfv4nAph2H5lfyg/8W8KdLncA0OO6UAzJqMBJfS10RM5d3NrMqO7JcEo8LOkn
hFc7zg1CVXkSSJP8T/qEtqaNAVUjYiqDgPp1yRcZAu9YjC5IcvLO/Yuv8PAnVxT5
Aj9V8cl6ER2VN6njH0agJHybFu6mobUNkoY/JYBev5HPxHA8IDzTzj+lcFE3kKmW
3+tX9Vm1dBvVBVViVulX4TcB8WUOdor18pEvOZE17W6FxVblWyymh3x3TbpMIJGZ
Bg5/2ltjAgMBAAECggEAJIE18wvg8vL754/JJ5M01xsyjMOulat852jMpC39f7we
nnJzwG0SpC58uybr0JX3Fy/pduJLyyYNotNyaz255vKpfsxY2v/m4EUtnVtaj1Cfz
2GfPmcN+m1bLyqX5wR+iZpAVt1diJHM9VHPz2A3ss+BdUUnp9f2iGsnlADsxntjE
Xw4C6DcOFh211DVT/UCfwtC8U+RS1af+bkyrm+3uZHf4F3x3xp7oyHwOJ0g6qNEF
L493sx8wnHA5qUtNt+ZP5Tp0JfbIa4HJ2PNNIxFAUsfxOW/EXlIq6EOO8oRCZJqg
eQfJMB4sme15Dt9ihwzT5yOIQaHc5piAslRg6gmXHQKBgQD3iuv00Y/VUtNspaLk
rHJ/whD3kDFxkHmkYE8v3SnmuiARfEMPpC+EbAKj2xs/N0rmf22fA9aZ4oxYLIfw
kKc34raog+GJiDrI0p71sUKGObgC0BmL/OTxMX+7FEygbUF0Z08+8OAINrEAunew
fdfZyWJL28IVqmlZ/TdtmRaRbQKBgQDs9Al4FXidelVRZWUrfNMtbBCHs+dkKO64
+jQJiT5MMKCtYAGBLKqb9XglI2q9LQbnc2qQQON+2KjoLXeV8GQMsDXMi2n4QmL5
U8zkL4V+7wvqG5y8EZ5p/7SIONV5czMf9K+ZZVYGFUNkaGBb/sPqlEr/nhSC8hMH
PVbUbVduDwKBgG3pPglu/wk+BGgqR2B3fUNivLvfR0TgXFJy/NYIwjETFWgOH4yT
XxHSD34HrFpuR5B+pgLD5oZfQ+dmpllXMRgsTuQV4o95cHh4pGH8+ce1WxHMqnsw
p8q8KrW7NqbIvBZeRJ5yv1aGSiNDqB8yUSP/Oejqw4txJePx/alpHs8lAoGAZVvW
mRmGKMblrBXVew7APtPVFldsibnAtDvC+rlMfsbmVIOW4Sy8Jk6QgEJwLFAQff0u
/lnjdqUzS233k5nrEkpmGQMh52Jud/zSzmFNl4il7hS1rPVUcD9DeGnnVzZiDi9a
1iE09REvbMoBPhjysWuR0VpLp9/pJ9WjSOck2R0CgYBOtqCzh02Md1KA0aq4an1q
hBBApWtn+5NllJSoEc9yYF+WvQSJP6RE24ng6K1Cw1jCHrr3CMU8DHtTUEJdj1ir
A2x5oiaTZgWqkeqL96KCIZZc7qSoBwC1h4fwMyt0RSgteYqN0vTmaYrMPmORO4SL
zpEvtlkL1VrxHTOnsfKHhA==
-----END PRIVATE KEY-----"""

GOOGLE_INFO = {
  "type": "service_account",
  "project_id": "responsive-task-492802-h3",
  "private_key_id": "0f08af7961386cb5c1df97577f7c0dca3038d83f",
  # Fix lỗi định dạng PEM bằng cách đảm bảo xuống dòng \n chuẩn
  "private_key": RAW_PRIVATE_KEY.strip().replace("\\n", "\n"),
  "client_email": "evn-danhht@responsive-task-492802-h3.iam.gserviceaccount.com",
  "client_id": "117157456897680448434",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/evn-danhht%40responsive-task-492802-h3.iam.gserviceaccount.com"
}

# ==========================================
# 1. KHỞI TẠO TRÌNH DUYỆT (SELENIUM)
# ==========================================
def create_driver(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

# ==========================================
# 2. CÁC HÀM TIỆN ÍCH
# ==========================================
def write_to_csv(filename, data, mode='a', header=False):
    fieldnames = ['Ma_KH', 'Thoi_gian_tra_cuu', 'Ket_qua']
    with csv_lock:
        with open(filename, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if header:
                writer.writeheader()
            writer.writerows(data)

def split_list(data, n):
    if n <= 0: return [data]
    k, m = divmod(len(data), n)
    return [data[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

# ==========================================
# 3. LOGIC CÀO DỮ LIỆU (SCRAPER)
# ==========================================
def scrape_power_outage(driver, ma_kh):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        driver.get('https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien')
        input_element = WebDriverWait(driver, 25).until(
            EC.visibility_of_element_located((By.ID, 'idMaKhachHang'))
        )
        driver.execute_script("document.getElementById('idThongTinLichNgungGiamMaKhachHang').innerHTML = '';")
        
        input_element.clear()
        input_element.send_keys(ma_kh)
        input_element.send_keys(Keys.RETURN)

        WebDriverWait(driver, 25).until(
            lambda d: d.find_element(By.ID, 'idThongTinLichNgungGiamMaKhachHang').text.strip() != ""
        )
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

# ==========================================
# 4. XỬ LÝ DỮ LIỆU & ĐẨY GOOGLE SHEETS
# ==========================================
def upload_to_sheets(dataframe):
    print("⏳ Đang đẩy dữ liệu lên Google Sheets...")
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        # Xác thực trực tiếp từ Dictionary GOOGLE_INFO
        creds = Credentials.from_service_account_info(GOOGLE_INFO, scopes=scope)
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
        print(f"❌ Lỗi Google Sheets: {e}")

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
# 5. CHƯƠNG TRÌNH CHÍNH
# ==========================================
if __name__ == '__main__':
    csv_raw = "datasauget.csv"
    makh_file = "makh_list.csv"

    if not os.path.exists(makh_file):
        print(f"❌ Không thấy file {makh_file}"); exit()
        
    with open(makh_file, 'r', encoding='utf-8') as f:
        ma_kh_all = [r[0].strip() for r in csv.reader(f) if r and r[0].strip()]

    if not ma_kh_all:
        print("❌ Danh sách mã khách hàng trống!"); exit()

    total = len(ma_kh_all)
    print(f"🚀 Bắt đầu quét {total} mã khách hàng...")
    
    d_path = ChromeDriverManager().install()

    write_to_csv(csv_raw, [], mode='w', header=True)
    
    num_threads = 3
    chunks = split_list(ma_kh_all, num_threads)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, chunks[i], i+1, csv_raw, d_path) for i in range(num_threads)]
        for f in as_completed(futures):
            f.result()

    print("🏁 Quét xong. Đang xử lý dữ liệu và upload...")
    process_and_finalize(csv_raw)
    print("✨ HOÀN TẤT.")
