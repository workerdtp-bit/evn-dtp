import csv
import pandas as pd
import datetime
import os
import time
import threading
import random

from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# =========================
# PROGRESS GLOBAL
# =========================
processed = 0
total = 0
progress_lock = threading.Lock()

# =========================
# 1. CREATE DRIVER (Tối ưu cho GitHub/Nhiều máy)
# =========================
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Giả lập User-Agent thật để tránh bị chặn
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    # Ẩn log rác
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # Tự động tải Driver phù hợp với phiên bản Chrome trên máy
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

# =========================
# 2. ĐỌC FILE CSV
# =========================
def read_makh(filename):
    if not os.path.exists(filename):
        print(f"Lỗi: Không tìm thấy file {filename}.")
        return []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            return [row[0].strip() for row in reader if row and row[0].strip()]
    except Exception as e:
        print(f"Lỗi khi đọc file: {e}")
        return []

# =========================
# 3. GHI CSV THREAD SAFE
# =========================
lock_file = threading.Lock()

def write_to_csv(filename, data, mode='a', header=False):
    fieldnames = ['Ma_KH', 'Thoi_gian_tra_cuu', 'Ket_qua']
    with lock_file:
        with open(filename, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if header:
                writer.writeheader()
            writer.writerows(data)

# =========================
# 4. CSV -> EXCEL
# =========================
def csv_to_excel(csv_filename, excel_filename):
    if not os.path.exists(csv_filename):
        return
    try:
        df = pd.read_csv(csv_filename, encoding='utf-8')
        df.to_excel(excel_filename, index=False)
        print(f"✅ Xuất Excel thành công: {excel_filename}")
    except Exception as e:
        print(f"Lỗi khi chuyển Excel: {e}")

# =========================
# 5. SCRAPE
# =========================
def scrape_power_outage(driver, ma_kh):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        driver.get('https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien')

        # Đợi ô nhập liệu xuất hiện
        input_element = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.ID, 'idMaKhachHang'))
        )

        # Xóa nội dung cũ trước khi nhập
        driver.execute_script("document.getElementById('idThongTinLichNgungGiamMaKhachHang').innerHTML = '';")
        
        input_element.clear()
        input_element.send_keys(ma_kh)
        input_element.send_keys(Keys.RETURN)

        # Đợi kết quả trả về
        WebDriverWait(driver, 20).until(
            lambda d: d.find_element(By.ID, 'idThongTinLichNgungGiamMaKhachHang').text.strip() != ""
        )

        div = driver.find_element(By.ID, 'idThongTinLichNgungGiamMaKhachHang')
        text_content = div.text.strip()
        result_text = "Lỗi hoặc không tìm thấy thông tin"

        tables = div.find_elements(By.TAG_NAME, 'table')
        if tables:
            rows = tables[0].find_elements(By.TAG_NAME, 'tr')
            all_results = []
            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, 'td')
                cols_text = [c.text.strip() for c in cols if c.text.strip()]
                if cols_text:
                    all_results.append(" | ".join(cols_text))
            result_text = "; ".join(all_results) if all_results else "Không có lịch cắt điện"
        else:
            result_text = text_content

        return {'Ma_KH': ma_kh, 'Thoi_gian_tra_cuu': current_time, 'Ket_qua': result_text}

    except Exception as e:
        return {'Ma_KH': ma_kh, 'Thoi_gian_tra_cuu': current_time, 'Ket_qua': f"Lỗi: {str(e)}"}

# =========================
# 6. RETRY
# =========================
def scrape_with_retry(driver, ma_kh, retries=3):
    for i in range(retries):
        result = scrape_power_outage(driver, ma_kh)
        if "Lỗi" not in result['Ket_qua']:
            return result
        time.sleep(2)
    return result

# =========================
# 7. WORKER
# =========================
def worker(ma_kh_list, thread_id, output_csv):
    global processed
    driver = create_driver()
    try:
        for ma_kh in ma_kh_list:
            result = scrape_with_retry(driver, ma_kh)
            with progress_lock:
                processed += 1
                print(f"[Thread {thread_id}] {processed}/{total} - {ma_kh}: DONE")
            
            write_to_csv(output_csv, [result])
            time.sleep(random.uniform(1.5, 3.0)) # Delay ngẫu nhiên để tránh bot detection
    finally:
        driver.quit()

# =========================
# 8. MAIN
# =========================
if __name__ == '__main__':
    input_file = "makh_list.csv"
    output_csv = "datasauget.csv"
    output_xlsx = "datasauget.xlsx"

    ma_kh_all = read_makh(input_file)
    if not ma_kh_all:
        print("Danh sách mã khách hàng trống!")
        exit()

    total = len(ma_kh_all)
    write_to_csv(output_csv, [], mode='w', header=True)

    num_threads = 4 # Để 4 luồng cho ổn định, tránh bị server EVN chặn IP
    chunks = [ma_kh_all[i::num_threads] for i in range(num_threads)]

    print(f"🚀 Bắt đầu tra cứu {total} mã với {num_threads} luồng...")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, chunks[i], i+1, output_csv) for i in range(num_threads)]
        for f in as_completed(futures):
            f.result()

    csv_to_excel(output_csv, output_xlsx)
    print("\n✅ HOÀN THÀNH TẤT CẢ.")
