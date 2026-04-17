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
# 1. CREATE DRIVER
# =========================
def create_driver(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Giả lập User-Agent để tránh bị block
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    # Ẩn log rác
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    service = Service(driver_path)
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
        print(f"Lỗi đọc file: {e}")
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
# 4. SCRAPE LOGIC
# =========================
def scrape_power_outage(driver, ma_kh):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        driver.get('https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien')

        input_element = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.ID, 'idMaKhachHang'))
        )

        # Xóa nội dung cũ để đảm bảo kết quả mới
        driver.execute_script("document.getElementById('idThongTinLichNgungGiamMaKhachHang').innerHTML = '';")

        input_element.clear()
        input_element.send_keys(ma_kh)
        input_element.send_keys(Keys.RETURN)

        # Đợi kết quả trả về từ hệ thống
        WebDriverWait(driver, 30).until(
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
            result_text = " // ".join(all_results)
        else:
            result_text = text_content

        return {'Ma_KH': ma_kh, 'Thoi_gian_tra_cuu': current_time, 'Ket_qua': result_text}

    except Exception as e:
        return {'Ma_KH': ma_kh, 'Thoi_gian_tra_cuu': current_time, 'Ket_qua': f"Lỗi: {str(e)}"}


# =========================
# 5. WORKER
# =========================
def worker(ma_kh_list, thread_id, output_csv, driver_path):
    global processed
    driver = create_driver(driver_path)

    try:
        for ma_kh in ma_kh_list:
            # Retry logic
            result = scrape_power_outage(driver, ma_kh)
            
            with progress_lock:
                processed += 1
                print(f"📊 [{processed}/{total}] Thread {thread_id} xử lý xong: {ma_kh}")

            write_to_csv(output_csv, [result])
            
            # Delay ngẫu nhiên để tránh bị chặn
            time.sleep(random.uniform(2, 4))
    finally:
        driver.quit()


# =========================
# 6. TIỆN ÍCH CHIA DỮ LIỆU
# =========================
def split_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]


# =========================
# 7. MAIN RUNNER
# =========================
if __name__ == '__main__':
    input_file = "makh_list.csv"
    output_csv = "datasauget.csv"
    output_xlsx = "datasauget.xlsx"

    ma_kh_list = read_makh(input_file)
    if not ma_kh_list:
        print("❌ Không có dữ liệu mã khách hàng để xử lý.")
        exit()

    total = len(ma_kh_list)
    print(f"🚀 Tổng số mã: {total}")

    # Bước fix lỗi quan trọng: Cài đặt Driver trước khi chia luồng
    print("⏳ Đang chuẩn bị trình duyệt...")
    try:
        driver_path = ChromeDriverManager().install()
    except Exception as e:
        print(f"❌ Lỗi khi tải driver: {e}")
        exit()

    # Khởi tạo file CSV kết quả
    write_to_csv(output_csv, [], mode='w', header=True)

    # Chia luồng (Khuyên dùng 3-4 luồng trên GitHub Actions để ổn định)
    num_threads = 4
    chunks = split_list(ma_kh_list, num_threads)

    print(f"🔥 Bắt đầu chạy đa luồng ({num_threads} luồng)...")
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i, chunk in enumerate(chunks, start=1):
            futures.append(executor.submit(worker, chunk, i, output_csv, driver_path))

        for f in as_completed(futures):
            f.result()

    # Xuất Excel cuối cùng
    try:
        if os.path.exists(output_csv):
            df = pd.read_csv(output_csv)
            df.to_excel(output_xlsx, index=False)
            print(f"✅ HOÀN THÀNH! Kết quả lưu tại: {output_xlsx}")
    except Exception as e:
        print(f"❌ Lỗi khi chuyển đổi Excel: {e}")
