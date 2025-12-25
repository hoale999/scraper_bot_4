import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import urllib3
import ssl
import json
import re
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util import ssl_
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from urllib3.util.retry import Retry

# Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CẤU HÌNH SSL FIX (Giữ nguyên từ các bot trước) ---
class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl_.create_urllib3_context()
        ctx.options |= 0x4 
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx
        )

current_year = datetime.now().year

def fetch_sbt_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét SBT (Năm {current_year}) ---")
    
    base_url = "https://ttcagris.com.vn"
    # Danh sách URL theo yêu cầu của bạn
    targets = [
        ("SBT - ĐHĐCĐ Thường niên", f"{base_url}/quan-he-nha-dau-tu/dai-hoi-dong-co-dong?year={current_year}&cate=1"),
        ("SBT - ĐHĐCĐ Bất thường",  f"{base_url}/quan-he-nha-dau-tu/dai-hoi-dong-co-dong?year={current_year}&cate=2"),
        ("SBT - ĐHĐCĐ Lấy ý kiến",  f"{base_url}/quan-he-nha-dau-tu/dai-hoi-dong-co-dong?year={current_year}&cate=3"),
        ("SBT - BCTC Kiểm toán",    f"{base_url}/quan-he-nha-dau-tu/bao-cao-tai-chinh?year={current_year}&cate=1"),
        ("SBT - BCTC Quý",          f"{base_url}/quan-he-nha-dau-tu/bao-cao-tai-chinh?year={current_year}&cate=3"),
    ]

    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=15, verify=False)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # --- CHIẾN THUẬT V2: FIND BY DATE ---
            # 1. Tìm tất cả các text node có định dạng ngày dd/mm/yyyy
            # Regex: tìm chuỗi có 2 số / 2 số / 4 số
            date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
            
            # Nếu không tìm thấy bằng ngày, thử tìm các thẻ div có class chứa 'row' hoặc 'item' (Backup)
            if not date_nodes:
                # print(f"   ⚠️ Không thấy ngày tháng tại {source_label}, thử backup...")
                pass

            for node in date_nodes:
                try:
                    date_str = node.strip()
                    # Lọc chính xác chuỗi ngày (đôi khi nó nằm lẫn trong text dài)
                    match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                    if not match: continue
                    clean_date = match.group(1)

                    # 2. Từ node ngày, tìm parent là dòng chứa tin (thường là tr, li, hoặc div)
                    # Ta tìm thẻ cha gần nhất có chứa thẻ <a>
                    container = node.find_parent(['tr', 'div', 'li', 'p'])
                    
                    if not container: continue

                    # 3. Tìm link & Title trong container đó
                    link_tag = container.find('a')
                    
                    # Trường hợp đặc biệt: Đôi khi date nằm TRONG thẻ a, hoặc thẻ a nằm bên cạnh
                    if not link_tag:
                        # Thử tìm thẻ a ở cấp cao hơn một chút (ông nội)
                        container = container.parent
                        if container:
                            link_tag = container.find('a')

                    if not link_tag: continue

                    link = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)

                    # Làm sạch dữ liệu
                    if not link: continue
                    if link.startswith('/'): link = base_url + link
                    
                    # Loại bỏ các link rác nếu vẫn lọt lưới (Check độ dài title)
                    if len(title) < 5: continue 
                    if "facebook" in link.lower() or "youtube" in link.lower(): continue

                    # 4. Lưu kết quả
                    item_id = link
                    if item_id in seen_ids:
                        continue
                    
                    # Nếu đang filter năm 2025 mà web trả về tin cũ thì bỏ qua (tuỳ chọn)
                    # if str(current_year) not in clean_date: continue 

                    new_items.append({
                        "source": source_label,
                        "id": item_id,
                        "title": title,
                        "date": clean_date,
                        "link": link
                    })
                    seen_ids.add(item_id)

                except Exception as e:
                    continue

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items

def fetch_vgc_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét VGC (Năm {current_year}) ---")
    
    base_url = "https://www.viglacera.com.vn"
    targets = [
        ("VGC - Báo cáo tài chính", f"{base_url}/document-category/bao-cao-tai-chinh"),
        ("VGC - Báo cáo thường niên", f"{base_url}/document-category/bao-cao-thuong-nien"),
    ]

    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=15, verify=False)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # --- CHIẾN THUẬT 1: TÌM THEO NGÀY (Ưu tiên) ---
            # Tìm text dạng dd/mm/yyyy
            date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
            
            # Danh sách tạm để check trùng trong loop này
            found_in_pass_1 = False

            if date_nodes:
                for node in date_nodes:
                    try:
                        date_str = node.strip()
                        match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                        if not match: continue
                        clean_date = match.group(1)

                        # Từ ngày -> tìm ngược ra thẻ cha chứa Link
                        # Thử các thẻ bao phổ biến: div, li, tr, article
                        container = node.find_parent(['div', 'li', 'tr', 'article'])
                        if not container: continue

                        link_tag = container.find('a')
                        if not link_tag: continue

                        link = link_tag.get('href', '')
                        title = link_tag.get_text(strip=True)

                        if not link: continue
                        if link.startswith('/'): link = base_url + link

                        # Validate
                        if len(title) < 5: continue
                        
                        item_id = link
                        if item_id in seen_ids: continue

                        # Chỉ lấy năm hiện tại (nếu cần thiết)
                        # if str(current_year) not in clean_date and str(current_year) not in title: continue

                        new_items.append({
                            "source": source_label,
                            "id": item_id,
                            "title": title,
                            "date": clean_date,
                            "link": link
                        })
                        seen_ids.add(item_id)
                        found_in_pass_1 = True

                    except Exception:
                        continue
            
            # --- CHIẾN THUẬT 2: QUÉT LINK CHỨA NĂM (Dự phòng) ---
            # Nếu chiến thuật 1 không ra kết quả nào (do web ẩn ngày hoặc format lạ),
            # ta tìm các link có title chứa "2025" (current_year)
            if not found_in_pass_1:
                # print(f"   ⚠️ VGC: Không thấy ngày tại {source_label}, chuyển sang quét Title...")
                all_links = soup.find_all('a')
                for a in all_links:
                    title = a.get_text(strip=True)
                    link = a.get('href', '')
                    
                    if not link or len(title) < 10: continue
                    
                    # Điều kiện: Title phải chứa Năm hiện tại
                    if str(current_year) in title:
                        if link.startswith('/'): link = base_url + link
                        
                        item_id = link
                        if item_id in seen_ids: continue
                        
                        # Giả lập ngày vì không lấy được
                        fake_date = f"01/01/{current_year}"

                        new_items.append({
                            "source": source_label,
                            "id": item_id,
                            "title": title,
                            "date": fake_date,
                            "link": link
                        })
                        seen_ids.add(item_id)

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items

def fetch_shs_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét SHS (Năm {current_year}) ---")
    
    targets = [
        ("SHS - Báo cáo tài chính", "https://www.shs.com.vn/quan-he-co-dong/bao-cao-dinh-ky/TAICHINH"),
        ("SHS - ĐHĐCĐ", "https://dhcd.shs.com.vn/") 
    ]

    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # Chiến thuật: Quét text Ngày tháng (dd/mm/yyyy) -> Neo ngược ra Link
            date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
            
            for node in date_nodes:
                try:
                    date_str = node.strip()
                    match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                    if not match: continue
                    clean_date = match.group(1)

                    # Tìm container chứa link
                    container = node.find_parent(['tr', 'div', 'li', 'article', 'td'])
                    if not container: continue

                    link_tag = container.find('a')
                    
                    # Nếu container hiện tại không có a, thử nhảy lên 1 cấp nữa (trường hợp table td)
                    if not link_tag:
                         container = container.parent
                         if container: link_tag = container.find('a')
                    
                    if not link_tag: continue

                    link = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)

                    if not link: continue
                    # Xử lý link tương đối
                    if link.startswith('/'): 
                        # Với trang dhcd.shs.com.vn thì base là dhcd...
                        if "dhcd.shs" in url:
                            link = "https://dhcd.shs.com.vn" + link
                        else:
                            link = "https://www.shs.com.vn" + link

                    # Validate rác
                    if len(title) < 5: continue
                    
                    item_id = link
                    if item_id in seen_ids: continue

                    # Lọc năm (chỉ lấy tin năm hiện tại)
                    if str(current_year) not in clean_date: continue

                    new_items.append({
                        "source": source_label,
                        "id": item_id,
                        "title": title,
                        "date": clean_date,
                        "link": link
                    })
                    seen_ids.add(item_id)

                except Exception:
                    continue

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items

def fetch_mbs_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét MBS (Selenium V2 - Năm {current_year}) ---")
    
    # URL cập nhật theo cấu trúc thực tế thường gặp
    targets = [
        ("MBS - Tin cổ đông", "https://www.mbs.com.vn/tin-co-dong/"),
        ("MBS - Báo cáo tài chính", "https://www.mbs.com.vn/bao-cao-tai-chinh/")
    ]

    new_items = []
    
    # --- CẤU HÌNH ANT-DETECT ---
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Quan trọng: Tắt tính năng báo hiệu là Bot
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"[MBS] Lỗi khởi tạo Driver: {e}")
        return []

    try:
        for source_label, url in targets:
            try:
                # print(f"   >> Đang truy cập: {source_label}...")
                driver.get(url)
                
                # Chờ tối đa 15s để thẻ 'body' tải xong
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                # DEBUG: In ra title của trang để chắc chắn đã vào được
                page_title = driver.title
                # print(f"      [Debug] Page Title: {page_title}")

                # Scroll nhẹ để trigger load (nếu có)
                driver.execute_script("window.scrollTo(0, 1000);")
                time.sleep(3) # Chờ render

                # Lấy HTML
                html_source = driver.page_source
                soup = BeautifulSoup(html_source, 'html.parser')

                # --- CHIẾN THUẬT QUÉT "VÉT CẠN" ---
                # MBS thường bọc tin trong các thẻ có class: 'news-item', 'item', 'row', 'doc-item'
                # Ta sẽ tìm tất cả thẻ <a> và check điều kiện
                
                all_links = soup.find_all('a')
                # print(f"      [Debug] Tìm thấy {len(all_links)} thẻ <a>")

                count_added = 0
                for link_tag in all_links:
                    link = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)
                    
                    # Nếu title trống, lấy attribute title
                    if not title: title = link_tag.get('title', '')
                    
                    if not link or len(title) < 10: continue

                    # Bỏ qua link header/footer/menu
                    if "facebook" in link or "youtube" in link or "mailto" in link: continue

                    # --- Xử lý Ngày tháng ---
                    date_str = ""
                    
                    # Cách 1: Tìm thẻ ngày là anh em hoặc con cháu của thẻ link này
                    # (Thường gặp: <div> <a>Title</a> <span class='date'>...</span> </div>)
                    container = link_tag.find_parent(['div', 'li', 'tr', 'article'])
                    
                    if container:
                        container_text = container.get_text(" ", strip=True)
                        match = re.search(r"(\d{2}/\d{2}/\d{4})", container_text)
                        if match:
                            date_str = match.group(1)
                    
                    # Cách 2: Nếu không thấy ngày, check Title có chứa năm hiện tại không
                    if not date_str:
                        if str(current_year) in title:
                            date_str = f"01/01/{current_year}"

                    if not date_str: continue # Không có ngày -> Bỏ

                    # --- Validate ---
                    if str(current_year) not in date_str and str(current_year) not in title:
                        continue
                    
                    if not link.startswith('http'):
                        link = "https://www.mbs.com.vn" + link

                    if link in seen_ids: continue

                    new_items.append({
                        "source": source_label,
                        "id": link,
                        "title": title,
                        "date": date_str,
                        "link": link
                    })
                    seen_ids.add(link)
                    count_added += 1

                # print(f"      => Lấy được {count_added} tin.")

            except Exception as e:
                print(f"   ! Lỗi xử lý {source_label}: {e}")

    finally:
        driver.quit()

    return new_items

def fetch_dxg_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét DXG (Năm {current_year}) ---")
    
    url = "https://ir.datxanh.vn/cong-bo-thong-tin"
    
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    new_items = []

    try:
        resp = session.get(url, headers=headers, timeout=20, verify=False)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Chiến thuật: Tìm Date (dd/mm/yyyy) -> Tìm dòng chứa nó -> Tìm Link
        date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))

        for node in date_nodes:
            try:
                date_str = node.strip()
                match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                if not match: continue
                clean_date = match.group(1)

                # Tìm thẻ bao ngoài (Row hoặc Item)
                container = node.find_parent(['tr', 'div', 'li', 'article'])
                if not container: continue

                # Tìm Link & Title
                link_tag = container.find('a')
                
                # Nếu không thấy thẻ a ngay, thử tìm rộng ra 1 cấp
                if not link_tag:
                    container = container.parent
                    if container: link_tag = container.find('a')
                
                if not link_tag: continue

                link = link_tag.get('href', '')
                title = link_tag.get_text(strip=True)

                if not title: title = link_tag.get('title', '')
                if not link or len(title) < 5: continue

                # Xử lý Link tương đối
                if link.startswith('/'): 
                    link = "https://ir.datxanh.vn" + link

                # --- PHÂN LOẠI NGUỒN TIN (Source Classification) ---
                # Vì DXG gộp chung 1 link, ta phân loại dựa trên Title để dễ nhìn
                title_upper = title.upper()
                source_label = "DXG - Công bố thông tin" # Mặc định
                
                if "TÀI CHÍNH" in title_upper or "BCTC" in title_upper or "KIỂM TOÁN" in title_upper:
                    source_label = "DXG - Báo cáo tài chính"
                elif "ĐẠI HỘI" in title_upper or "CỔ ĐÔNG" in title_upper or "NGHỊ QUYẾT" in title_upper:
                    source_label = "DXG - ĐHĐCĐ"

                # Check trùng
                item_id = link
                if item_id in seen_ids: continue

                # Lọc năm
                if str(current_year) not in clean_date: continue

                new_items.append({
                    "source": source_label,
                    "id": item_id,
                    "title": title,
                    "date": clean_date,
                    "link": link
                })
                seen_ids.add(item_id)

            except Exception:
                continue

    except Exception as e:
        print(f"   ! Lỗi khi quét DXG: {e}")

    return new_items

# ==============================================================================
# 16. TCH - Tài chính Hoàng Huy (UPDATE V2 - Fix lỗi sót tin)
# ==============================================================================
# Vấn đề cũ: Chỉ bắt được BCTC (có ngày tháng), bỏ sót ĐHĐCĐ/HĐQT (thường ko có ngày).
# Giải pháp: Thêm cơ chế quét theo Tiêu đề chứa "2025".
# ==============================================================================

def fetch_tch_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét TCH (Năm {current_year}) ---")
    
    base_url = "https://www.hoanghuy.vn"
    targets = [
        ("TCH - ĐHĐCĐ", "https://www.hoanghuy.vn/dai-hoi-co-dong/"),
        ("TCH - HĐQT", "https://www.hoanghuy.vn/hoat-dong-cua-hoi-dong-quan-tri/"),
        ("TCH - BCTC", "https://www.hoanghuy.vn/bao-cao-tai-chinh/")
    ]

    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=20, verify=False)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # --- CHIẾN THUẬT 1: TÌM THEO NGÀY (Cho BCTC) ---
            # (Giữ nguyên logic cũ vì nó đang hoạt động tốt cho BCTC)
            date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
            found_ids_pass1 = set()

            for node in date_nodes:
                try:
                    date_str = node.strip()
                    match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                    if not match: continue
                    clean_date = match.group(1)

                    container = node.find_parent(['li', 'tr', 'div', 'p'])
                    if not container: continue

                    link_tag = container.find('a')
                    if not link_tag:
                         container = container.parent
                         if container: link_tag = container.find('a')
                    
                    if not link_tag: continue

                    link = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)
                    if not title: title = link_tag.get('title', '')
                    
                    if not link: continue
                    if link.startswith('/'): link = base_url + link
                    
                    # Validate
                    if len(title) < 5: continue
                    if str(current_year) not in clean_date: continue

                    item_id = link
                    if item_id in seen_ids: continue

                    new_items.append({
                        "source": source_label,
                        "id": item_id,
                        "title": title,
                        "date": clean_date,
                        "link": link
                    })
                    seen_ids.add(item_id)
                    found_ids_pass1.add(item_id)

                except Exception:
                    continue
            
            # --- CHIẾN THUẬT 2: QUÉT TIÊU ĐỀ (Cho ĐHĐCĐ và HĐQT) ---
            # Nếu item chưa được lấy ở pass 1, ta quét tiếp dựa trên Title chứa năm
            all_links = soup.find_all('a')
            for a in all_links:
                link = a.get('href', '')
                title = a.get_text(strip=True)
                
                if not title: title = a.get('title', '')
                if not link or len(title) < 5: continue
                
                # Chuẩn hóa link
                if link.startswith('/'): link = base_url + link
                
                # Bỏ qua nếu đã lấy ở pass 1 hoặc đã seen
                if link in found_ids_pass1 or link in seen_ids: continue
                
                # ĐIỀU KIỆN LẤY: Title phải chứa năm hiện tại (2025)
                # (Dành cho các mục ko có ngày tháng cụ thể, thường tiêu đề sẽ ghi "Năm 2025")
                if str(current_year) in title:
                    # Ngày giả lập (vì web không hiện ngày)
                    fake_date = f"01/01/{current_year}"
                    
                    new_items.append({
                        "source": source_label,
                        "id": link,
                        "title": title,
                        "date": fake_date, # Date giả để bot không báo lỗi
                        "link": link
                    })
                    seen_ids.add(link)

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items

def fetch_dcm_news(seen_ids):
    """
    Hàm cào Đạm Cà Mau (DCM).
    - Sử dụng Selenium để xử lý AJAX (box-document-ajax).
    - Quét 2 mục: BCTC và ĐHĐCĐ dựa trên cấu trúc HTML bạn cung cấp.
    """
    
    current_year = datetime.now().year
    url = "https://www.pvcfc.com.vn/quan-he-dau-tu"
    
    # --- CẤU HÌNH SELENIUM ---
    chrome_options = Options()
    chrome_options.add_argument("--headless") # Chạy ngầm
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    new_items = []
    
    print(f"--- 🚀 Bắt đầu quét DCM (Năm {current_year}) ---")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        driver.get(url)
        
        # 1. Chờ AJAX load dữ liệu (Quan trọng)
        # Web này load từng box, nên ta chờ khoảng 5-7 giây cho chắc
        time.sleep(7)
        
        # Lấy toàn bộ HTML đã render
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 2. Tìm các Box tài liệu (Dựa trên ảnh 1 và 3)
        # Class chung là "box-document"
        document_boxes = soup.select('.box-document')
        
        for box in document_boxes:
            # Lấy tiêu đề Box để phân loại (BCTC hay ĐHĐCĐ)
            # Selector: .title.uppercase (Ảnh 1)
            title_div = box.select_one('.title.uppercase')
            if not title_div: continue
            
            box_title_text = title_div.get_text(strip=True).lower()
            
            # Xác định loại tin
            category = None
            if "báo cáo tài chính" in box_title_text:
                category = "BCTC"
            elif "đại hội đồng cổ đông" in box_title_text:
                category = "ĐHĐCĐ"
            
            # Nếu không phải 2 mục cần tìm thì bỏ qua
            if not category: continue
            
            # 3. Quét các item bên trong Box này (Ảnh 2, 4, 5)
            # Selector: .document-item
            items = box.select('.document-item')
            
            count_in_box = 0
            for item in items:
                # --- A. Lấy Link & Title ---
                # Dựa trên ảnh 2: <a class="download" href="..." title="...">
                a_tag = item.select_one('a.download')
                if not a_tag: continue
                
                link = a_tag.get('href')
                # Lấy title từ thuộc tính title của thẻ a, nếu không có thì lấy text bên trong div title
                title = a_tag.get('title')
                
                if not title:
                    # Fallback: Lấy từ div.doc-title (Ảnh 2)
                    doc_title_div = item.select_one('.doc-title')
                    if doc_title_div: title = doc_title_div.get_text(strip=True)
                
                if not link or not title: continue
                
                # Chuẩn hóa link
                if not link.startswith('http'):
                    link = f"https://www.pvcfc.com.vn{link}"
                
                # --- B. Lấy Ngày tháng ---
                # Dựa trên ảnh 2: <time ...>Thứ ba, 28/10/2025</time>
                time_tag = item.select_one('time')
                date_str = str(current_year) # Mặc định
                
                if time_tag:
                    raw_date = time_tag.get_text(strip=True)
                    # Xử lý chuỗi "Thứ ba, 28/10/2025" -> Lấy "28/10/2025"
                    # Logic: Tìm chuỗi ngày/tháng/năm
                    match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', raw_date)
                    if match:
                        clean_date_str = match.group(1)
                        try:
                            pub_date = datetime.strptime(clean_date_str, "%d/%m/%Y")
                            
                            # LỌC NĂM: Chỉ lấy tin năm nay
                            if pub_date.year != current_year:
                                continue
                            
                            date_str = clean_date_str
                        except:
                            pass
                
                # --- C. Lưu trữ ---
                news_id = link
                if news_id in seen_ids: continue
                if any(x['id'] == news_id for x in new_items): continue
                
                new_items.append({
                    "source": f"DCM - {category}",
                    "id": news_id,
                    "title": title,
                    "date": date_str,
                    "link": link
                })
                count_in_box += 1
            
            # print(f"   > {category}: Tìm thấy {count_in_box} tin.")

    except Exception as e:
        print(f"[DCM] Lỗi Selenium: {e}")
    finally:
        driver.quit()
        
    return new_items

def fetch_vpi_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét VPI (Năm {current_year}) ---")
    
    url = "https://vanphu.vn/quan-he-co-dong/"
    
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    new_items = []

    try:
        resp = session.get(url, headers=headers, timeout=20, verify=False)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # --- CHIẾN THUẬT 1: TÌM THEO NGÀY (Ưu tiên) ---
        # Tìm các node text chứa ngày tháng dd/mm/yyyy
        date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
        
        # Danh sách item đã tìm thấy ở bước 1 (để bước 2 không lấy trùng)
        found_in_pass1 = set()

        for node in date_nodes:
            try:
                date_str = node.strip()
                match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                if not match: continue
                clean_date = match.group(1)

                # Từ ngày, tìm ra container (thẻ bao)
                container = node.find_parent(['div', 'li', 'tr', 'article', 'td'])
                if not container: continue

                # Tìm link
                link_tag = container.find('a')
                
                # Nếu không thấy link, thử nhảy lên cấp cha
                if not link_tag:
                    container = container.parent
                    if container: link_tag = container.find('a')
                
                if not link_tag: continue

                link = link_tag.get('href', '')
                title = link_tag.get_text(strip=True)
                
                # Fallback title
                if not title: title = link_tag.get('title', '')
                
                if not link or len(title) < 5: continue

                # Xử lý link
                if link.startswith('/'): 
                    link = "https://vanphu.vn" + link

                # Lọc năm hiện tại
                if str(current_year) not in clean_date: continue

                # Check trùng ID
                item_id = link
                if item_id in seen_ids: continue

                # --- PHÂN LOẠI TIN (Auto-Tagging) ---
                t_upper = title.upper()
                source_label = "VPI - Tin cổ đông" # Mặc định
                
                if "BCTC" in t_upper or "TÀI CHÍNH" in t_upper or "KIỂM TOÁN" in t_upper:
                    source_label = "VPI - Báo cáo tài chính"
                elif "ĐHĐCĐ" in t_upper or "ĐẠI HỘI" in t_upper or "NGHỊ QUYẾT" in t_upper:
                    source_label = "VPI - ĐHĐCĐ/HĐQT"
                elif "QUẢN TRỊ" in t_upper or "BÁO CÁO THƯỜNG NIÊN" in t_upper:
                    source_label = "VPI - Báo cáo quản trị/TN"

                new_items.append({
                    "source": source_label,
                    "id": item_id,
                    "title": title,
                    "date": clean_date,
                    "link": link
                })
                seen_ids.add(item_id)
                found_in_pass1.add(item_id)

            except Exception:
                continue

        # --- CHIẾN THUẬT 2: TÌM THEO TITLE CHỨA NĂM (Backup) ---
        # Dành cho các mục không hiện ngày ra ngoài (ví dụ Báo cáo thường niên chỉ ghi tên)
        all_links = soup.find_all('a')
        for a in all_links:
            link = a.get('href', '')
            title = a.get_text(strip=True)
            if not title: title = a.get('title', '')
            
            if not link or len(title) < 5: continue
            
            # Nếu item này đã lấy ở bước 1 rồi thì bỏ qua
            if link.startswith('/'): full_link = "https://vanphu.vn" + link
            else: full_link = link
            
            if full_link in found_in_pass1 or full_link in seen_ids: continue

            # Điều kiện: Title phải chứa "2025"
            if str(current_year) in title:
                # Phân loại lại
                t_upper = title.upper()
                source_label = "VPI - Tin khác"
                if "BCTC" in t_upper or "TÀI CHÍNH" in t_upper: source_label = "VPI - BCTC"
                elif "ĐHĐCĐ" in t_upper or "NGHỊ QUYẾT" in t_upper: source_label = "VPI - ĐHĐCĐ"
                
                fake_date = f"01/01/{current_year}"

                new_items.append({
                    "source": source_label,
                    "id": full_link,
                    "title": title,
                    "date": fake_date,
                    "link": full_link
                })
                seen_ids.add(full_link)

    except Exception as e:
        print(f"   ! Lỗi khi quét VPI: {e}")

    return new_items

def fetch_sjs_news(seen_ids):
    """
    Hàm cào SJ Group (SJS).
    - Sử dụng Requests (vì dữ liệu có trong view-source).
    - Lấy link từ thuộc tính 'data' của div.show-data.
    - Lọc: Chỉ lấy Tiếng Việt, bỏ Tiếng Anh.
    """
    
    current_year = datetime.now().year
    
    # Cấu hình 2 link gốc
    categories = [
        {
            "name": "Báo cáo tài chính",
            "url": "https://sjgroups.com.vn/bao-cao-tai-chinh-fd143.html"
        },
        {
            "name": "Đại hội đồng cổ đông",
            "url": "https://sjgroups.com.vn/tai-lieu-dai-hoi-dong-co-dong-fd144.html"
        }
    ]
    
    base_domain = "https://sjgroups.com.vn"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    
    # Setup session
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét SJS (Năm {current_year}) ---")

    for cat in categories:
        # Quét 3 trang đầu mỗi mục (thường là đủ cho 1 năm)
        for page in range(1, 4):
            params = {
                "publicdate_time": current_year, # Lọc theo năm 2025
                "page": page
            }
            
            try:
                response = session.get(cat['url'], headers=headers, params=params, timeout=20, verify=False)
                
                if response.status_code != 200:
                    print(f"[SJS] Lỗi kết nối {cat['name']}: {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Tìm bảng dữ liệu
                # Cấu trúc: table -> tbody -> tr (class odd/even)
                rows = soup.select('table tbody tr')
                
                if not rows:
                    # Nếu không có dòng nào -> Hết trang hoặc không có dữ liệu
                    break
                
                count_in_page = 0
                
                for row in rows:
                    # 1. TÌM TITLE & LINK (Cột 1 - class="first")
                    first_td = row.select_one('td.first')
                    if not first_td: continue
                    
                    # Dữ liệu nằm trong div class="show-data"
                    data_div = first_td.select_one('.show-data')
                    if not data_div: continue
                    
                    # Lấy Title
                    title = data_div.get_text(strip=True)
                    
                    # --- LỌC NGÔN NGỮ ---
                    # Chỉ lấy Tiếng Việt -> Bỏ Tiếng Anh
                    if "tiếng anh" in title.lower() or "english" in title.lower():
                        continue
                        
                    # Lấy Link từ thuộc tính 'data' (Đây là chìa khóa!)
                    # data="/download-file.html?id=..."
                    relative_link = data_div.get('data')
                    
                    if not relative_link: continue
                    
                    full_link = f"{base_domain}{relative_link}"
                    
                    # 2. TÌM NGÀY THÁNG (Cột 2 - class="released")
                    # Dựa vào ảnh 44d072.png, cột ngày có id/class liên quan publicdate
                    # Nhưng inspect code trong ảnh thấy: <td class="released">23-10-2025</td> (đoán class dựa trên thói quen code table)
                    # Nếu soi kỹ ảnh 4: Cột ngày nằm ngay sau cột title.
                    # Ta lấy danh sách td, ngày thường là td thứ 2 (index 1)
                    tds = row.find_all('td')
                    date_str = str(current_year)
                    if len(tds) >= 2:
                        raw_date = tds[1].get_text(strip=True) # VD: 23-10-2025
                        try:
                            # Parse ngày
                            datetime.strptime(raw_date, "%d-%m-%Y")
                            date_str = raw_date
                        except:
                            pass # Nếu lỗi thì giữ nguyên current_year

                    # 3. Check trùng & Lưu
                    news_id = full_link
                    if news_id in seen_ids: continue
                    if any(x['id'] == news_id for x in new_items): continue

                    new_items.append({
                        "source": f"SJS - {cat['name']}",
                        "id": news_id,
                        "title": title,
                        "date": date_str,
                        "link": full_link
                    })
                    count_in_page += 1
                
                # Nếu trang này không có tin nào (sau khi lọc tiếng Anh) -> Có thể vẫn còn tin tiếng Việt ở trang sau?
                # Nhưng nếu rows rỗng thì break. Nếu rows có mà filtered hết thì cứ chạy tiếp trang sau cho chắc.
                if len(rows) == 0:
                    break
                
                time.sleep(0.5)

            except Exception as e:
                print(f"[SJS] Lỗi tại {cat['name']}: {e}")
                break
                
    return new_items

def fetch_nlg_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét NLG (Năm {current_year}) ---")
    
    url = "https://www.namlongvn.com/quan-he-nha-dau-tu/"
    
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    new_items = []

    try:
        resp = session.get(url, headers=headers, timeout=30, verify=False)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # --- CHIẾN THUẬT: QUÉT TẤT CẢ THẺ A & LỌC THEO URL KEYWORD ---
        all_links = soup.find_all('a', href=True)
        
        # Các từ khóa bắt buộc phải có trong Link (hoặc Title)
        # Nhóm 1: BCTC (như ảnh 1,2,3)
        kw_bctc = ['bao-cao-tai-chinh', 'bctc', 'financial-report']
        # Nhóm 2: ĐHĐCĐ & Cổ tức (như ảnh 4,5 và yêu cầu của bạn)
        kw_dhcd = ['dai-hoi-dong-co-dong', 'lay-y-kien', 'co-tuc']

        for a in all_links:
            try:
                link = a['href']
                title = a.get_text(strip=True)
                if not title: title = a.get('title', '')
                
                # Validate cơ bản
                if not link or len(title) < 5: continue
                
                # Chuẩn hóa link
                if link.startswith('/'): 
                    link = "https://www.namlongvn.com" + link
                
                link_lower = link.lower()
                title_lower = title.lower()

                # --- BƯỚC 1: PHÂN LOẠI & LỌC ---
                is_bctc = any(k in link_lower for k in kw_bctc)
                is_dhcd = any(k in link_lower for k in kw_dhcd)

                # Nếu không thuộc 2 nhóm này -> BỎ QUA (theo yêu cầu chỉ lấy đúng loại)
                if not is_bctc and not is_dhcd:
                    continue

                # Gán nhãn Source
                source_label = "NLG - Tin tức"
                if is_bctc: source_label = "NLG - Báo cáo tài chính"
                elif is_dhcd: source_label = "NLG - ĐHĐCĐ/Cổ tức"

                # --- BƯỚC 2: TÌM NGÀY THÁNG ---
                # Tìm ngày trong chính thẻ a hoặc thẻ cha của nó
                date_str = ""
                
                # Thử tìm trong thẻ cha (div/li/tr)
                container = a.find_parent(['div', 'li', 'tr', 'article'])
                if container:
                    txt = container.get_text(" ", strip=True)
                    match = re.search(r"(\d{2}/\d{2}/\d{4})", txt)
                    if match: date_str = match.group(1)
                
                # Nếu không thấy ngày, nhưng Title có chứa Năm hiện tại -> Lấy luôn
                if not date_str:
                    if str(current_year) in title:
                        date_str = f"01/01/{current_year}"

                # --- BƯỚC 3: CHECK ---
                if not date_str: continue # Không có ngày -> Bỏ
                
                # Lọc năm 2025
                if str(current_year) not in date_str and str(current_year) not in title:
                    continue

                if link in seen_ids: continue

                new_items.append({
                    "source": source_label,
                    "id": link,
                    "title": title,
                    "date": date_str,
                    "link": link
                })
                seen_ids.add(link)

            except Exception:
                continue

    except Exception as e:
        print(f"   ! Lỗi khi quét NLG: {e}")

    return new_items

def fetch_pvs_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét PVS (Năm {current_year}) ---")
    
    # URL mục BCTC
    url = "https://www.ptsc.com.vn/co-dong/danh-cho-co-dong/bao-cao-tai-chinh"
    
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    new_items = []

    try:
        resp = session.get(url, headers=headers, timeout=30, verify=False)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # --- CHIẾN THUẬT 1: TÌM THEO NGÀY (Ưu tiên) ---
        # Tìm text chứa ngày tháng dạng dd/mm/yyyy
        date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
        
        found_ids_pass1 = set()

        for node in date_nodes:
            try:
                date_str = node.strip()
                match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                if not match: continue
                clean_date = match.group(1)

                # Tìm thẻ bao (container) chứa link
                # PVS thường dùng div class 'item' hoặc tr
                container = node.find_parent(['div', 'li', 'tr', 'article'])
                if not container: continue

                # Tìm Link & Title
                link_tag = container.find('a')
                
                # Nếu không thấy link ngay cạnh ngày, thử tìm trong thẻ cha của container
                if not link_tag:
                     container = container.parent
                     if container: link_tag = container.find('a')
                
                if not link_tag: continue

                link = link_tag.get('href', '')
                title = link_tag.get_text(strip=True)
                if not title: title = link_tag.get('title', '')

                if not link or len(title) < 5: continue
                
                # Chuẩn hóa Link
                if link.startswith('/'): 
                    link = "https://www.ptsc.com.vn" + link

                # Lọc năm 2025
                if str(current_year) not in clean_date: continue

                # Check trùng
                item_id = link
                if item_id in seen_ids: continue

                new_items.append({
                    "source": "PVS - Báo cáo tài chính",
                    "id": item_id,
                    "title": title,
                    "date": clean_date,
                    "link": link
                })
                seen_ids.add(item_id)
                found_ids_pass1.add(item_id)

            except Exception:
                continue

        # --- CHIẾN THUẬT 2: QUÉT THEO TITLE CHỨA NĂM (Backup) ---
        # Nếu web PVS ẩn ngày tháng, ta quét các link có chứa "2025" trong title
        all_links = soup.find_all('a')
        for a in all_links:
            link = a.get('href', '')
            title = a.get_text(strip=True)
            if not title: title = a.get('title', '')
            
            if not link or len(title) < 5: continue
            
            # Chuẩn hóa link
            if link.startswith('/'): link = "https://www.ptsc.com.vn" + link
            
            # Bỏ qua nếu đã lấy ở pass 1 hoặc đã seen
            if link in found_ids_pass1 or link in seen_ids: continue
            
            # Điều kiện: Title phải chứa "2025"
            if str(current_year) in title:
                new_items.append({
                    "source": "PVS - Báo cáo tài chính",
                    "id": link,
                    "title": title,
                    "date": f"01/01/{current_year}", # Giả lập ngày
                    "link": link
                })
                seen_ids.add(link)

    except Exception as e:
        print(f"   ! Lỗi khi quét PVS: {e}")

    return new_items

def fetch_tal_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét TAL (Năm {current_year}) ---")
    
    base_url = "https://tasecoland.vn"
    targets = [
        ("TAL - ĐHĐCĐ", "https://tasecoland.vn/dai-hoi-dong-co-dong-nam-2025-34251157"),
        ("TAL - ĐHĐCĐ (Tài liệu)", "https://tasecoland.vn/dai-hoi-dong-co-dong-nam-2025-34251157?tailieu=2"),
        ("TAL - Báo cáo tài chính", "https://tasecoland.vn/bao-cao-tai-chinh-nam-2025-34251249")
    ]

    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=30, verify=False)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # --- CHIẾN THUẬT: VÉT CẠN LINK (Safe Scan) ---
            # Tìm tất cả thẻ a, sau đó lọc kỹ
            all_links = soup.find_all('a', href=True)

            for a in all_links:
                try:
                    link = a['href']
                    title = a.get_text(strip=True)
                    # Nếu title rỗng, lấy attribute title
                    if not title: title = a.get('title', '')
                    
                    if not link or len(title) < 5: continue

                    # --- 1. LỌC NGÔN NGỮ (VIETNAMESE ONLY) ---
                    title_lower = title.lower()
                    link_lower = link.lower()
                    
                    # Bỏ qua nếu là tiếng Anh
                    if "english" in title_lower or "(en)" in title_lower or "_en" in link_lower:
                        continue

                    # --- 2. XỬ LÝ NGÀY THÁNG ---
                    date_str = ""
                    
                    # Cách 1: Tìm ngày trong text của thẻ cha (div/li/tr/td)
                    # TAL thường để ngày trong 1 thẻ span hoặc td bên cạnh
                    container = a.find_parent(['tr', 'li', 'div', 'p'])
                    if container:
                        txt = container.get_text(" ", strip=True)
                        match = re.search(r"(\d{2}/\d{2}/\d{4})", txt)
                        if match: date_str = match.group(1)

                    # Cách 2: Backup - Nếu title chứa năm 2025 -> Lấy
                    if not date_str:
                        if str(current_year) in title:
                            date_str = f"01/01/{current_year}"

                    if not date_str: continue

                    # --- 3. CHECK HỢP LỆ ---
                    # Chuẩn hóa link
                    if link.startswith('/'): 
                        link = base_url + link
                    
                    # Lọc năm 2025
                    if str(current_year) not in date_str and str(current_year) not in title:
                        continue

                    if link in seen_ids: continue

                    new_items.append({
                        "source": source_label,
                        "id": link,
                        "title": title,
                        "date": date_str,
                        "link": link
                    })
                    seen_ids.add(link)

                except Exception:
                    continue

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items

def fetch_qns_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét QNS (Năm {current_year}) ---")
    
    base_url = "https://qns.com.vn"
    targets = [
        ("QNS - ĐHĐCĐ", "https://qns.com.vn/dai-hoi-co-dong"),
        ("QNS - Báo cáo tài chính", "https://qns.com.vn/bao-cao-tai-chinh")
    ]

    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=30, verify=False)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # --- CHIẾN THUẬT 1: TÌM THEO NGÀY (Ưu tiên) ---
            # Tìm text chứa ngày tháng dạng dd/mm/yyyy hoặc dd-mm-yyyy
            date_nodes = soup.find_all(string=re.compile(r'\d{2}[/-]\d{2}[/-]\d{4}'))
            
            found_ids_pass1 = set()

            for node in date_nodes:
                try:
                    date_str = node.strip()
                    match = re.search(r"(\d{2}[/-]\d{2}[/-]\d{4})", date_str)
                    if not match: continue
                    clean_date = match.group(1).replace('-', '/') # Chuẩn hóa về /

                    # Tìm thẻ bao (container) chứa link
                    # QNS thường dùng thẻ div class 'item' hoặc tr/li
                    container = node.find_parent(['div', 'li', 'tr', 'article', 'td'])
                    if not container: continue

                    # Tìm Link
                    link_tag = container.find('a')
                    
                    # Nếu không thấy link ngay cạnh ngày, thử tìm trong thẻ cha của container (leo lên 1 cấp)
                    if not link_tag:
                         container = container.parent
                         if container: link_tag = container.find('a')
                    
                    if not link_tag: continue

                    link = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)
                    if not title: title = link_tag.get('title', '')

                    if not link or len(title) < 5: continue
                    
                    # Chuẩn hóa Link
                    if link.startswith('/'): 
                        link = base_url + link

                    # Lọc năm 2025
                    if str(current_year) not in clean_date: continue

                    # Check trùng
                    item_id = link
                    if item_id in seen_ids: continue

                    new_items.append({
                        "source": source_label,
                        "id": item_id,
                        "title": title,
                        "date": clean_date,
                        "link": link
                    })
                    seen_ids.add(item_id)
                    found_ids_pass1.add(item_id)

                except Exception:
                    continue

            # --- CHIẾN THUẬT 2: QUÉT THEO TITLE CHỨA NĂM (Backup) ---
            # Nếu web QNS ẩn ngày tháng ở một số mục, quét title có "2025"
            all_links = soup.find_all('a')
            for a in all_links:
                link = a.get('href', '')
                title = a.get_text(strip=True)
                if not title: title = a.get('title', '')
                
                if not link or len(title) < 5: continue
                
                # Chuẩn hóa link
                if link.startswith('/'): link = base_url + link
                
                # Bỏ qua nếu đã lấy ở pass 1 hoặc đã seen
                if link in found_ids_pass1 or link in seen_ids: continue
                
                # Điều kiện: Title phải chứa "2025"
                if str(current_year) in title:
                    new_items.append({
                        "source": source_label,
                        "id": link,
                        "title": title,
                        "date": f"01/01/{current_year}", # Giả lập ngày
                        "link": link
                    })
                    seen_ids.add(link)

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items

def fetch_dig_news(seen_ids):
    """
    Hàm cào DIC Corp (DIG).
    - Cấu trúc: HTML tĩnh.
    - Dữ liệu nằm trong div.intro.intro1
    - Ngày tháng: <i> nằm trong <span> sau icon calendar.
    """
    
    current_year = datetime.now().year
    base_domain = "https://www.dic.vn"
    
    # Các danh mục cần cào (dựa trên link bạn gửi và cấu trúc trong ảnh 4 có thêm 'cong-bo-thong-tin')
    categories = [
        "dai-hoi-co-dong-thuong-nien",
        "bao-cao-tai-chinh",
        "cong-bo-thong-tin" # Thêm cái này dựa trên ảnh 4 bạn gửi
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét DIG (Năm {current_year}) ---")

    for cat in categories:
        url = f"{base_domain}/{cat}"
        
        # Trang này thường show nhiều tin một lúc, nhưng ta cứ thử loop page=1,2 nếu cần
        # Tuy nhiên link bạn đưa là dạng category root, ta quét trang đầu trước.
        # Nếu web dùng pagination dạng ?page=2 hoặc /page/2, bạn có thể mở rộng vòng lặp.
        # Ở đây mình quét trang chủ của danh mục (thường chứa tin mới nhất).
        
        try:
            # print(f"   >> Đang tải: {cat}...")
            response = session.get(url, headers=headers, timeout=20, verify=False)
            
            if response.status_code != 200:
                print(f"[DIG] Lỗi kết nối {cat}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm các khối tin (Ảnh 1, 3: div class="item col-md-6")
            items = soup.select('.item.col-md-6')
            
            count_in_cat = 0
            
            for item in items:
                # Tìm vào khối intro1 (Ảnh 1, 3)
                intro_div = item.select_one('.intro.intro1')
                if not intro_div: continue
                
                # 1. Lấy Tiêu đề & Link
                title_tag = intro_div.select_one('a.title')
                if not title_tag: continue
                
                title = title_tag.get_text(strip=True)
                relative_link = title_tag.get('href')
                
                if not relative_link: continue
                
                # Xử lý Link (Link trong ảnh là relative: "bao-cao-tai-chinh/...")
                if not relative_link.startswith('http'):
                    # Đảm bảo không bị double slash
                    if relative_link.startswith('/'):
                        full_link = f"{base_domain}{relative_link}"
                    else:
                        full_link = f"{base_domain}/{relative_link}"
                else:
                    full_link = relative_link

                # 2. Lấy Ngày tháng
                # Cấu trúc ảnh 1: <i class="fa fa-calendar"></i><span><i> 21/04/2025</i></span>
                # Tìm thẻ i có class fa-calendar, sau đó tìm thẻ span kế tiếp, rồi lấy text bên trong
                date_str = str(current_year)
                
                # Cách 1: Tìm theo sibling
                calendar_icon = intro_div.select_one('.fa-calendar')
                if calendar_icon:
                    # Tìm thẻ span ngay sau icon
                    date_span = calendar_icon.find_next_sibling('span')
                    if date_span:
                        raw_date = date_span.get_text(strip=True) # VD: 21/04/2025
                        try:
                            pub_date = datetime.strptime(raw_date, "%d/%m/%Y")
                            
                            # LỌC NĂM
                            if pub_date.year != current_year:
                                continue # Bỏ qua tin cũ
                                
                            date_str = raw_date
                        except:
                            pass
                
                # 3. Check trùng & Lưu
                news_id = full_link
                if news_id in seen_ids: continue
                if any(x['id'] == news_id for x in new_items): continue

                new_items.append({
                    "source": f"DIG - {cat}",
                    "id": news_id,
                    "title": title,
                    "date": date_str,
                    "link": full_link
                })
                count_in_cat += 1
            
            time.sleep(0.5)

        except Exception as e:
            print(f"[DIG] Lỗi xử lý {cat}: {e}")
            continue

    return new_items

def fetch_dpm_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét DPM (Năm {current_year}) ---")
    
    base_url = "https://dpm.vn"
    targets = [
        ("DPM - Báo cáo tài chính", "https://dpm.vn/bao-cao-tai-chinh"),
        ("DPM - Công bố thông tin", "https://dpm.vn/cong-bo-thong-tin")
    ]

    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    # Từ khóa bắt buộc cho mục Công bố thông tin
    keywords_filter = ['dai-hoi-dong-co-dong', 'hoi-dong-quan-tri', 'co-tuc', 'lay-y-kien']

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=30, verify=False)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # --- CHIẾN THUẬT: QUÉT TẤT CẢ LINK (VÉT CẠN & LỌC) ---
            # Tìm tất cả thẻ a, sau đó lọc kỹ
            all_links = soup.find_all('a', href=True)

            for a in all_links:
                try:
                    link = a['href']
                    title = a.get_text(strip=True)
                    if not title: title = a.get('title', '')
                    
                    if not link or len(title) < 5: continue

                    # Chuẩn hóa link
                    if link.startswith('/'): 
                        link = base_url + link

                    # --- 2. TÌM NGÀY THÁNG ---
                    date_str = ""
                    
                    # Cách 1: Tìm ngày trong text của thẻ cha (div/li/tr/td)
                    # DPM thường có thẻ <span class="date"> hoặc tương tự
                    container = a.find_parent(['div', 'li', 'tr', 'article'])
                    if container:
                        txt = container.get_text(" ", strip=True)
                        # Regex tìm dd/mm/yyyy
                        match = re.search(r"(\d{2}/\d{2}/\d{4})", txt)
                        if match: date_str = match.group(1)

                    # Cách 2: Nếu không thấy ngày, check Title chứa năm hiện tại
                    if not date_str:
                        if str(current_year) in title:
                            date_str = f"01/01/{current_year}"

                    if not date_str: continue

                    # --- 3. CHECK HỢP LỆ ---
                    # Lọc năm 2025
                    if str(current_year) not in date_str and str(current_year) not in title:
                        continue

                    if link in seen_ids: continue

                    new_items.append({
                        "source": source_label,
                        "id": link,
                        "title": title,
                        "date": date_str,
                        "link": link
                    })
                    seen_ids.add(link)

                except Exception:
                    continue

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items

def fetch_vcg_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét VCG (Selenium - Năm {current_year}) ---")
    
    targets = [
        ("VCG - Báo cáo tài chính", "https://vinaconex.com.vn/quan-he-co-dong/bao-cao-tai-chinh"),
        ("VCG - ĐHĐCĐ", "https://vinaconex.com.vn/quan-he-co-dong/dai-hoi-co-dong")
    ]

    new_items = []
    
    # Cấu hình Selenium
    chrome_options = Options()
    chrome_options.add_argument("--headless") # Chạy ngầm (nếu muốn xem tận mắt thì comment dòng này)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    chrome_options.add_argument("--ignore-certificate-errors")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
    except Exception as e:
        print(f"[VCG] Lỗi khởi tạo Driver: {e}")
        return []

    try:
        for source_label, url in targets:
            try:
                # print(f"   >> Đang truy cập: {source_label}...")
                driver.get(url)
                
                # Chờ nội dung load (tìm thẻ body hoặc vùng content)
                try:
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
                except:
                    pass
                
                time.sleep(3) # Chờ thêm chút cho chắc
                
                # Lấy HTML về xử lý bằng BeautifulSoup cho nhanh và chuẩn
                html_source = driver.page_source
                soup = BeautifulSoup(html_source, 'html.parser')

                # --- CHIẾN THUẬT: QUÉT TẤT CẢ LINK ---
                # Tìm vùng nội dung chính để loại bỏ Menu/Footer
                # VCG thường để nội dung trong col-md-9 hoặc col-lg-9
                content_area = soup.find('div', class_=re.compile(r'(col-md|col-lg|main-content)'))
                if not content_area: content_area = soup # Fallback

                all_links = content_area.find_all('a', href=True)
                
                count_found = 0
                for a in all_links:
                    try:
                        link = a['href']
                        title = a.get_text(strip=True)
                        if not title: title = a.get('title', '')
                        
                        # Bỏ qua link quá ngắn hoặc rỗng
                        if not link or len(title) < 5: continue
                        
                        # --- 1. LỌC HEADER (QUAN TRỌNG) ---
                        # Nếu tiêu đề chỉ chứa thông tin chung chung như "Quý 3/2025", "Năm 2025" -> Bỏ qua
                        t_lower = title.lower()
                        # Regex check xem title có phải chỉ toàn là "Quý ... Năm ..." không
                        if len(title) < 25 and ("quý" in t_lower or "năm" in t_lower or "bán niên" in t_lower):
                            # Ví dụ: "Báo cáo tài chính Quý 3 năm 2025" -> Có thể là header
                            # Nhưng "Giải trình BCTC Quý 3..." -> Là tin thật
                            # Cách đơn giản: Header thường không có ngày tháng cụ thể (dd/mm)
                            pass 

                        # --- 2. TÌM NGÀY THÁNG ---
                        date_str = ""
                        
                        # Tìm trong text của thẻ a hoặc thẻ cha (li, tr, div)
                        container = a.find_parent(['li', 'tr', 'div', 'p'])
                        if container:
                            txt = container.get_text(" ", strip=True)
                            # Regex tìm dd/mm/yyyy hoặc dd.mm.yyyy
                            match = re.search(r"(\d{2}[./-]\d{2}[./-]\d{4})", txt)
                            if match: 
                                date_str = match.group(1).replace('.', '/').replace('-', '/')

                        # Backup: Nếu không thấy ngày, check xem Title có năm hiện tại không
                        # NHƯNG: Với VCG, nếu dùng backup này dễ dính Header.
                        # Nên ta chỉ dùng backup nếu title ĐỦ DÀI (nghĩa là title văn bản thực sự)
                        if not date_str:
                            if str(current_year) in title and len(title) > 20: 
                                date_str = f"01/01/{current_year}"

                        if not date_str: continue

                        # --- 3. CHECK CUỐI ---
                        if str(current_year) not in date_str and str(current_year) not in title:
                            continue

                        # Chuẩn hóa Link
                        if not link.startswith('http'):
                            if link.startswith('/'):
                                link = "https://vinaconex.com.vn" + link
                            else:
                                link = "https://vinaconex.com.vn/" + link

                        if link in seen_ids: continue

                        new_items.append({
                            "source": source_label,
                            "id": link,
                            "title": title,
                            "date": date_str,
                            "link": link
                        })
                        seen_ids.add(link)
                        count_found += 1

                    except Exception:
                        continue
                
                # print(f"      => Tìm thấy {count_found} tin.")

            except Exception as e:
                print(f"   ! Lỗi khi quét {source_label}: {e}")

    finally:
        driver.quit()

    return new_items

def fetch_idc_news(seen_ids):
    """
    Hàm cào IDICO (IDC).
    - Lọc cứng năm 2025.
    - Lấy ngày tháng từ Item cha (Bài viết) để chính xác thời điểm công bố.
    - Lọc từ khóa cho mục CBTT.
    """
    
    # --- CẤU HÌNH ---
    TARGET_YEAR = 2025  # <--- GÁN CỨNG NĂM 2025
    base_api_domain = "https://admin.idico.com.vn"
    
    keywords_cbtt = [
        "cổ tức", "báo cáo tài chính", "bctc", 
        "hội đồng quản trị", "hđqt", "lấy ý kiến"
    ]

    targets = [
        {
            "name": "Đại hội cổ đông",
            "url": "https://admin.idico.com.vn/api/tai-lieus",
            "params": {
                "populate": "files.media",
                "filters[category][$eq]": "Đại hội cổ đông",
                "filters[files][title][$containsi]": "",
                "locale": "vi",
                "sort[0]": "updatedAt:desc"
            },
            "filter_keywords": False
        },
        {
            "name": "Công bố thông tin",
            "url": "https://admin.idico.com.vn/api/tai-lieus",
            "params": {
                "populate": "files.media",
                "filters[category][$eq]": "Công bố thông tin",
                "filters[files][title][$containsi]": "",
                "locale": "vi",
                "sort[0]": "updatedAt:desc"
            },
            "filter_keywords": True
        }
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.idico.com.vn",
        "Referer": "https://www.idico.com.vn/"
    }

    new_items = []
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())

    print(f"--- 🚀 Bắt đầu quét IDC (Chỉ lấy năm {TARGET_YEAR}) ---")

    for target in targets:
        try:
            response = session.get(target["url"], headers=headers, params=target["params"], timeout=20, verify=False)
            if response.status_code != 200:
                print(f"[IDC] Lỗi API {target['name']}: {response.status_code}")
                continue

            json_data = response.json()
            data_list = json_data.get("data", [])
            
            if not data_list: continue

            count_in_cat = 0
            
            for item in data_list:
                attributes = item.get("attributes", {})
                
                # --- 1. KIỂM TRA NGÀY CỦA GÓI TIN (QUAN TRỌNG) ---
                # Ưu tiên updatedAt của bài viết (Item cha)
                item_date_str = attributes.get("updatedAt") 
                if not item_date_str: 
                    item_date_str = attributes.get("publishedAt") # Fallback
                
                date_display = str(TARGET_YEAR)
                is_valid_year = False
                
                if item_date_str:
                    try:
                        # Format: 2025-07-29T10:13:42.100Z
                        dt_obj = datetime.fromisoformat(item_date_str.replace("Z", "+00:00"))
                        
                        # LOGIC LỌC NĂM
                        if dt_obj.year == TARGET_YEAR:
                            is_valid_year = True
                            date_display = dt_obj.strftime("%d/%m/%Y")
                    except:
                        pass
                
                # Nếu bài viết không phải năm 2025 -> Bỏ qua
                if not is_valid_year: 
                    continue

                # --- 2. XỬ LÝ FILES ---
                files = attributes.get("files", [])
                # Trường hợp files là dict (một file) hoặc list (nhiều file)
                if isinstance(files, dict) and "data" in files:
                     # Đôi khi Strapi trả về cấu trúc lạ, nhưng theo ảnh bạn gửi thì files là mảng trực tiếp bên trong attributes?
                     # Nhìn lại ảnh 1: attributes -> files -> [ {id:..., title:...} ]
                     # Vậy files là list các object file.
                     pass
                
                if not files: continue

                for file_info in files:
                    title = file_info.get("title")
                    if not title: continue

                    # Lọc Từ khóa (Chỉ áp dụng cho CBTT)
                    if target["filter_keywords"]:
                        lower_title = title.lower()
                        is_match = False
                        for kw in keywords_cbtt:
                            if kw.lower() in lower_title:
                                is_match = True
                                break
                        if not is_match: continue 

                    # Lấy Link
                    media_obj = file_info.get("media", {})
                    if not media_obj: continue
                    
                    media_data = media_obj.get("data")
                    if not media_data: continue
                    
                    media_attrs = media_data.get("attributes", {})
                    relative_url = media_attrs.get("url")
                    
                    if not relative_url: continue
                    full_link = f"{base_api_domain}{relative_url}"

                    # Check trùng
                    news_id = full_link
                    if news_id in seen_ids: continue
                    if any(x['id'] == news_id for x in new_items): continue

                    new_items.append({
                        "source": f"IDC - {target['name']}",
                        "id": news_id,
                        "title": title,
                        "date": date_display,
                        "link": full_link
                    })
                    count_in_cat += 1
            
            # print(f"   > {target['name']}: Lấy được {count_in_cat} tài liệu.")
            time.sleep(0.5)

        except Exception as e:
            print(f"[IDC] Lỗi xử lý {target['name']}: {e}")
            continue

    return new_items

def fetch_abb_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét ABB (Năm {current_year}) ---")
    
    url = "https://abbank.vn/thong-tin/tin-tuc-co-dong"
    
    # Cấu hình Mạng mạnh (Retry + Headers xịn)
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.verify = False # Bỏ qua lỗi SSL (thường gặp với web bank VN)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://abbank.vn/'
    }

    new_items = []

    try:
        resp = session.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # --- CHIẾN THUẬT: NEO THEO NGÀY (Date Anchoring) ---
        # Tìm tất cả các text có định dạng ngày dd/mm/yyyy
        date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
        
        for node in date_nodes:
            try:
                date_str = node.strip()
                # Lấy chính xác ngày (phòng trường hợp text có lẫn chữ khác)
                match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                if not match: continue
                clean_date = match.group(1)

                # Tìm thẻ bao (container) chứa cả Ngày và Link
                # Thường là thẻ div, li, tr hoặc article
                container = node.find_parent(['div', 'li', 'tr', 'article', 'h3', 'p'])
                if not container: continue

                # Từ container, tìm Link (thẻ a)
                link_tag = container.find('a')
                
                # Nếu không thấy link, thử leo lên 1 cấp cha nữa (trường hợp cấu trúc lồng nhau phức tạp)
                if not link_tag:
                    container = container.parent
                    if container: link_tag = container.find('a')
                
                if not link_tag: continue

                link = link_tag.get('href', '')
                title = link_tag.get_text(strip=True)
                
                # Nếu title trong thẻ a quá ngắn (ví dụ "Xem thêm"), tìm title ở thẻ khác trong cùng container
                if len(title) < 5:
                    title_tag = container.find(['h2', 'h3', 'h4', 'span'], class_=re.compile(r'(title|name)'))
                    if title_tag: title = title_tag.get_text(strip=True)
                    # Nếu vẫn không có, lấy attribute title của thẻ a
                    if not title: title = link_tag.get('title', '')

                if not link or len(title) < 5: continue

                # Chuẩn hóa Link (ABB hay dùng link tương đối)
                if link.startswith('/'):
                    link = "https://abbank.vn" + link

                # Lọc Năm (Chỉ lấy 2025)
                if str(current_year) not in clean_date: continue

                # Check trùng
                item_id = link
                if item_id in seen_ids: continue

                new_items.append({
                    "source": "ABB - Tin cổ đông",
                    "id": item_id,
                    "title": title,
                    "date": clean_date,
                    "link": link
                })
                seen_ids.add(item_id)

            except Exception:
                continue
                
        # --- CHIẾN THUẬT PHỤ: QUÉT LINK CHỨA NĂM (Backup) ---
        # Nếu web ẩn ngày, quét các link có title chứa "2025"
        if not new_items:
            # print("   ⚠️ Không thấy ngày, quét backup theo Title...")
            all_links = soup.find_all('a', href=True)
            for a in all_links:
                link = a['href']
                title = a.get_text(strip=True)
                if not title: title = a.get('title', '')
                
                if len(title) > 10 and str(current_year) in title:
                    if link.startswith('/'): link = "https://abbank.vn" + link
                    if link in seen_ids: continue
                    
                    new_items.append({
                        "source": "ABB - Tin cổ đông",
                        "id": link,
                        "title": title,
                        "date": f"01/01/{current_year}", # Ngày giả định
                        "link": link
                    })
                    seen_ids.add(link)

    except Exception as e:
        print(f"   ! Lỗi khi quét ABB: {e}")

    return new_items

def fetch_pvd_news(seen_ids):
    print(f"--- 🚀 Bắt đầu quét PVD (Năm {current_year}) ---")
    
    base_url = "https://www.pvdrilling.com.vn"
    targets = [
        ("PVD - ĐHĐCĐ (Tin tức)", "https://www.pvdrilling.com.vn/quan-he-co-dong/dai-hoi-dong-co-dong"),
        ("PVD - ĐHĐCĐ (Tài liệu)", "https://www.pvdrilling.com.vn/quan-he-co-dong/tai-lieu-dhdcd"),
        ("PVD - Báo cáo tài chính", "https://www.pvdrilling.com.vn/quan-he-co-dong/bao-cao-tai-chinh")
    ]
    
    # Cấu hình Session mạnh
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.verify = False
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.pvdrilling.com.vn/'
    }

    new_items = []

    for source_label, url in targets:
        try:
            resp = session.get(url, headers=headers, timeout=30)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # --- CHIẾN THUẬT: TÌM NGÀY -> SUY RA LINK ---
            # Tìm tất cả node text chứa ngày dd/mm/yyyy
            date_nodes = soup.find_all(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
            
            # Biến cờ để biết trang này có tìm được tin nào không
            found_any_in_page = False

            for node in date_nodes:
                try:
                    date_str = node.strip()
                    match = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
                    if not match: continue
                    clean_date = match.group(1)

                    # Tìm Container (thẻ bao)
                    container = node.find_parent(['div', 'li', 'tr', 'article', 'td'])
                    if not container: continue

                    # Tìm Link
                    link_tag = container.find('a')
                    # Nếu không thấy, leo lên 1 cấp nữa
                    if not link_tag:
                        container = container.parent
                        if container: link_tag = container.find('a')
                    
                    if not link_tag: continue

                    link = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)
                    if not title: title = link_tag.get('title', '')

                    if not link or len(title) < 5: continue

                    # Chuẩn hóa Link
                    if link.startswith('/'):
                        link = base_url + link
                    
                    # Lọc Năm
                    if str(current_year) not in clean_date: continue

                    # Check trùng
                    if link in seen_ids: continue

                    new_items.append({
                        "source": source_label,
                        "id": link,
                        "title": title,
                        "date": clean_date,
                        "link": link
                    })
                    seen_ids.add(link)
                    found_any_in_page = True

                except Exception:
                    continue

            # --- BACKUP: NẾU KHÔNG THẤY NGÀY ---
            # Nếu trang này không tìm được tin nào theo ngày (có thể do layout khác), quét theo Title
            if not found_any_in_page:
                all_links = soup.find_all('a', href=True)
                for a in all_links:
                    link = a['href']
                    title = a.get_text(strip=True)
                    if not title: title = a.get('title', '')
                    
                    if len(title) > 10 and str(current_year) in title:
                        if link.startswith('/'): link = base_url + link
                        if link in seen_ids: continue
                        
                        new_items.append({
                            "source": source_label,
                            "id": link,
                            "title": title,
                            "date": f"01/01/{current_year}",
                            "link": link
                        })
                        seen_ids.add(link)

        except Exception as e:
            print(f"   ! Lỗi khi quét {source_label}: {e}")

    return new_items