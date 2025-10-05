import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
import requests
import os
import time
import threading
import configparser
from datetime import datetime, timedelta
import json
import re
import unicodedata
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class ExcelToDBApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel to Database Sync Tool")
        self.root.geometry("800x600")

        # Khởi tạo biến
        self.config = configparser.ConfigParser()
        self.load_config()

        self.is_running = False
        self.observer = None
        self.processed_files = {}

        # Thống kê
        self.total_files_processed = 0
        self.total_barcodes_uploaded = 0
        self.total_errors = 0

        self.setup_ui()
        
    def load_config(self):
        """Load cấu hình từ file config.ini"""
        if os.path.exists('config.ini'):
            self.config.read('config.ini')
        else:
            # Tạo config mặc định
            self.config['network'] = {
                'API_URL': 'http://192.168.100.252:86/api',
                'X_API_KEY': 'project_power_2025'
            }
            self.config['settings'] = {
                'EXCEL_FOLDER_PATH': '',
                'POLLING_INTERVAL': '5'
            }
            self.save_config()
    
    def save_config(self):
        """Lưu cấu hình vào file"""
        with open('config.ini', 'w') as configfile:
            self.config.write(configfile)
    
    def setup_ui(self):
        """Tạo giao diện người dùng"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Cấu hình
        config_frame = ttk.LabelFrame(main_frame, text="Cấu hình", padding="10")
        config_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # API URL
        ttk.Label(config_frame, text="API URL:").grid(row=0, column=0, sticky=tk.W)
        self.api_url_var = tk.StringVar(value=self.config.get('network', 'API_URL', fallback=''))
        api_url_entry = ttk.Entry(config_frame, textvariable=self.api_url_var, width=50)
        api_url_entry.grid(row=0, column=1, padx=(10, 0), sticky=(tk.W, tk.E))

        # API Key không hiển thị trên UI
        self.api_key_var = tk.StringVar(value=self.config.get('network', 'X_API_KEY', fallback=''))
        
        # Folder path
        ttk.Label(config_frame, text="Thư mục Excel:").grid(row=2, column=0, sticky=tk.W, pady=(5, 0))
        folder_frame = ttk.Frame(config_frame)
        folder_frame.grid(row=2, column=1, padx=(10, 0), sticky=(tk.W, tk.E), pady=(5, 0))
        
        self.folder_path_var = tk.StringVar(value=self.config.get('settings', 'EXCEL_FOLDER_PATH', fallback=''))
        folder_entry = ttk.Entry(folder_frame, textvariable=self.folder_path_var, width=40)
        folder_entry.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        browse_button = ttk.Button(folder_frame, text="Chọn", command=self.browse_folder)
        browse_button.grid(row=0, column=1, padx=(5, 0))
        
        # Polling interval
        ttk.Label(config_frame, text="Kiểm tra (giây):").grid(row=3, column=0, sticky=tk.W, pady=(5, 0))
        self.interval_var = tk.StringVar(value=self.config.get('settings', 'POLLING_INTERVAL', fallback='5'))
        interval_entry = ttk.Entry(config_frame, textvariable=self.interval_var, width=10)
        interval_entry.grid(row=3, column=1, padx=(10, 0), sticky=tk.W, pady=(5, 0))
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=1, column=0, columnspan=2, pady=(0, 10))
        
        self.start_button = ttk.Button(button_frame, text="Bắt đầu", command=self.start_monitoring)
        self.start_button.grid(row=0, column=0, padx=(0, 5))
        
        self.stop_button = ttk.Button(button_frame, text="Dừng", command=self.stop_monitoring, state=tk.DISABLED)
        self.stop_button.grid(row=0, column=1, padx=(5, 5))
        
        save_config_button = ttk.Button(button_frame, text="Lưu cấu hình", command=self.save_current_config)
        save_config_button.grid(row=0, column=2, padx=(5, 5))
        
        test_api_button = ttk.Button(button_frame, text="Test API", command=self.test_api_connection)
        test_api_button.grid(row=0, column=3, padx=(5, 0))
        
        # Status
        status_frame = ttk.LabelFrame(main_frame, text="Trạng thái", padding="10")
        status_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        self.status_var = tk.StringVar(value="Chờ bắt đầu...")
        status_label = ttk.Label(status_frame, textvariable=self.status_var)
        status_label.grid(row=0, column=0, sticky=tk.W)
        
        # Thống kê
        stats_frame = ttk.Frame(status_frame)
        stats_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(5, 0))
        
        self.stats_var = tk.StringVar(value="File: 0 | Barcode: 0 | Lỗi: 0")
        stats_label = ttk.Label(stats_frame, textvariable=self.stats_var, font=('TkDefaultFont', 8))
        stats_label.grid(row=0, column=0, sticky=tk.W)
        
        # Log
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="10")
        log_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, width=80)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(3, weight=1)
        config_frame.columnconfigure(1, weight=1)
        folder_frame.columnconfigure(0, weight=1)
        status_frame.columnconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
    
    def browse_folder(self):
        """Chọn thư mục chứa file Excel"""
        folder = filedialog.askdirectory()
        if folder:
            self.folder_path_var.set(folder)
    
    def test_api_connection(self):
        """Test kết nối API"""
        def test_in_thread():
            try:
                api_url = self.api_url_var.get().rstrip('/')
                api_key = self.api_key_var.get()
                
                headers = {
                    'X-API-KEY': api_key,
                    'Content-Type': 'application/json'
                }
                
                self.log_message("🔍 Đang test kết nối API...")
                self.log_message(f"URL: {api_url}")
                
                # Test 1: Kết nối cơ bản
                try:
                    response = requests.get(f"{api_url}/health", headers=headers, timeout=5)
                    self.log_message(f"Health check: {response.status_code}")
                except:
                    self.log_message("⚠️ Endpoint /health không có, thử endpoints khác...")
                
                # Test 2: Kiểm tra endpoint models
                try:
                    response = requests.get(f"{api_url}/check_scan_models", headers=headers, timeout=5)
                    self.log_message(f"GET /check_scan_models: {response.status_code}")
                    if response.status_code != 200:
                        self.log_message(f"Response: {response.text[:200]}")
                except Exception as e:
                    self.log_message(f"❌ Lỗi test models: {str(e)}")
                
                # Test 3: Kiểm tra endpoint tmp
                test_data = {
                    'barcode': 'TEST123',
                    'result': 'TEST',
                    'model': 'TEST',
                    'file_name': 'test.xlsx',
                    'datetime': '2025-10-04 12:00:00'
                }
                
                try:
                    response = requests.post(f"{api_url}/check_scans_tmp", headers=headers, json=test_data, timeout=5)
                    self.log_message(f"POST /check_scans_tmp: {response.status_code}")
                    if response.status_code not in [200, 201]:
                        self.log_message(f"Response: {response.text[:200]}")
                    else:
                        self.log_message("✅ API connection OK!")
                except Exception as e:
                    self.log_message(f"❌ Lỗi test upload: {str(e)}")
                
                # Test 4: Kiểm tra các endpoint phổ biến khác
                common_endpoints = [
                    "/api/check_scans_tmp",
                    "/check-scans-tmp", 
                    "/scans/tmp",
                    "/upload/barcode"
                ]
                
                self.log_message("🔍 Kiểm tra các endpoint khác:")
                for endpoint in common_endpoints:
                    try:
                        test_url = api_url.replace('/api', '') + endpoint
                        response = requests.post(test_url, headers=headers, json=test_data, timeout=3)
                        self.log_message(f"  {endpoint}: {response.status_code}")
                    except:
                        pass
                        
            except Exception as e:
                self.log_message(f"❌ Lỗi test API: {str(e)}")
        
        # Chạy test trong thread riêng
        thread = threading.Thread(target=test_in_thread, daemon=True)
        thread.start()
    
    def save_current_config(self):
        """Lưu cấu hình hiện tại"""
        self.config['network']['API_URL'] = self.api_url_var.get()
        self.config['network']['X_API_KEY'] = self.api_key_var.get()
        self.config['settings']['EXCEL_FOLDER_PATH'] = self.folder_path_var.get()
        self.config['settings']['POLLING_INTERVAL'] = self.interval_var.get()
        
        self.save_config()
        self.log_message("Đã lưu cấu hình")
    
    def log_message(self, message):
        """Ghi log vào text widget"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        # Update UI in main thread
        self.root.after(0, lambda: self._append_log(log_entry))
    
    def _append_log(self, log_entry):
        """Thêm log vào text widget (chạy trong main thread)"""
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
    
    def update_stats(self):
        """Cập nhật thống kê"""
        stats_text = f"File: {self.total_files_processed} | Barcode: {self.total_barcodes_uploaded} | Lỗi: {self.total_errors}"
        self.root.after(0, lambda: self.stats_var.set(stats_text))
    
    def update_status(self, status):
        """Cập nhật trạng thái"""
        self.root.after(0, lambda: self.status_var.set(status))
    
    def start_monitoring(self):
        """Bắt đầu giám sát thư mục"""
        if not self.folder_path_var.get():
            messagebox.showerror("Lỗi", "Vui lòng chọn thư mục chứa file Excel")
            return
        
        if not os.path.exists(self.folder_path_var.get()):
            messagebox.showerror("Lỗi", "Thư mục không tồn tại")
            return
        
        self.save_current_config()
        
        self.is_running = True
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        
        # Bắt đầu monitoring thread
        self.monitoring_thread = threading.Thread(target=self.monitor_folder, daemon=True)
        self.monitoring_thread.start()
        
        self.update_status("Đang giám sát...")
        self.log_message("Bắt đầu giám sát thư mục: " + self.folder_path_var.get())
    
    def stop_monitoring(self):
        """Dừng giám sát"""
        self.is_running = False
        
        if self.observer:
            self.observer.stop()
            self.observer.join()
        
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        
        self.update_status("Đã dừng")
        self.log_message("Đã dừng giám sát")
    
    def monitor_folder(self):
        """Giám sát thư mục và xử lý file Excel"""
        folder_path = self.folder_path_var.get()
        interval = int(self.interval_var.get())
        
        # Xử lý file có sẵn
        self.process_existing_files()
        
        # Thiết lập file watcher
        event_handler = ExcelFileHandler(self)
        self.observer = Observer()
        self.observer.schedule(event_handler, folder_path, recursive=False)
        self.observer.start()
        
        # Polling để kiểm tra file thay đổi
        while self.is_running:
            try:
                time.sleep(interval)
                if self.is_running:
                    self.check_for_modified_files()
            except Exception as e:
                self.log_message(f"Lỗi monitoring: {str(e)}")
    
    def process_existing_files(self):
        """Xử lý các file Excel có sẵn trong thư mục, chỉ lấy file trong 10 ngày gần nhất"""
        folder_path = self.folder_path_var.get()
        current_time = time.time()
        ten_days_ago = current_time - (10 * 24 * 60 * 60)  # 10 ngày trước
        
        # Lọc file Excel trong 10 ngày gần nhất
        excel_files = []
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                try:
                    file_mod_time = os.path.getmtime(file_path)
                    if file_mod_time >= ten_days_ago:
                        excel_files.append(filename)
                except:
                    # Nếu không đọc được thời gian file, vẫn thêm vào danh sách
                    excel_files.append(filename)
        
        total_files = len(excel_files)
        if total_files == 0:
            self.log_message("📂 Không có file Excel nào trong 10 ngày gần nhất")
            return
            
        self.log_message(f"📂 Tìm thấy {total_files} file Excel trong 10 ngày gần nhất")
        
        for i, filename in enumerate(excel_files, 1):
            file_path = os.path.join(folder_path, filename)
            
            # Kiểm tra file có đang được mở hay không
            try:
                with open(file_path, 'rb') as f:
                    pass
                self.process_excel_file(file_path)
                # Giảm delay xuống 2 giây để upload nhanh hơn
                time.sleep(2)
            except PermissionError:
                self.log_message(f"⚠️ {filename} - đang được mở, bỏ qua")
                self.total_errors += 1
            except Exception as e:
                self.log_message(f"❌ {filename} - Lỗi: {str(e)}")
                self.total_errors += 1
                # Bỏ qua file lỗi và tiếp tục
                continue
            
            self.update_stats()
    
    def check_for_modified_files(self):
        """Kiểm tra file đã thay đổi"""
        folder_path = self.folder_path_var.get()
        
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                
                # Kiểm tra thời gian modify
                mod_time = os.path.getmtime(file_path)
                current_time = time.time()
                
                # Nếu file được modify trong 1 phút qua
                if current_time - mod_time < 60:
                    if file_path not in self.processed_files or mod_time > self.processed_files.get(file_path, 0):
                        self.process_excel_file(file_path)
                        self.processed_files[file_path] = mod_time
    
    def extract_model_from_filename(self, filename):
        """Trích xuất model từ tên file: loại bỏ phần ngày tháng ở cuối, ví dụ TPRF 40W 36V 5PCB2025-12-03.xlsx => TPRF 40W 36V 5PCB"""
        import re
        base_name = os.path.splitext(filename)[0]
        # Loại bỏ phần ngày tháng ở cuối (yyyy-mm-dd hoặc yyyy-mm-d)
        model_match = re.match(r'(.+?)(\d{4}-\d{2}-\d{2}|\d{4}-\d{2}-\d{1})$', base_name)
        if model_match:
            model_part = model_match.group(1)
        else:
            model_part = base_name
        # Nếu có prefix IPS- thì loại bỏ
        if model_part.startswith('IPS-'):
            model_part = model_part[4:]
        return model_part.strip()
    
    def get_model_from_database(self, extracted_model):
        """So sánh toàn chuỗi tên file với code, luôn trả về code (tên model) khi khớp"""
        import difflib
        try:
            api_url = self.api_url_var.get().rstrip('/')
            api_key = self.api_key_var.get()
            headers = {
                'X-API-Key': api_key,
                'Content-Type': 'application/json'
            }
            # Gọi API lấy danh sách models
            response = requests.get(f"{api_url}/check-scan/models", headers=headers, timeout=10)
            if response.status_code == 200:
                models = response.json()
                if isinstance(models, dict) and 'data' in models and 'models' in models['data']:
                    models = models['data']['models']
                extracted_model_norm = extracted_model.strip().lower().replace(' ', '').replace('_','').replace('-','')
                # Tạo danh sách tất cả code đã chuẩn hóa
                model_map = []
                for model in models:
                    code = str(model.get('code', '')).strip().lower().replace(' ', '').replace('_','').replace('-','')
                    if code:
                        model_map.append((model.get('code'), code))
                # Tìm best match bằng difflib
                if model_map:
                    best = difflib.get_close_matches(extracted_model_norm, [m[1] for m in model_map], n=1, cutoff=0.6)
                    if best:
                        for name, norm in model_map:
                            if norm == best[0]:
                                return name
                return None
            else:
                return None
        except Exception as e:
            return None
    
    def is_valid_barcode(self, cell_value):
        """Kiểm tra xem giá trị có phải barcode hợp lệ không"""
        if not cell_value or str(cell_value).strip() == '':
            return False
            
        cell_str = str(cell_value).strip()
        
        # Debug: log giá trị đang kiểm tra
        # self.log_message(f"🔍 Kiểm tra: '{cell_str}'")
        
        # Bỏ qua nếu là NaN hoặc None
        if cell_str.lower() in ['nan', 'none', 'null', '']:
            return False
        
        # Bỏ qua nếu là ngày tháng với format khác nhau
        if ('/' in cell_str or '-' in cell_str or ':' in cell_str) and any(c.isdigit() for c in cell_str):
            import re
            # Kiểm tra nhiều format ngày tháng
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',           # 2025-10-04
                r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}',  # 2025-10-04 02:13:59
                r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',         # 10/04/2025
                r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',           # 2025/10/04
                r'\d{2}:\d{2}:\d{2}',                     # 02:13:59
                r'\d{1,2}:\d{1,2}'                        # 2:13
            ]
            for pattern in date_patterns:
                if re.search(pattern, cell_str):
                    return False
        
        # Bỏ qua các từ khóa sai - mở rộng danh sách
        invalid_keywords = [
            'no', 'ng', 'yes', 'ok', 'on', 'off', 'error', 'test', 'sample', 
            'header', 'title', 'barcode', 'sheet', 'pass', 'fail', 'null',
            '二维码', '测试时间', '测试结果'  # Thêm từ khóa tiếng Trung
        ]
        if any(keyword == cell_str.lower() for keyword in invalid_keywords):
            return False
        
        # Bỏ qua nếu chứa từ khóa tiếng Trung
        chinese_keywords = ['二维码', '测试', '结果', '时间']
        if any(keyword in cell_str for keyword in chinese_keywords):
            return False
        
        # Bỏ qua nếu chỉ toàn số hoặc toàn chữ
        if cell_str.isdigit() or cell_str.isalpha():
            return False
        
        # Bỏ qua nếu là số thập phân đơn thuần (như 33.60, 220.0, 29.69)
        try:
            float_val = float(cell_str)
            if '.' in cell_str and len(cell_str.split('.')) == 2:
                # Nếu là số thập phân có ít hơn 6 ký tự thì bỏ qua
                if len(cell_str.replace('.', '')) < 6:
                    return False
        except:
            pass
        
        # Bỏ qua nếu quá ngắn
        if len(cell_str) < 8:
            return False
        
        # Bỏ qua nếu có quá nhiều ký tự đặc biệt
        special_chars = sum(1 for c in cell_str if not c.isalnum())
        if special_chars > len(cell_str) * 0.3:
            return False
        
        # Barcode hợp lệ phải có cả số và chữ, và đủ dài
        has_digit = any(c.isdigit() for c in cell_str)
        has_alpha = any(c.isalpha() for c in cell_str)
        
        return (has_digit and has_alpha and len(cell_str) >= 8)

    def normalize_column_name(self, column_name):
        """Chuẩn hóa tên cột để dễ so khớp"""
        if column_name is None:
            return ''
        column_str = self.remove_diacritics(str(column_name).strip().lower())
        return re.sub(r'[^a-z0-9]', '', column_str)

    def remove_diacritics(self, text):
        """Loại bỏ dấu tiếng Việt hoặc ký tự có dấu"""
        if not isinstance(text, str):
            return text
        normalized = unicodedata.normalize('NFD', text)
        without_diacritics = ''.join(ch for ch in normalized if unicodedata.category(ch) != 'Mn')
        return unicodedata.normalize('NFC', without_diacritics)

    def normalize_result_value(self, value):
        """Chuẩn hóa giá trị kết quả (PASS/FAIL) nếu có thể"""
        if value is None:
            return None

        value_str = str(value).strip()
        if value_str == '':
            return None

        normalized = self.remove_diacritics(value_str).lower().strip()
        normalized = normalized.replace(' ', '').replace('_', '')

        positive_keywords = {
            'ok', 'pass', 'passed', 'good', 'success', 'qualified', '合格',
            '良品', 'dat', 'đạt', 'đạt', 'completed', 'done', 'passok', 'okpass',
            'pass1', 'pass2', 'pass3', 'pass4'
        }
        negative_keywords = {
            'ng', 'ngok', 'fail', 'failed', 'error', 'ng1', 'ng2', 'ng3', 'ng4',
            'notgood', 'reject', 'nok', 'nok1', 'nok2', '不良', '不合格',
            'failng', 'fail1', 'fail2', 'fail3'
        }

        if normalized in positive_keywords or normalized.endswith('pass') or normalized.endswith('ok'):
            return 'PASS'
        if normalized in negative_keywords or normalized.endswith('ng') or normalized.endswith('fail'):
            return 'FAIL'

        return value_str

    def parse_datetime_value(self, value):
        """Cố gắng parse giá trị ngày giờ từ nhiều định dạng khác nhau"""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, (int, float)):
            try:
                # Excel serial number
                base_date = datetime(1899, 12, 30)
                return base_date + timedelta(days=float(value))
            except Exception:
                pass

        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None

        try:
            parsed = pd.to_datetime(value, errors='coerce')
            if pd.isna(parsed):
                parsed = pd.to_datetime(value, errors='coerce', dayfirst=True)
            if pd.isna(parsed):
                return None
            if isinstance(parsed, pd.Timestamp):
                if parsed.tzinfo is not None:
                    parsed = parsed.tz_convert(None)
                return parsed.to_pydatetime()
            return parsed
        except Exception:
            return None

    def extract_datetime_from_filename(self, filename):
        """Cố gắng lấy ngày giờ từ tên file nếu có"""
        base_name = os.path.splitext(filename)[0]
        base_name = base_name.replace(' ', '_')

        # Pattern: YYYYMMDDHHMMSS hoặc YYYY-MM-DD_HHMMSS
        match = re.search(r'(20\d{2}|19\d{2})[-_]?(\d{2})[-_]?(\d{2})[T\s_-]?(\d{2})(\d{2})(\d{2})', base_name)
        if match:
            year, month, day, hour, minute, second = map(int, match.groups())
            try:
                return datetime(year, month, day, hour, minute, second)
            except ValueError:
                pass

        # Pattern: YYYYMMDD hoặc YYYY-MM-DD, không có giờ
        match = re.search(r'(20\d{2}|19\d{2})[-_]?(\d{2})[-_]?(\d{2})', base_name)
        if match:
            year, month, day = map(int, match.groups())
            try:
                return datetime(year, month, day, 0, 0, 0)
            except ValueError:
                pass

        return None

    def _find_barcode_in_row(self, row, barcode_columns):
        """Tìm barcode hợp lệ trong một dòng dữ liệu"""
        for col in barcode_columns:
            if col in row.index and self.is_valid_barcode(row[col]):
                return str(row[col]).strip()

        for value in row:
            if self.is_valid_barcode(value):
                return str(value).strip()
        return None

    def _find_result_in_row(self, row, result_columns):
        """Tìm kết quả PASS/FAIL trong một dòng"""
        for col in result_columns:
            if col in row.index:
                normalized = self.normalize_result_value(row[col])
                if normalized:
                    return normalized

        for value in row:
            normalized = self.normalize_result_value(value)
            if normalized:
                return normalized
        return None

    def _find_datetime_in_row(self, row, datetime_columns, date_columns=None, time_columns=None):
        """Tìm thời gian scan trong một dòng"""
        for col in datetime_columns:
            if col in row.index:
                parsed = self.parse_datetime_value(row[col])
                if parsed:
                    return parsed

        for value in row:
            parsed = self.parse_datetime_value(value)
            if parsed:
                return parsed

        date_component = None
        time_component = None

        if date_columns:
            for col in date_columns:
                if col in row.index:
                    parsed = self.parse_datetime_value(row[col])
                    if parsed:
                        date_component = parsed
                        break

        if time_columns:
            for col in time_columns:
                if col in row.index:
                    parsed = self.parse_datetime_value(row[col])
                    if parsed:
                        time_component = parsed
                        break

        if date_component and time_component:
            try:
                return datetime.combine(date_component.date(), time_component.time())
            except Exception:
                pass

        if date_component:
            return date_component

        if time_component:
            try:
                today = datetime.now().date()
                return datetime.combine(today, time_component.time())
            except Exception:
                pass

        return None

    def extract_standard_rows(self, df, sheet_name, model, filename):
        """Trích xuất dữ liệu chuẩn hóa từ DataFrame"""
        normalized_headers = {
            col: self.normalize_column_name(col) for col in df.columns
        }

        barcode_keywords = ['barcode', 'qrcode', 'serial', 'sn', 'macode', 'mavach', 'code', 'sncode', 'imei']
        result_keywords = ['result', 'ketqua', '判定', 'status', 'state', 'kq', '判断', 'kiemtra', 'testresult', 'judgement', 'judgment', 'phanloai']
        datetime_keywords = [
            'time', 'datetime', 'timestamp', 'thoigiantest', 'thoigiankiemtra',
            'thoigianquet', 'thoigiancheck', 'testtime', 'starttime', 'endtime',
            'finishtime', 'scantime', 'scan', '检测时间', '測試時間', '时间'
        ]
        date_keywords = [
            'date', 'ngay', 'ngaygio', 'recorddate', 'docudate', 'calibrationdate',
            'datime', '日期', '日付', 'day'
        ]
        time_keywords = [
            'time', 'gio', 'giophut', 'giay', 'scantime', 'thoigian', 'testtime',
            'timing', 'recordtime', '検査時刻', '時刻', 'hour', 'minute', 'second'
        ]

        barcode_columns = [col for col, norm in normalized_headers.items()
                           if any(keyword in norm for keyword in barcode_keywords)]
        result_columns = [col for col, norm in normalized_headers.items()
                          if any(keyword in norm for keyword in result_keywords)]
        datetime_columns = [col for col, norm in normalized_headers.items()
                            if any(keyword in norm for keyword in datetime_keywords)]
        date_columns = [col for col, norm in normalized_headers.items()
                        if any(keyword in norm for keyword in date_keywords)]
        time_columns = [col for col, norm in normalized_headers.items()
                        if any(keyword in norm for keyword in time_keywords)]

        if not datetime_columns and not (date_columns and time_columns):
            self.log_message(
                f"⚠️ Sheet '{sheet_name}' không tìm thấy cột thời gian. Tiêu đề: {', '.join(map(str, df.columns.tolist()))}"
            )

        cleaned_rows = []
        missing_datetime_logs = 0
        fallback_logged = False

        for idx, row in df.iterrows():
            barcode_value = self._find_barcode_in_row(row, barcode_columns)
            if not barcode_value:
                continue

            result_value = self._find_result_in_row(row, result_columns)
            scan_time = self._find_datetime_in_row(row, datetime_columns, date_columns, time_columns)

            # Nếu không tìm thấy thời gian, thử lấy từ tên file
            if scan_time is None:
                fallback_time = self.extract_datetime_from_filename(filename)
                if fallback_time:
                    scan_time = fallback_time
                    if not fallback_logged:
                        self.log_message(
                            f"ℹ️ Sheet '{sheet_name}': sử dụng thời gian từ tên file {fallback_time.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        fallback_logged = True

            if scan_time is None and missing_datetime_logs < 3:
                self.log_message(
                    f"⚠️ Sheet '{sheet_name}', dòng {idx}: không tìm thấy thời gian. Giá trị mẫu: {', '.join(str(row[col]) for col in df.columns[:5])}"
                )
                missing_datetime_logs += 1

            if isinstance(idx, (int, float)):
                row_number = int(idx) + 2  # +2 vì pandas index bắt đầu tử 0 và dòng tiêu đề
            else:
                row_number = idx

            cleaned_rows.append({
                'barcode': barcode_value,
                'result': result_value or '',
                'model': model or '',
                'file_name': filename,
                'sheet_name': sheet_name,
                'row_number': row_number,
                'datetime': scan_time.strftime('%Y-%m-%d %H:%M:%S') if scan_time else ''
            })

        return cleaned_rows

    def create_clean_excel_file(self, file_path, model=None):
        """Tạo file Excel chuẩn chỉ chứa các dòng hợp lệ"""
        import tempfile
        import os

        filename = os.path.basename(file_path)
        temp_file_path = None

        try:
            excel_file = pd.ExcelFile(file_path)

            fd, temp_file_path = tempfile.mkstemp(prefix='filtered_', suffix='.xlsx')
            os.close(fd)

            total_rows = 0
            valid_rows = 0

            with pd.ExcelWriter(temp_file_path, engine='openpyxl') as writer:
                for sheet_name in excel_file.sheet_names:
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, engine='openpyxl')
                        df_clean = df.dropna(how='all')

                        if len(df_clean) == 0:
                            continue

                        total_rows += len(df_clean)
                        standardized_rows = self.extract_standard_rows(df_clean, sheet_name, model, filename)

                        if standardized_rows:
                            clean_df = pd.DataFrame(standardized_rows)
                            clean_df.drop_duplicates(subset=['barcode', 'sheet_name', 'row_number'], inplace=True)
                            ordered_columns = [
                                'barcode', 'result', 'model', 'file_name',
                                'sheet_name', 'row_number', 'datetime'
                            ]
                            for col in ordered_columns:
                                if col not in clean_df.columns:
                                    clean_df[col] = ''
                            clean_df = clean_df[ordered_columns]
                            clean_df.to_excel(writer, sheet_name=sheet_name, index=False)
                            valid_rows += len(clean_df)
                    except Exception as sheet_error:
                        self.log_message(f"❌ Lỗi đọc sheet '{sheet_name}': {str(sheet_error)}")
                        continue

            if valid_rows == 0:
                self.log_message(f"⚠️ {filename} không có dòng barcode hợp lệ nào")
                if temp_file_path and os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                return None, 0, total_rows

            self.log_message(
                f"📊 {filename}: {valid_rows}/{total_rows} dòng dữ liệu chuẩn sẽ được upload"
            )
            self.log_message(f"📁 File chuẩn tạm: {temp_file_path}")
            return temp_file_path, valid_rows, total_rows

        except Exception as e:
            self.log_message(f"❌ Lỗi tạo file sạch cho {filename}: {str(e)}")
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return None, 0, 0

    def process_excel_file(self, file_path):
        """Upload file Excel đã được lọc sạch lên API"""
        filename = os.path.basename(file_path)
        temp_file = None
        
        try:
            # Lấy model từ tên file
            extracted_model = self.extract_model_from_filename(filename)
            model = self.get_model_from_database(extracted_model)
            if model is None or str(model).strip() == '':
                model = extracted_model if extracted_model else 'NULL'

            self.log_message(f"🧾 Model sử dụng: {model}")

            # Tạo file Excel sạch
            temp_file, valid_rows, total_rows = self.create_clean_excel_file(file_path, model)

            if temp_file is None or valid_rows == 0:
                self.total_errors += 1
                return False
            
            api_url = self.api_url_var.get().rstrip('/')
            api_key = self.api_key_var.get()
            headers = {'X-API-Key': api_key}
            
            # Upload file sạch với retry - QUAN TRỌNG: dùng temp_file (file đã lọc)
            max_retries = 3
            retry_delay = 3
            
            for attempt in range(max_retries):
                try:
                    # ĐÂY LÀ ĐIỂM QUAN TRỌNG: Upload temp_file (file sạch) nhưng với tên file gốc
                    with open(temp_file, 'rb') as file_handle:
                        files = {
                            'scans[0][file]': (filename, file_handle, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                        }
                        data = {'scans[0][model]': model}
                        
                        self.log_message(f"📤 Uploading file sạch với {valid_rows} dòng hợp lệ...")
                        response = requests.post(f"{api_url}/check-scan/upload-file", headers=headers, files=files, data=data, timeout=60)
                    
                    if response.status_code in [200, 201]:
                        self.log_message(f"✅ {filename} ({model}) - Upload {valid_rows} dòng thành công")
                        self.total_files_processed += 1
                        self.total_barcodes_uploaded += valid_rows
                        self.update_stats()
                        return True
                        
                    elif response.status_code == 500:
                        if attempt < max_retries - 1:
                            self.log_message(f"⏳ Server lỗi 500, thử lại lần {attempt + 2}...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            self.log_message(f"❌ {filename} ({model}) - Server lỗi (500) sau {max_retries} lần thử")
                            self.total_errors += 1
                            return False
                            
                    elif response.status_code == 422:
                        self.log_message(f"❌ {filename} ({model}) - Lỗi format file (422)")
                        self.total_errors += 1
                        return False
                        
                    else:
                        self.log_message(f"❌ {filename} ({model}) - Lỗi upload ({response.status_code})")
                        self.total_errors += 1
                        return False
                        
                except requests.exceptions.Timeout:
                    if attempt < max_retries - 1:
                        self.log_message(f"⏳ {filename} - Timeout, thử lại lần {attempt + 2}...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.log_message(f"❌ {filename} ({model}) - Timeout sau {max_retries} lần thử")
                        self.total_errors += 1
                        return False
                        
                except requests.exceptions.ConnectionError:
                    if attempt < max_retries - 1:
                        self.log_message(f"🔄 {filename} - Mất kết nối, thử lại lần {attempt + 2}...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.log_message(f"❌ {filename} ({model}) - Lỗi kết nối")
                        self.total_errors += 1
                        return False
                        
        except Exception as e:
            self.log_message(f"❌ {filename} - Lỗi xử lý: {str(e)}")
            self.total_errors += 1
            return False
            
        finally:
            # Xóa file tạm thời
            if temp_file and os.path.exists(temp_file):
                try:
                    os.unlink(temp_file)
                    self.log_message(f"🗑️ Đã xóa file tạm thời")
                except:
                    pass
    
    def process_tmp_to_main_table(self):
        """Xử lý dữ liệu từ bảng tmp sang bảng chính"""
        try:
            api_url = self.api_url_var.get()
            api_key = self.api_key_var.get()
            headers = {
                'X-API-Key': api_key
            }
            # Gọi API để xử lý dữ liệu
            response = requests.post(
                f"{api_url}/process_tmp_to_main",
                headers=headers,
                timeout=30
            )
            if response.status_code in [200, 201]:
                self.log_message("✅ Đã xử lý dữ liệu từ tmp sang bảng chính thành công")
                try:
                    result = response.json()
                    if 'processed_count' in result:
                        self.log_message(f"📊 Số bản ghi đã xử lý: {result['processed_count']}")
                    if 'duplicates_removed' in result:
                        self.log_message(f"🗑️ Số bản ghi trùng lặp đã loại bỏ: {result['duplicates_removed']}")
                except:
                    pass
            else:
                self.log_message(f"❌ Lỗi xử lý tmp->main: {response.status_code}")
        except Exception as e:
            self.log_message(f"❌ Lỗi xử lý tmp->main: {str(e)}")


class ExcelFileHandler(FileSystemEventHandler):
    """Handler cho file system events"""
    
    def __init__(self, app):
        self.app = app
    
    def on_modified(self, event):
        """Xử lý khi file được modify"""
        if not event.is_directory and (event.src_path.endswith('.xlsx') or event.src_path.endswith('.xls')):
            # Đợi một chút để file được ghi hoàn toàn
            time.sleep(2)
            self.app.process_excel_file(event.src_path)
    
    def on_created(self, event):
        """Xử lý khi file mới được tạo"""
        if not event.is_directory and (event.src_path.endswith('.xlsx') or event.src_path.endswith('.xls')):
            # Đợi một chút để file được ghi hoàn toàn
            time.sleep(2)
            self.app.process_excel_file(event.src_path)


def main():
    root = tk.Tk()
    import os
    icon_path = os.path.join(os.path.dirname(__file__), 'readexcel.ico')
    if os.path.exists(icon_path):
        try:
            root.iconbitmap(icon_path)
        except Exception:
            pass
    app = ExcelToDBApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()