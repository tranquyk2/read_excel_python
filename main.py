import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
import requests
import os
import time
import threading
import configparser
from datetime import datetime
import json
import re
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
                # Tăng delay lên 5 giây giữa các file để tránh nghẽn server
                time.sleep(5)
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
    
    def process_excel_file(self, file_path):
        """Chỉ upload file Excel lên API, lấy model đối chiếu tên file"""
        filename = os.path.basename(file_path)
        try:
            # Kiểm tra file có thể đọc được không (không hiển thị log chi tiết)
            try:
                excel_file = pd.ExcelFile(file_path)
                sheet_names = excel_file.sheet_names
                
                total_rows = 0
                valid_sheets = []
                
                for sheet_name in sheet_names:
                    try:
                        # Đọc với error handling cho từng sheet
                        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                        
                        # Loại bỏ dòng trống hoàn toàn
                        df_clean = df.dropna(how='all')
                        
                        # Loại bỏ dòng có lỗi format (dòng có quá nhiều cột trống hoặc merge cells)
                        if len(df_clean.columns) > 0:
                            # Loại bỏ dòng có tất cả các cột quan trọng đều NaN hoặc lỗi
                            df_valid = df_clean.dropna(thresh=3)  # Giữ dòng có ít nhất 3 cột có dữ liệu
                            
                            sheet_rows = len(df_valid)
                            if sheet_rows > 0:
                                total_rows += sheet_rows
                                valid_sheets.append(sheet_name)
                                
                    except Exception as sheet_error:
                        # Nếu sheet bị lỗi hoàn toàn, thử đọc với skiprows
                        try:
                            df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=1, engine='openpyxl')
                            df_clean = df.dropna(how='all')
                            df_valid = df_clean.dropna(thresh=2)
                            sheet_rows = len(df_valid)
                            if sheet_rows > 0:
                                total_rows += sheet_rows
                                valid_sheets.append(sheet_name)
                        except:
                            # Sheet hoàn toàn không đọc được, bỏ qua
                            continue
                
                if total_rows < 2:
                    self.log_message(f"⚠️ File {filename} có quá ít dữ liệu hợp lệ ({total_rows} dòng), bỏ qua")
                    return False
                    
            except Exception as read_error:
                self.log_message(f"❌ Không đọc được file {filename}")
                self.total_errors += 1
                return False
            
            # Lấy model từ tên file đối chiếu bảng model
            extracted_model = self.extract_model_from_filename(filename)
            model = self.get_model_from_database(extracted_model)
            if model is None or str(model).strip() == '':
                model = 'NULL'
            
            api_url = self.api_url_var.get().rstrip('/')
            api_key = self.api_key_var.get()
            headers = {
                'X-API-Key': api_key
            }
            
            # Upload file với retry
            try:
                max_retries = 3
                retry_delay = 5
                
                for attempt in range(max_retries):
                    try:
                        with open(file_path, 'rb') as file_handle:
                            files = {
                                'scans[0][file]': (filename, file_handle, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                            }
                            data = {
                                'scans[0][model]': model
                            }
                            
                            response = requests.post(f"{api_url}/check-scan/upload-file", headers=headers, files=files, data=data, timeout=60)
                        
                        if response.status_code in [200, 201]:
                            self.log_message(f"✅ {filename} ({model}) - Upload thành công")
                            self.total_files_processed += 1
                            break
                            
                        elif response.status_code == 500:
                            if attempt < max_retries - 1:
                                time.sleep(retry_delay)
                                continue
                            else:
                                self.log_message(f"❌ {filename} ({model}) - Server lỗi (500)")
                                self.total_errors += 1
                                break
                                
                        elif response.status_code == 422:
                            self.log_message(f"❌ {filename} ({model}) - Lỗi format file (422)")
                            self.total_errors += 1
                            break
                            
                        else:
                            self.log_message(f"❌ {filename} ({model}) - Lỗi upload ({response.status_code})")
                            self.total_errors += 1
                            break
                            
                    except requests.exceptions.Timeout:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        else:
                            self.log_message(f"❌ {filename} ({model}) - Timeout")
                            self.total_errors += 1
                            break
                            
                    except requests.exceptions.ConnectionError:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        else:
                            self.log_message(f"❌ {filename} ({model}) - Lỗi kết nối")
                            self.total_errors += 1
                            break
                            
            except Exception as e:
                self.log_message(f"❌ {filename} ({model}) - Lỗi: {str(e)}")
                self.total_errors += 1
                
        except Exception as e:
            self.log_message(f"❌ {filename} - Lỗi xử lý: {str(e)}")
            self.total_errors += 1
        return False
    
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