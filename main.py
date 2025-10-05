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
        """Xử lý các file Excel có sẵn trong thư mục"""
        folder_path = self.folder_path_var.get()
        
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                self.process_excel_file(file_path)
    
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
        """Trích xuất model từ tên file: lấy phần giữa prefix và ngày tháng, ví dụ IPS-EP150WN-24V-CV_2025-09-25.xlsx => EP150WN-24V-CV"""
        base_name = os.path.splitext(filename)[0]
        # Tìm phần model giữa prefix và ngày tháng
        # Tách theo dấu gạch dưới, lấy phần đầu tiên (trước ngày tháng)
        parts = base_name.split('_')
        if len(parts) > 1:
            model_part = parts[0]
        else:
            model_part = base_name
        # Nếu có prefix IPS- thì loại bỏ
        if model_part.startswith('IPS-'):
            model_part = model_part[4:]
        return model_part.strip()
    
    def get_model_from_database(self, extracted_model):
        """So sánh toàn chuỗi tên file với barcode/code, lấy model giống nhất (best match)"""
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
                # Tạo danh sách tất cả barcode/code đã chuẩn hóa
                model_map = []
                for model in models:
                    barcode = str(model.get('barcode', '')).strip().lower().replace(' ', '').replace('_','').replace('-','')
                    code = str(model.get('code', '')).strip().lower().replace(' ', '').replace('_','').replace('-','')
                    if barcode:
                        model_map.append((model.get('barcode'), barcode))
                    if code:
                        model_map.append((model.get('barcode') or model.get('code'), code))
                # Tìm best match bằng difflib
                if model_map:
                    best = difflib.get_close_matches(extracted_model_norm, [m[1] for m in model_map], n=1, cutoff=0.6)
                    if best:
                        for name, norm in model_map:
                            if norm == best[0]:
                                return name
                return None
            else:
                self.log_message(f"Không thể lấy danh sách models: {response.status_code}")
                return None
        except Exception as e:
            self.log_message(f"Lỗi khi lấy model từ database: {str(e)}")
            return None
    
    def process_excel_file(self, file_path):
        """Chỉ upload file Excel lên API, lấy model đối chiếu tên file và đếm số barcode"""
        try:
            filename = os.path.basename(file_path)
            self.log_message(f"📁 Đang upload file: {filename}")
            # Đếm số barcode trong file Excel
            barcode_count = 0
            try:
                df = pd.read_excel(file_path)
                # Tìm cột barcode (ưu tiên tên 'barcode', nếu không lấy cột đầu tiên)
                barcode_col = None
                for col in df.columns:
                    if str(col).strip().lower() == 'barcode':
                        barcode_col = col
                        break
                if barcode_col is None:
                    barcode_col = df.columns[0]
                barcode_count = df[barcode_col].dropna().shape[0]
                # Kiểm tra cột 测试结果 (Test Result) nếu có
                test_result_col = None
                for col in df.columns:
                    if str(col).strip() in ['测试结果', 'Test Result']:
                        test_result_col = col
                        break
                if test_result_col:
                    pass_count = df[df[test_result_col].isin(['PASS'])].shape[0]
                    fail_count = df[df[test_result_col].isin(['FAIL', 'NG'])].shape[0]
                    self.log_message(f"📊 Số PASS: {pass_count}, Số FAIL/NG: {fail_count}")
            except Exception as e:
                self.log_message(f"⚠️ Không đọc được số barcode: {str(e)}")
            # Lấy model từ tên file đối chiếu bảng model
            extracted_model = self.extract_model_from_filename(filename)
            model = self.get_model_from_database(extracted_model)
            if model is None or str(model).strip() == '':
                model = 'NULL'
                self.log_message(f"⚠️ Không tìm thấy model khớp với tên file: {extracted_model}. Ghi vào model NULL!")
            else:
                self.log_message(f"🔑 Model khớp: {model}")
            api_url = self.api_url_var.get().rstrip('/')
            api_key = self.api_key_var.get()
            headers = {
                'X-API-Key': api_key
            }
            files = {
                'scans[0][file]': (filename, open(file_path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            }
            data = {}
            data['scans[0][model]'] = model
            try:
                response = requests.post(f"{api_url}/check-scan/upload-file", headers=headers, files=files, data=data, timeout=30)
                if response.status_code in [200, 201]:
                    self.log_message("✅ Upload file thành công. Dữ liệu sẽ được xử lý tự động trên server.")
                    self.total_files_processed += 1
                    self.total_barcodes_uploaded += barcode_count
                else:
                    self.log_message(f"❌ Upload file thất bại: {response.status_code} - {response.text}")
                    self.total_errors += 1
            except Exception as e:
                self.log_message(f"❌ Lỗi upload file: {str(e)}")
                self.total_errors += 1
            self.update_stats()
        except Exception as e:
            self.log_message(f"❌ Lỗi upload database: {str(e)}")
            self.total_errors += 1
            self.update_stats()
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
    app = ExcelToDBApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()