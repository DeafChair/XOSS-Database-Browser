import sys
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import requests
from bs4 import BeautifulSoup
import os
import threading
import urllib.parse
import json
from datetime import datetime
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import subprocess
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

class AstronomyDBBrowser:
    def __init__(self, root):
        # 常量定义
        self.INVALID_CHARS = '/\\:*?"<>|'  # 无效文件名字符
        self.DATE_FORMAT = "%d-%b-%Y %H:%M"  # 日期解析格式
        self.DEFAULT_WINDOW_SIZE = "1200x700"  # 默认窗口大小
        self.MAX_HISTORY = 1000  # 最大历史记录数
        
        self.root = root
        self.root.title("星明天文台数据库浏览工具")
        self.root.geometry(self.DEFAULT_WINDOW_SIZE)
        self.root.resizable(True, True)
        
        # 创建应用数据目录
        self.appdata_dir = os.path.join(os.path.expanduser('~'), 'AppData', 'Roaming', 'AstronomyDB')
        os.makedirs(self.appdata_dir, exist_ok=True)
        
        # 初始化日志
        logging.basicConfig(
            filename=os.path.join(self.appdata_dir, 'astronomy_tool.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # 动态获取图标路径
        self.icon_path = self.get_resource_path('favicon.ico')
        self._set_window_icon()
        
        # 数据库列表及对应地址
        self.db_list = {
            "PSP": "http://psp.china-vo.org/pspdata/",
            "HMT（PSP）": "https://nadc.china-vo.org/psp/hmt/PSP-HMT-DATA/data/",
            "NEXT": "http://psp.china-vo.org/next/",
            "HMT": "http://psp.china-vo.org/hmt/",
            "CSP": "http://psp.china-vo.org/csp/",
            "PAT": "https://nadc.china-vo.org/psp/pat/",
        }
        
        # 初始化功能相关变量
        self.download_history = []  # 下载历史
        
        # 加载用户设置和数据
        self.load_settings()
        self.load_download_history()
        
        # 初始化组件（先显示数据库选择界面）
        self.show_db_selector()
        
        # 确保下载目录存在
        os.makedirs(self.settings["download_dir"], exist_ok=True)
    
    def get_resource_path(self, relative_path):
        """获取资源路径：开发时返回当前目录路径，打包后返回临时目录路径"""
        if hasattr(sys, '_MEIPASS'):
            return os.path.join(sys._MEIPASS, relative_path)
        return os.path.join(os.path.abspath('.'), relative_path)
    
    def _set_window_icon(self):
        """设置窗口图标"""
        try:
            # 优先尝试从当前目录加载
            self.root.iconbitmap("favicon.ico")
        except tk.TclError:
            try:
                # 尝试从资源路径加载
                self.root.iconbitmap(self.icon_path)
            except tk.TclError as e:
                logging.warning(f"设置图标失败: {str(e)}")
    
    def load_settings(self):
        """加载用户设置"""
        # 使用已创建的应用数据目录
        self.settings_file = os.path.join(self.appdata_dir, "astronomy_settings.json")
        self.default_download_dir = os.path.join(os.path.expanduser("~"), "天文数据下载")
        
        # 默认设置
        self.settings = {
            "download_dir": self.default_download_dir,
            "last_db": list(self.db_list.keys())[0],
            "window_size": self.DEFAULT_WINDOW_SIZE,
            "max_concurrent_tasks": 3  # 默认最大并发任务数
        }
        
        # 尝试加载设置文件
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r') as f:
                    saved_settings = json.load(f)
                    # 更新设置，只保留有效的键
                    for key in self.settings:
                        if key in saved_settings:
                            self.settings[key] = saved_settings[key]
            
            # 应用窗口大小设置
            if "window_size" in self.settings:
                self.root.geometry(self.settings["window_size"])
                
            # 应用下载目录
            self.download_dir = self.settings["download_dir"]
                
        except Exception as e:
            logging.error(f"加载设置时出错: {str(e)}")
    
    def save_settings(self):
        """保存用户设置"""
        try:
            # 更新当前设置
            self.settings["download_dir"] = self.download_dir
            self.settings["last_db"] = self.db_var.get() if hasattr(self, 'db_var') else list(self.db_list.keys())[0]
            self.settings["window_size"] = self.root.geometry()
            
            with open(self.settings_file, 'w') as f:
                json.dump(self.settings, f, indent=2)
        except Exception as e:
            logging.error(f"保存设置时出错: {str(e)}")
    
    def cleanup_expired_cache(self):
        """清理过期的目录缓存"""
        try:
            # 复制一份缓存字典以避免在迭代时修改
            current_cache = self.directory_cache.copy()
            expired_count = 0
            for url in current_cache:
                if not self.is_cache_valid(url):
                    del self.directory_cache[url]
                    expired_count += 1
            if expired_count > 0:
                self.save_directory_cache()
                logging.info(f"清理了 {expired_count} 个过期的缓存条目")
        except Exception as e:
            logging.error(f"清理过期缓存时出错: {e}")

    def on_closing(self):
        """窗口关闭时保存设置和清理过期缓存"""
        self.save_settings()
        self.cleanup_expired_cache()
        self.root.destroy()

    def _create_retry_session(self):
        """创建带重试机制的请求会话"""
        session = requests.Session()
        retry = Retry(
            total=3,  # 重试3次
            backoff_factor=1,  # 每次重试间隔1秒
            status_forcelist=[429, 500, 502, 503, 504]  # 针对这些状态码重试
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def show_db_selector(self):
        """显示数据库选择窗口，让用户选择要访问的数据库"""
        # 清空当前窗口（如果有其他组件）
        for widget in self.root.winfo_children():
            widget.destroy()
        
        # 标题标签
        title_frame = ttk.Frame(self.root)
        title_frame.pack(expand=True)
        
        title_label = ttk.Label(
            title_frame, 
            text="请选择要访问的数据库：", 
            font=("SimHei", 14, "bold")
        )
        title_label.pack(pady=40)
        
        # 下拉选择框 - 使用上次选择的数据库
        default_db = self.settings["last_db"]
        self.db_var = tk.StringVar(value=default_db)
        db_combobox = ttk.Combobox(
            title_frame, 
            textvariable=self.db_var, 
            values=list(self.db_list.keys()),
            state="readonly",  # 只读，避免用户输入
            width=30,
            font=("SimHei", 12)
        )
        db_combobox.pack(pady=10)
        
        # 按钮框架
        btn_frame = ttk.Frame(title_frame)
        btn_frame.pack(pady=20)
        
        # 确认按钮
        confirm_btn = ttk.Button(
            btn_frame, 
            text="确认选择", 
            command=self.confirm_db_choice,
            width=15
        )
        confirm_btn.pack(side=tk.LEFT, padx=10)

        # 退出按钮
        exit_btn = ttk.Button(
            btn_frame, 
            text="退出", 
            command=self.on_closing,
            width=15
        )
        exit_btn.pack(side=tk.LEFT, padx=10)

    def confirm_db_choice(self):
        """用户确认选择后，初始化浏览界面"""
        # 显示加载动画
        loading_window = tk.Toplevel(self.root)
        loading_window.transient(self.root)
        loading_window.grab_set()
        loading_window.geometry("250x120")
        loading_window.title("加载中")
        loading_window.resizable(False, False)
        
        # 居中显示
        window_x = self.root.winfo_x() + (self.root.winfo_width() // 2) - 125
        window_y = self.root.winfo_y() + (self.root.winfo_height() // 2) - 60
        loading_window.geometry(f"250x120+{window_x}+{window_y}")
        
        ttk.Label(
            loading_window, 
            text="正在加载数据库...", 
            font=("SimHei", 12)
        ).pack(side=tk.TOP, pady=20)
        
        progress = ttk.Progressbar(loading_window, mode="indeterminate", length=200)
        progress.pack(side=tk.TOP, pady=10)
        progress.start()
        
        # 在后台执行切换逻辑
        def background_switch():
            selected_db_name = self.db_var.get()
            self.base_url = self.db_list[selected_db_name]
            self.path_stack = [self.base_url]  # 初始化路径栈
            
            # 销毁旧组件
            for widget in self.root.winfo_children():
                widget.destroy()
            
            # 创建新界面
            self.create_widgets()
            self.load_current_directory()
            
            # 关闭加载窗口
            loading_window.destroy()
        
        threading.Thread(target=background_switch, daemon=True).start()

    def create_widgets(self):
        """创建主界面组件"""
        # 设置全局样式
        self._setup_styles()
        
        # 顶部状态栏（带返回按钮）
        self.status_frame = ttk.Frame(self.root, padding="10")
        self.status_frame.pack(fill=tk.X)
        
        # 返回按钮
        self.back_btn = ttk.Button(
            self.status_frame, text="返回上级", command=self.go_back, width=10
        )
        self.back_btn.pack(side=tk.LEFT, padx=5)
        
        # 刷新按钮
        self.refresh_btn = ttk.Button(
            self.status_frame, 
            text="刷新", 
            command=self.refresh_current_directory,
            width=10
        )
        self.refresh_btn.pack(side=tk.LEFT, padx=5)
        
        # 状态标签
        self.status_label = ttk.Label(self.status_frame, text="加载中...")
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)
        
        # 切换数据库按钮
        self.switch_db_btn = ttk.Button(
            self.status_frame, 
            text="切换数据库", 
            command=self.show_db_selector,
            width=12
        )
        self.switch_db_btn.pack(side=tk.RIGHT, padx=5)

        # 主内容区（Treeview）
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        self.columns = ("name", "date", "size", "type")
        self.tree = ttk.Treeview(
            self.main_frame, 
            columns=self.columns, 
            show="headings", 
            selectmode="extended",  # 支持多选
            displaycolumns=(0,1,2,3)  # 显式控制列顺序
        )
        
        # 设置列标题和宽度
        self.tree.heading("name", text="名称", command=lambda: self.sort_column("name", False))
        self.tree.heading("date", text="日期", command=lambda: self.sort_column("date", False))
        self.tree.heading("size", text="大小", command=lambda: self.sort_column("size", False))
        self.tree.heading("type", text="类型", command=lambda: self.sort_column("type", False))
        
        self.tree.column("name", width=300, anchor=tk.W)
        self.tree.column("date", width=200, anchor=tk.W)
        self.tree.column("size", width=150, anchor=tk.W)
        self.tree.column("type", width=100, anchor=tk.W)
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.tree.bind("<Double-1>", self.on_double_click)

        # 滚动条
        self.scrollbar = ttk.Scrollbar(
            self.main_frame, 
            orient=tk.VERTICAL, 
            command=self.tree.yview
        )
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.configure(yscrollcommand=self.scrollbar.set)

        # 底部操作栏
        self.button_frame = ttk.Frame(self.root, padding="10")
        self.button_frame.pack(fill=tk.X)
        
        # 左侧按钮组
        left_btn_frame = ttk.Frame(self.button_frame)
        left_btn_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.download_selected_btn = ttk.Button(
            left_btn_frame, 
            text="下载选中项", 
            command=self.download_selected,
            width=15
        )
        self.download_selected_btn.pack(side=tk.LEFT, padx=5)

        self.download_all_btn = ttk.Button(
            left_btn_frame, 
            text="下载当前目录所有项", 
            command=self.download_all,
            width=20
        )
        self.download_all_btn.pack(side=tk.LEFT, padx=5)

        self.set_dir_btn = ttk.Button(
            left_btn_frame, 
            text="设置下载目录", 
            command=self.set_download_dir,
            width=15
        )
        self.set_dir_btn.pack(side=tk.LEFT, padx=5)
        
        self.history_btn = ttk.Button(
            left_btn_frame, 
            text="下载历史", 
            command=self.show_download_history,
            width=15
        )
        self.history_btn.pack(side=tk.LEFT, padx=5)
        
        # 右侧按钮组
        right_btn_frame = ttk.Frame(self.button_frame)
        right_btn_frame.pack(side=tk.RIGHT)
        
        self.about_btn = ttk.Button(
            right_btn_frame, 
            text="关于", 
            command=self.show_about,
            width=10
        )
        self.about_btn.pack(side=tk.RIGHT, padx=5)

        self.progress = ttk.Progressbar(
            self.button_frame, 
            orient=tk.HORIZONTAL, 
            length=200, 
            mode='determinate'
        )
        self.progress.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=5)

        # 状态栏
        self.status_bar = ttk.Label(
            self.root, 
            text=f"下载目录: {self.download_dir}", 
            relief=tk.SUNKEN, 
            anchor=tk.W
        )
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # 绑定窗口关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # 初始化目录缓存
        self.load_directory_cache()

    def _setup_styles(self):
        """设置界面样式"""
        style = ttk.Style()
        style.configure("Treeview", rowheight=25, font=("SimHei", 10))
        style.configure("Treeview.Heading", font=("SimHei", 10, "bold"))
        style.configure("TButton", font=("SimHei", 10))
        style.configure("TLabel", font=("SimHei", 10))
        style.configure("TLabelframe", font=("SimHei", 10, "bold"))
        style.configure("TLabelframe.Label", font=("SimHei", 10, "bold"))

    def load_directory_cache(self):
        """加载目录缓存"""
        try:
            self.cache_file = os.path.join(self.appdata_dir, "directory_cache.json")
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.directory_cache = json.load(f)
            else:
                self.directory_cache = {}
        except Exception as e:
            logging.error(f"加载目录缓存失败: {e}")
            self.directory_cache = {}

    def save_directory_cache(self):
        """保存目录缓存"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.directory_cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存目录缓存失败: {e}")

    def is_cache_valid(self, url):
        """检查缓存是否有效（有效期24小时）"""
        if url not in self.directory_cache or "cache_time" not in self.directory_cache[url]:
            return False
        cache_time = datetime.strptime(self.directory_cache[url]["cache_time"], "%Y-%m-%d %H:%M:%S")
        return (datetime.now() - cache_time).total_seconds() < 86400  # 24小时

    def refresh_current_directory(self):
        """刷新当前目录"""
        if self.path_stack:
            self.load_current_directory()

    def load_current_directory(self):
        """加载当前目录内容"""
        self.status_label.config(text="加载中...")
        threading.Thread(
            target=self._fetch_directory_contents, 
            args=(self.path_stack[-1],), 
            daemon=True
        ).start()

    def _fetch_directory_contents(self, current_url):
        """获取并解析目录内容（优先使用缓存）"""
        try:
            # 检查缓存是否有效
            if hasattr(self, 'directory_cache') and self.is_cache_valid(current_url):
                logging.info(f"使用缓存数据: {current_url}")
                # 兼容旧缓存格式，优先使用dir_items，如果不存在则使用content
                if "dir_items" in self.directory_cache[current_url]:
                    dir_items = self.directory_cache[current_url]["dir_items"]
                elif "content" in self.directory_cache[current_url]:
                    dir_items = self.directory_cache[current_url]["content"]
                else:
                    dir_items = []
                self._render_directory(dir_items, current_url)
                return

            # 缓存无效或不存在，请求网络
            if not current_url.endswith('/'):
                current_url += '/'
                
            session = self._create_retry_session()
            response = session.get(current_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            pre = soup.find('pre')
            if not pre:
                raise ValueError("未找到目录列表")
            
            # 解析目录内容
            a_tags = pre.find_all('a')
            dir_items = []  # 存储解析后的目录项
            
            for a in a_tags:
                # 安全获取属性值
                name = a.text.strip() if a.text else ""
                href = a.get('href', "")  # 使用get方法避免KeyError
                
                # 过滤无效项
                if self._should_skip_item(name, href):
                    continue
                
                # 构造完整URL
                if not href:
                    continue
                    
                full_url = urllib.parse.urljoin(current_url, href)
                
                # 解析日期和大小
                date, size = self._parse_file_info(a)
                
                # 判断是否为目录
                file_type = "目录" if name.endswith('/') or href.endswith('/') else "文件"
                
                # 添加到目录项列表
                dir_items.append({
                    "name": name,
                    "href": href,
                    "date": date,
                    "size": size,
                    "file_type": file_type,
                    "full_url": full_url
                })
            
            # 更新缓存
            if hasattr(self, 'directory_cache'):
                 self.directory_cache[current_url] = {
                     "dir_items": dir_items,
                     "cache_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                 }
                 self.save_directory_cache()

            # 渲染目录
            self._render_directory(dir_items, current_url)

        except Exception as e:
            self._update_status(f"加载失败: {str(e)}")
            self._show_error(f"无法加载目录: {str(e)}")

    def _should_skip_item(self, name, href):
        """判断是否应该跳过当前项（上级目录或无效项）"""
        skip_conditions = [
            name in (".", ".."),
            href in ("../", "..", "./", "."),
            href.startswith("../") and len(href) > 3,
            "parent directory" in name.lower(),
            "上级目录" in name
        ]
        return any(skip_conditions)

    def _parse_file_info(self, a_tag):
        """解析文件的日期和大小信息"""
        date = "未知"
        size = "未知"
        
        next_sibling = a_tag.next_sibling
        if next_sibling:
            next_sibling = next_sibling.strip()
            if next_sibling:
                parts = next_sibling.split()
                if len(parts) >= 3:
                    date = ' '.join(parts[:2])
                    size = parts[2] if parts[2] != '-' else "未知"
        
        return date, size

    def _render_directory(self, dir_items, current_url):
        """渲染目录内容到Treeview"""
        # 在主线程中更新UI
        def update_ui():
            # 清空Treeview
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # 添加目录项
            for item in dir_items:
                self.tree.insert(
                    "", 
                    tk.END, 
                    values=(item["name"], item["date"], item["size"], item["file_type"]), 
                    tags=(item["full_url"],)
                )
            
            # 更新状态
            self.status_label.config(text=f"当前目录: {current_url} (共 {len(dir_items)} 项)")
            self.back_btn.config(state=tk.NORMAL if len(self.path_stack) > 1 else tk.DISABLED)
        
        self.root.after(0, update_ui)

    def on_double_click(self, event):
        """双击进入子目录"""
        item = self.tree.identify_row(event.y)
        if not item:
            return
        
        values = self.tree.item(item, "values")
        file_type = values[3]
        file_url = self.tree.item(item, "tags")[0]
        
        if file_type == "目录":
            if file_url not in self.path_stack:
                self.path_stack.append(file_url)
                self.load_current_directory()

    def go_back(self):
        """返回上级目录"""
        if len(self.path_stack) > 1:
            self.path_stack.pop()
            self.load_current_directory()

    def sort_column(self, col, reverse):
        """按列排序"""
        # 获取列数据
        data = [
            (self.tree.set(child, col), child)
            for child in self.tree.get_children()
        ]
        
        # 特殊处理日期列
        if col == "date":
            def parse_date(date_str):
                try:
                    return datetime.strptime(date_str, self.DATE_FORMAT)
                except:
                    return datetime.min
                    
            data.sort(key=lambda x: parse_date(x[0]), reverse=reverse)
        # 特殊处理大小列
        elif col == "size":
            def parse_size(size_str):
                try:
                    if size_str == "未知":
                        return 0
                    if size_str.endswith("K"):
                        return float(size_str[:-1]) * 1024
                    if size_str.endswith("M"):
                        return float(size_str[:-1]) * 1024 * 1024
                    if size_str.endswith("G"):
                        return float(size_str[:-1]) * 1024 * 1024 * 1024
                    return float(size_str)
                except:
                    return 0
                    
            data.sort(key=lambda x: parse_size(x[0]), reverse=reverse)
        else:
            # 默认排序（字符串）
            data.sort(reverse=reverse)
        
        # 重新排列项目
        for i, (val, child) in enumerate(data):
            self.tree.move(child, "", i)
        
        # 反转下次排序顺序
        self.tree.heading(col, command=lambda: self.sort_column(col, not reverse))

    def show_about(self):
        """显示关于对话框"""
        about_text = (
            "星明天文台数据库浏览工具\n\n"
            "版本: 1.2\n"
            "DeafChair开发\n"
            "开发日期: 2025年\n\n"
            "联系方式: deafchair@qq.com\n"
            "本工具用于浏览和下载星明天文台天文数据。"
        )
        messagebox.showinfo("关于", about_text)

    def _update_status(self, text):
        """在主线程中更新状态标签"""
        def update():
            self.status_label.config(text=text)
        self.root.after(0, update)

    def _show_error(self, message):
        """在主线程中显示错误消息"""
        def show():
            messagebox.showerror("错误", message)
        self.root.after(0, show)

    def set_download_dir(self):
        """设置下载目录"""
        directory = filedialog.askdirectory(
            title="选择下载目录",
            initialdir=self.download_dir
        )
        if directory:
            self.download_dir = directory
            self.status_bar.config(text=f"下载目录: {directory}")
            self.settings["download_dir"] = directory  # 更新设置
            self.save_settings()

    def download_selected(self):
        """下载选中项"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("提示", "请选择要下载的内容")
            return
        
        confirm = messagebox.askyesno(
            "确认下载", 
            f"即将下载 {len(selected)} 个项目，是否继续？"
        )
        if not confirm:
            return
        
        threading.Thread(
            target=self._batch_download, 
            args=(selected,), 
            daemon=True
        ).start()

    def download_all(self):
        """下载当前目录所有项"""
        all_items = self.tree.get_children()
        if not all_items:
            messagebox.showinfo("提示", "当前目录没有可下载的内容")
            return
        
        confirm = messagebox.askyesno(
            "确认下载", 
            f"即将下载 {len(all_items)} 个项目，是否继续？"
        )
        if not confirm:
            return
        
        threading.Thread(
            target=self._batch_download, 
            args=(all_items,), 
            daemon=True
        ).start()

    def _download_item(self, item, total, index):
        """下载单个项目（用于并发执行）"""
        try:
            values = self.tree.item(item, "values")
            file_type = values[3]
            file_url = self.tree.item(item, "tags")[0]
            file_name = values[0]
            
            self._update_status(f"正在下载 ({index+1}/{total}): {file_name}")
            
            if file_type == "目录":
                dir_path = os.path.join(self.download_dir, file_name.rstrip('/'))
                self._download_directory(file_url, dir_path)
                return True, file_name, None
            else:
                # 获取当前目录名（从URL中提取）
                url_parts = [p for p in file_url.rstrip('/').split('/') if p]
                if len(url_parts) >= 2:
                    # 取倒数第二个部分作为目录名（假设URL格式为.../目录名/文件名）
                    dir_name = url_parts[-2]
                    # 对目录名进行URL解码
                    dir_name = urllib.parse.unquote(dir_name)
                    # 过滤目录名非法字符
                    for c in self.INVALID_CHARS:
                        dir_name = dir_name.replace(c, '_')
                    # 创建目录文件夹
                    file_dir = os.path.join(self.download_dir, dir_name)
                    os.makedirs(file_dir, exist_ok=True)
                else:
                    # 如果无法提取目录名，则直接使用下载目录
                    file_dir = self.download_dir
                # 下载文件到目录文件夹
                self._download_file(file_url, file_dir, show_msg=False)
                return True, file_name, None
        except Exception as e:
            return False, file_name, str(e)

    def _batch_download(self, items):
        """批量下载逻辑（支持并发控制）"""
        total = len(items)
        success = 0
        fail = 0
        fail_list = []
        self.cancel_download = False  # 取消标志
        
        # 添加取消按钮
        cancel_btn = ttk.Button(
            self.button_frame, 
            text="取消下载", 
            command=lambda: setattr(self, 'cancel_download', True)
        )
        cancel_btn.pack(side=tk.LEFT, padx=5)
        
        try:
            # 使用线程池控制并发下载
            max_workers = self.settings.get("max_concurrent_tasks", 3)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有下载任务
                futures = []
                for i, item in enumerate(items):
                    if self.cancel_download:
                        break
                    future = executor.submit(self._download_item, item, total, i)
                    futures.append(future)
                
                # 处理任务结果
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    if self.cancel_download:
                        break
                    
                    try:
                        result, file_name, error = future.result()
                        if result:
                            success += 1
                        else:
                            fail += 1
                            fail_list.append(f"{file_name}: {error}")
                            self._update_status(f"下载失败 ({i+1}/{total}): {file_name}")
                        
                        # 更新进度条
                        self._update_progress(int((i+1)/total * 100))
                    except Exception as e:
                        fail += 1
                        fail_list.append(f"任务{i+1}: {str(e)}")
                        self._update_status(f"任务出错 ({i+1}/{total})")
            
            # 处理取消情况
            if self.cancel_download:
                result = f"下载已取消：成功 {success} 个，失败 {fail} 个，取消 {total - success - fail} 个"
            else:
                result = f"下载完成：成功 {success} 个，失败 {fail} 个\n"
                if fail > 0:
                    result += "\n失败项（前5个）：\n" + "\n".join(fail_list[:5])
                    if len(fail_list) > 5:
                        result += f"\n... 还有 {len(fail_list)-5} 个失败项"
            
            self._update_status(f"批量下载完成: {success}/{total}")
            self._show_result("批量下载结果", result)
            
        finally:
            # 重置进度条和按钮
            self._update_progress(0)
            self.root.after(0, cancel_btn.destroy)
            self.save_settings()  # 下载完成后保存设置

    def _download_file(self, file_url, save_dir, show_msg=True):
        """下载单个文件（支持断点续传）"""
        try:
            file_name = urllib.parse.unquote(file_url.split('/')[-1])
            save_path = os.path.join(save_dir, file_name)
            
            # 检查是否已部分下载
            resume_byte_pos = 0
            if os.path.exists(save_path):
                resume_byte_pos = os.path.getsize(save_path)
                # 如果文件已完整下载，直接返回
                if self._is_file_complete(file_url, resume_byte_pos):
                    logging.info(f"文件已完整下载：{file_name}")
                    if show_msg:
                        self._show_info(f"{file_name} 已完整下载")
                    self.add_download_record(file_url, save_path, "成功", file_name)
                    return
                else:
                    logging.info(f"继续下载：{file_name}（已下载 {resume_byte_pos} 字节）")
            
            # 构造请求头，支持断点续传
            headers = {"Range": f"bytes={resume_byte_pos}-"} if resume_byte_pos > 0 else {}
            
            def reporthook(count, block_size, total_size):
                # 检查是否需要取消下载
                if self.cancel_download:
                    raise Exception("下载已取消")
                    
                if total_size <= 0:  # 未知总大小
                    self._update_progress(0)
                    self._update_status(f"下载中：{file_name}（大小未知）")
                else:
                    # 加上已下载的字节数
                    total_downloaded = resume_byte_pos + count * block_size
                    percent = min(int(total_downloaded * 100 / total_size), 100)
                    self._update_progress(percent)
                    self._update_status(f"下载中：{file_name}（{percent}%）")
            
            # 下载（使用ab模式追加）
            with open(save_path, 'ab' if resume_byte_pos > 0 else 'wb') as f:
                req = urllib.request.Request(file_url, headers=headers)
                with urllib.request.urlopen(req) as response:
                    while True:
                        if self.cancel_download:
                            raise Exception("下载已取消")
                            
                        chunk = response.read(1024 * 1024)  # 1MB块
                        if not chunk:
                            break
                        f.write(chunk)
                        # 更新进度
                        downloaded = resume_byte_pos + f.tell()
                        total_size = int(response.headers.get('Content-Length', 0)) + resume_byte_pos
                        if total_size > 0:
                            percent = min(int(downloaded * 100 / total_size), 100)
                            self._update_progress(percent)
                            self._update_status(f"下载中：{file_name}（{percent}%）")
            
            # 验证文件完整性
            if not self._is_file_complete(file_url, os.path.getsize(save_path)):
                raise Exception("文件下载不完整")
                
            # 记录下载历史
            self.add_download_record(file_url, save_path, "成功", file_name)
            
            if show_msg:
                self._show_info(f"{file_name} 已下载到 {save_dir}")

        except Exception as e:
            if "已取消" not in str(e):
                # 记录失败历史
                self.add_download_record(file_url, "", "失败", file_name)
                logging.error(f"下载失败：{file_url}，错误：{str(e)}")
            
            if show_msg:
                self._show_error(f"无法下载 {file_url}: {str(e)}")
            raise

    def _is_file_complete(self, file_url, local_size):
        """检查文件是否已完整下载"""
        try:
            response = requests.head(file_url, allow_redirects=True, timeout=10)
            remote_size = int(response.headers.get('Content-Length', 0))
            return local_size >= remote_size - 1024  # 允许1KB误差
        except:
            return False  # 无法验证时默认重新下载

    def _download_directory(self, dir_url, save_root):
        """递归下载目录"""
        try:
            # 1. 规范化 URL
            if not dir_url.endswith('/'):
                dir_url = dir_url.rstrip('/') + '/'
            # 避免替换协议部分的双斜杠
            if '://' in dir_url:
                protocol, rest = dir_url.split('://', 1)
                dir_url = f'{protocol}://{rest.replace("//", "/")}'
                # 修复URL中的协议双斜杠
            else:
                dir_url = dir_url.replace('//', '/')
            
            # 2. 提取当前目录名
            url_parts = [p for p in dir_url.rstrip('/').split('/') if p]
            if not url_parts:
                raise ValueError("无法解析目录URL")
            dir_name = url_parts[-1]  # 使用URL的最后一部分作为目录名
            # 3. 对目录名进行URL解码
            dir_name = urllib.parse.unquote(dir_name)
            
            # 4. 过滤目录名非法字符
            for c in self.INVALID_CHARS:
                dir_name = dir_name.replace(c, '_')
            
            # 5. 直接使用传入的save_root作为保存目录
            save_dir = save_root
            os.makedirs(save_dir, exist_ok=True)
            
            # 5. 检查是否需要取消下载
            if self.cancel_download:
                raise Exception("下载已取消")
            
            # 6. 请求网络获取目录内容
            session = self._create_retry_session()
            response = session.get(dir_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            pre = soup.find('pre')
            if not pre:
                raise ValueError("未找到目录列表")
            
            # 解析目录内容
            a_tags = pre.find_all('a')
            dir_items = []
            for a in a_tags:
                name = a.text.strip() if a.text else ""
                href = a.get('href', "")
                
                if self._should_skip_item(name, href):
                    continue
                    
                if not href:
                    continue
                    
                full_url = urllib.parse.urljoin(dir_url, href)
                date, size = self._parse_file_info(a)
                file_type = "目录" if name.endswith('/') or href.endswith('/') else "文件"
                
                dir_items.append({
                    "name": name,
                    "href": href,
                    "date": date,
                    "size": size,
                    "file_type": file_type,
                    "full_url": full_url
                })
            
            # 7. 处理目录中的项目
            for item in dir_items:
                if self.cancel_download:
                    raise Exception("下载已取消")
                    
                if item["file_type"] == "目录":
                    # 递归下载子目录
                    self._download_directory(item["full_url"], save_dir)
                else:
                    # 下载文件
                    try:
                        self._download_file(item["full_url"], save_dir, show_msg=False)
                    except Exception as e:
                        logging.error(f"文件下载失败: {item['full_url']} → {str(e)}")
                        raise

        except Exception as e:
            error_info = f"下载目录失败: {dir_url}\n错误: {str(e)}"
            logging.error(error_info)
            self._update_status(error_info[:60] + "...")
            raise

    def _update_progress(self, value):
        """在主线程中更新进度条"""
        def update():
            self.progress['value'] = value
        self.root.after(0, update)

    def _show_info(self, message):
        """在主线程中显示信息对话框"""
        def show():
            messagebox.showinfo("提示", message)
        self.root.after(0, show)

    def _show_result(self, title, message):
        """在主线程中显示结果对话框"""
        def show():
            messagebox.showinfo(title, message)
        self.root.after(0, show)

    def load_download_history(self):
        """加载下载历史"""
        try:
            self.history_file = os.path.join(self.appdata_dir, "download_history.json")
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    self.download_history = json.load(f)
            else:
                self.download_history = []
        except Exception as e:
            logging.error(f"加载历史记录失败: {e}")
            self.download_history = []

    def save_download_history(self):
        """保存下载历史"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.download_history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存历史记录失败: {e}")

    def add_download_record(self, url, local_path, status, name):
        """添加一条下载记录"""
        record = {
            "url": url,
            "local_path": local_path,
            "name": name,
            "status": status,  # "成功" 或 "失败"
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        # 限制历史记录数量
        self.download_history.insert(0, record)
        if len(self.download_history) > self.MAX_HISTORY:
            self.download_history = self.download_history[:self.MAX_HISTORY]
        self.save_download_history()

    def show_download_history(self):
        """显示下载历史窗口"""
        if not self.download_history:
            messagebox.showinfo("下载历史", "暂无下载记录")
            return
            
        history_window = tk.Toplevel(self.root)
        history_window.title("下载历史记录")
        history_window.geometry("900x600")
        # 设置窗口图标
        try:
            # 尝试从资源路径加载
            history_window.iconbitmap(self.icon_path)
        except tk.TclError as e:
            logging.warning(f"设置历史窗口图标失败: {str(e)}")
        history_window.transient(self.root)  # 设置为主窗口的子窗口
        history_window.grab_set()  # 模态窗口
        
        # 居中显示
        window_width = 900
        window_height = 600
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width // 2) - (window_width // 2)
        y = (screen_height // 2) - (window_height // 2)
        history_window.geometry(f"{window_width}x{window_height}+{x}+{y}")

        # 创建按钮栏
        button_frame = ttk.Frame(history_window, padding=10)
        button_frame.pack(fill=tk.X)
        
        ttk.Button(
            button_frame, 
            text="清空历史", 
            command=lambda: clear_history()
        ).pack(side=tk.RIGHT, padx=5)

        # 创建Treeview
        columns = ("name", "time", "status", "path")
        tree = ttk.Treeview(history_window, columns=columns, show="headings")
        tree.heading("name", text="文件名", command=lambda: sort_history(tree, "name"))
        tree.heading("time", text="下载时间", command=lambda: sort_history(tree, "time"))
        tree.heading("status", text="状态", command=lambda: sort_history(tree, "status"))
        tree.heading("path", text="本地路径", command=lambda: sort_history(tree, "path"))

        tree.column("name", width=200)
        tree.column("time", width=150)
        tree.column("status", width=80)
        tree.column("path", width=400)

        # 添加滚动条
        scrollbar_y = ttk.Scrollbar(history_window, orient=tk.VERTICAL, command=tree.yview)
        scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
        tree.configure(yscrollcommand=scrollbar_y.set)
        
        scrollbar_x = ttk.Scrollbar(history_window, orient=tk.HORIZONTAL, command=tree.xview)
        scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)
        tree.configure(xscrollcommand=scrollbar_x.set)

        # 填充数据
        def populate_tree(records):
            for item in tree.get_children():
                tree.delete(item)
            for i, record in enumerate(records):
                tree.insert("", tk.END, iid=str(i), values=(
                    record["name"],
                    record["time"],
                    record["status"],
                    record["local_path"] or "下载失败"
                ))
        
        populate_tree(self.download_history)
        tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # 右键菜单
        menu = tk.Menu(history_window, tearoff=0)
        menu.add_command(label="重新下载", command=lambda: redownload_selected(tree))
        menu.add_command(label="打开文件位置", command=lambda: open_file_location(tree))
        menu.add_separator()
        menu.add_command(label="删除记录", command=lambda: delete_record(tree))

        def on_right_click(event):
            item = tree.identify_row(event.y)
            if item:
                tree.selection_set(item)
                menu.post(event.x_root, event.y_root)

        tree.bind("<Button-3>", on_right_click)
        
        # 双击打开文件位置
        def on_double_click(event):
            item = tree.identify_row(event.y)
            if item:
                open_file_location(tree)
        
        tree.bind("<Double-1>", on_double_click)

        # 排序历史记录
        def sort_history(tree, col):
            # 获取当前排序状态
            current_sort = tree.heading(col, "text")
            reverse = False
            if current_sort.endswith(" ↑"):
                reverse = True
                tree.heading(col, text=col)
            else:
                # 重置其他列的排序指示
                for c in columns:
                    if tree.heading(c, "text").endswith(" ↑") or tree.heading(c, "text").endswith(" ↓"):
                        tree.heading(c, text=c)
                tree.heading(col, text=col + " ↑")
            
            # 获取当前显示的数据
            items = tree.get_children()
            records = []
            for item in items:
                idx = int(item)
                records.append((self.download_history[idx], idx))
            
            # 根据列排序
            if col == "time":
                records.sort(key=lambda x: datetime.strptime(x[0]["time"], "%Y-%m-%d %H:%M:%S"), reverse=reverse)
            else:
                records.sort(key=lambda x: x[0][col].lower(), reverse=reverse)
            
            # 重新排列
            for i, (_, idx) in enumerate(records):
                tree.move(str(idx), "", i)

        # 重新下载选中的记录
        def redownload_selected(tree):
            selected = tree.selection()
            if not selected:
                return
            record_idx = int(selected[0])
            record = self.download_history[record_idx]
            
            history_window.destroy()  # 关闭历史窗口
            
            # 调用下载方法
            threading.Thread(
                target=self._download_file,
                args=(record["url"], self.download_dir, True),
                daemon=True
            ).start()

        # 打开文件所在文件夹
        def open_file_location(tree):
            selected = tree.selection()
            if not selected:
                return
            record_idx = int(selected[0])
            record = self.download_history[record_idx]
            
            if record["status"] == "成功" and record["local_path"] and os.path.exists(record["local_path"]):
                try:
                    # 跨平台打开文件夹
                    file_dir = os.path.dirname(record["local_path"])
                    if os.name == 'nt':  # Windows
                        os.startfile(file_dir)
                    else:  # macOS/Linux
                        subprocess.run(['open' if sys.platform == 'darwin' else 'xdg-open', file_dir])
                except Exception as e:
                    messagebox.showerror("错误", f"无法打开文件夹: {str(e)}")
            else:
                messagebox.showinfo("提示", "文件不存在或下载失败")

        # 删除记录
        def delete_record(tree):
            selected = tree.selection()
            if not selected:
                return
            record_idx = int(selected[0])
            
            if messagebox.askyesno("确认", f"确定要删除 '{self.download_history[record_idx]['name']}' 的记录吗？"):
                del self.download_history[record_idx]
                self.save_download_history()
                populate_tree(self.download_history)
        
        # 清空历史
        def clear_history():
            if not self.download_history:
                return
                
            if messagebox.askyesno("确认", "确定要清空所有下载历史记录吗？"):
                self.download_history = []
                self.save_download_history()
                populate_tree(self.download_history)

if __name__ == "__main__":
    root = tk.Tk()
    app = AstronomyDBBrowser(root)
    root.mainloop()