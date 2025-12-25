print(">>> CLI LOADED WITH GUI SUPPORT (V1.3 Docker-Safe) <<<") 
import sys
import logging
import argparse
import time
import threading
import queue
import os
import subprocess
import platform
from pathlib import Path

# Ensure Python finds the 'src' module
sys.path.append(str(Path(__file__).parent))

# --- CONDITIONAL GUI IMPORTS ---
try:
    import tkinter as tk
    from tkinter import scrolledtext
    import ttkbootstrap as ttk
    from ttkbootstrap.constants import *
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False

# ==========================================
#             HELPER FUNCTIONS
# ==========================================

def setup_logging(debug_mode: bool):
    """Configures logging for both CLI and GUI modes."""
    level = logging.DEBUG if debug_mode else logging.INFO
    format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[
            logging.FileHandler("merger_system.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Silence noisy libraries
    logging.getLogger("pdfminer").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("ultralytics").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING) 

def run_reporting(db_manager):
    """Helper to run the report generator safely."""
    try:
        from src.core.reporter import ReportGenerator
        logger = logging.getLogger(__name__)
        logger.info(">>> Generating Session Report...")
        
        reporter = ReportGenerator(db_manager)
        report_path = reporter.generate_excel_report()
        
        if report_path:
            logger.info(f"✅ REPORT UPDATED: {report_path}")
        else:
            logger.info("ℹ️  No data to report.")
            
    except Exception as e:
        logging.error(f"Failed to generate report: {e}")

# ==========================================
#               GUI CLASSES
# ==========================================

if GUI_AVAILABLE:
    class TextHandler(logging.Handler):
        """Redirects logging to the GUI text box safely."""
        def __init__(self, text_widget):
            logging.Handler.__init__(self)
            self.text_widget = text_widget
            self.queue = queue.Queue()

        def emit(self, record):
            msg = self.format(record)
            self.queue.put((msg, record.levelname))
            self.text_widget.event_generate("<<LogUpdate>>")

    class ReceiptApp(ttk.Window):
        def __init__(self):
            super().__init__(themename="darkly")
            self.title("Nexus Receipt Orchestrator (V1.2)")
            self.geometry("950x700")
            
            self.input_folders = ["Purchase_order", "Delivery_note", "Sales_invoice"]
            
            from src.core.pipeline import PipelineOrchestrator
            self.orchestrator = PipelineOrchestrator()
            self.is_running = False

            self._setup_ui()
            self._setup_logging()
            self._refresh_pending_count()

        def _setup_ui(self):
            header = ttk.Frame(self, padding=20)
            header.pack(fill=X)
            ttk.Label(header, text="Automated PDF Merger System", font=("Helvetica", 20, "bold")).pack(side=LEFT)
            ttk.Label(header, text="V1.2 Hybrid", font=("Helvetica", 10), bootstyle="secondary").pack(side=LEFT, padx=10, pady=(10, 0))

            stats_frame = ttk.Labelframe(self, text="Session Statistics", padding=15, bootstyle="info")
            stats_frame.pack(fill=X, padx=20, pady=10)
            
            for i in range(3): stats_frame.columnconfigure(i, weight=1)

            self.lbl_pending = self._make_card(stats_frame, "Pending Files", "0", 0, "secondary")
            self.lbl_merged = self._make_card(stats_frame, "Merged", "0", 1, "success")
            self.lbl_quarantine = self._make_card(stats_frame, "Quarantined", "0", 2, "danger")

            console_frame = ttk.Labelframe(self, text="Live System Logs", padding=10)
            console_frame.pack(fill=BOTH, expand=True, padx=20, pady=5)
            
            self.log_display = scrolledtext.ScrolledText(console_frame, height=15, state='disabled', bg="#222", fg="#eee", font=("Consolas", 10))
            self.log_display.pack(fill=BOTH, expand=True)

            controls = ttk.Frame(self, padding=20)
            controls.pack(fill=X, side=BOTTOM)
            
            self.btn_run = ttk.Button(controls, text="RUN BATCH NOW", command=self.run_thread, bootstyle="success-outline", width=20)
            self.btn_run.pack(side=LEFT, padx=5)

            ttk.Separator(controls, orient=VERTICAL).pack(side=LEFT, padx=15, fill=Y)
            
            ttk.Button(controls, text="📂 Open Root", command=lambda: self.open_folder("."), bootstyle="secondary").pack(side=LEFT, padx=5)
            ttk.Button(controls, text="☣ Quarantine", command=lambda: self.open_folder("quarantine"), bootstyle="danger").pack(side=LEFT, padx=5)
            ttk.Button(controls, text="📊 Reports", command=lambda: self.open_folder("reports"), bootstyle="info").pack(side=LEFT, padx=5)
            
            ttk.Button(controls, text="Exit", command=self.destroy, bootstyle="danger-outline").pack(side=RIGHT)

        def _make_card(self, parent, title, val, col, style):
            f = ttk.Frame(parent)
            f.grid(row=0, column=col, sticky="ew")
            ttk.Label(f, text=title).pack()
            l = ttk.Label(f, text=val, font=("Helvetica", 24, "bold"), bootstyle=style)
            l.pack()
            return l

        def _setup_logging(self):
            self.handler = TextHandler(self.log_display)
            self.handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
            
            self.log_display.tag_config("INFO", foreground="white")
            self.log_display.tag_config("WARNING", foreground="orange")
            self.log_display.tag_config("ERROR", foreground="#ff4d4d")
            self.log_display.tag_config("CRITICAL", foreground="#ff0000", background="white")

            logging.getLogger().addHandler(self.handler)
            self.log_display.bind("<<LogUpdate>>", self._update_logs)

        def _update_logs(self, event):
            while not self.handler.queue.empty():
                msg, level = self.handler.queue.get()
                self.log_display.configure(state='normal')
                self.log_display.insert(tk.END, msg + "\n", level)
                self.log_display.see(tk.END)
                self.log_display.configure(state='disabled')
                
                if "MOVED TO QUARANTINE" in msg or "Quarantining Bundle" in msg: 
                    self._inc_stat(self.lbl_quarantine)
                if "MERGED:" in msg:
                    self._inc_stat(self.lbl_merged)

        def _inc_stat(self, lbl):
            try:
                val = int(lbl.cget("text"))
                lbl.config(text=str(val + 1))
            except: pass

        def _refresh_pending_count(self):
            total = 0
            for folder in self.input_folders:
                if os.path.exists(folder):
                    try:
                        total += len([f for f in os.listdir(folder) if f.lower().endswith('.pdf')])
                    except Exception: pass
            try:
                self.lbl_pending.config(text=str(total))
            except: pass

        def run_thread(self):
            if self.is_running: return
            self.is_running = True
            self.btn_run.config(state="disabled", text="Running...")
            threading.Thread(target=self._execute_pipeline, daemon=True).start()

        def _execute_pipeline(self):
            try:
                self.lbl_pending.after(0, self._refresh_pending_count)
                self.orchestrator.run()
                run_reporting(self.orchestrator.db)
                self.lbl_pending.after(0, self._refresh_pending_count)
            except Exception as e:
                logging.error(f"GUI Execution Error: {e}")
            finally:
                self.is_running = False
                self.btn_run.after(0, lambda: self.btn_run.config(state="normal", text="RUN BATCH NOW"))

        def open_folder(self, path):
            p = os.path.abspath(path)
            os.makedirs(p, exist_ok=True)
            try:
                if platform.system() == "Windows": os.startfile(p)
                elif platform.system() == "Darwin": subprocess.Popen(["open", p])
                else: subprocess.Popen(["xdg-open", p])
            except Exception as e:
                logging.error(f"Cannot open folder: {e}")

# ==========================================
#               ENTRY POINT
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated PDF Merger V1.2")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    parser.add_argument("--loop", action="store_true", help="Run continuously (Daemon Mode)")
    parser.add_argument("--interval", type=int, default=60, help="Sleep interval in seconds for loop mode")
    parser.add_argument("--gui", action="store_true", help="Launch the GUI Dashboard")
    
    args = parser.parse_args()
    setup_logging(args.debug)

    if args.gui:
        if not GUI_AVAILABLE:
            print("❌ Error: 'ttkbootstrap' not found or No Display. Install with: pip install ttkbootstrap")
            sys.exit(1)
        # We access ReceiptApp here, which is safe because GUI_AVAILABLE is confirmed True
        app = ReceiptApp()
        app.mainloop()
    else:
        # Headless mode logic
        from src.core.pipeline import PipelineOrchestrator
        logger = logging.getLogger(__name__)
        try:
            orchestrator = PipelineOrchestrator()
            if args.loop:
                logger.info(f"Starting DAEMON mode...")
                while True:
                    orchestrator.run()
                    run_reporting(orchestrator.db)
                    time.sleep(args.interval)
            else:
                orchestrator.run()
                run_reporting(orchestrator.db)
        except KeyboardInterrupt:
            logger.info("Exiting...")
