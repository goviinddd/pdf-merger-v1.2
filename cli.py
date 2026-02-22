import os
import sys
import logging
import threading
import time
import queue
import argparse
from tkinter import Tk, Text, END, messagebox, filedialog, Listbox, SINGLE
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from pypdf import PdfWriter

# --- CORE IMPORTS ---
from src.core.pipeline import PipelineOrchestrator
from src.core.pattern_loader import pattern_config
from src.core.prompt_loader import PromptLoader

# --- CONFIG ---
AUTO_EXIT_AFTER_BATCH = False

# Setup Logging Queue for GUI
log_queue = queue.Queue()

class QueueHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(record)

# Setup basic logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        QueueHandler()
    ]
)
logger = logging.getLogger(__name__)

class GUILogger:
    def __init__(self, text_widget):
        self.text_widget = text_widget
        self.text_widget.tag_config("INFO", foreground="#aaaaaa")
        self.text_widget.tag_config("SUCCESS", foreground="#00ff00")
        self.text_widget.tag_config("WARNING", foreground="orange")
        self.text_widget.tag_config("ERROR", foreground="red")

    def poll_queue(self):
        while not log_queue.empty():
            record = log_queue.get()
            
            # --- CLIENT FIX: HIDE DEBUG/SPAMMY LOGS ---
            # Only show Warnings, Errors, or highly specific SUCCESS logs to the user
            if record.levelno == logging.INFO:
                if "MERGED:" not in record.getMessage() and "Solved:" not in record.getMessage():
                    continue # Skip boring debug statements
            
            msg = self.format(record)
            
            tag = "INFO"
            if record.levelno == logging.WARNING: tag = "WARNING"
            elif record.levelno >= logging.ERROR: tag = "ERROR"
            if "MERGED:" in record.getMessage(): tag = "SUCCESS"

            self.text_widget.insert(END, msg + "\n", tag)
            self.text_widget.see(END)
            
            num_lines = int(self.text_widget.index('end-1c').split('.')[0])
            if num_lines > 200:
                self.text_widget.delete("1.0", "10.0")

        self.text_widget.after(100, self.poll_queue)

    def format(self, record):
        timestamp = time.strftime("%H:%M:%S", time.localtime(record.created))
        return f"[{timestamp}] {record.getMessage()}"


class MergerApp(ttk.Window):
    def __init__(self):
        super().__init__(themename="darkly") # Clean, professional dark theme
        self.title("AUTOMATED DOCUMENT MERGER")
        self.geometry("950x650")
        
        # --- TAB SYSTEM ---
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=BOTH, expand=True, padx=10, pady=10)
        
        self.tab_auto = ttk.Frame(self.notebook)
        self.tab_manual = ttk.Frame(self.notebook)
        
        self.notebook.add(self.tab_auto, text="⚙️ Auto Merger")
        self.notebook.add(self.tab_manual, text="🛠️ Manual Builder")
        
        # Build Tabs
        self.build_auto_tab()
        self.build_manual_tab()
        
        # Logic Init
        self.logger = GUILogger(self.log_area)
        self.logger.poll_queue()
        self.pipeline = PipelineOrchestrator()
        
        # Auto Start
        self.start_stats_heartbeat()
        logger.info("System Initialized. Awaiting documents...")
        self.after(2000, self.start_pipeline_thread)

    # ==========================================
    # TAB 1: AUTO MERGER (Clean UI)
    # ==========================================
    def build_auto_tab(self):
        # Stats
        self.stats_frame = ttk.Labelframe(self.tab_auto, text="Live Dashboard", padding=15)
        self.stats_frame.pack(fill=X, padx=10, pady=10)
        
        self.lbl_pending = self.create_stat_card("Pending", "0", "info")
        self.lbl_merged = self.create_stat_card("Merged", "0", "success")
        self.lbl_quarantine = self.create_stat_card("Action Required", "0", "danger")

        # --- 📊 NEW: REPORT BUTTON ---
        btn_frame = ttk.Frame(self.tab_auto)
        btn_frame.pack(fill=X, padx=10, pady=5)
        ttk.Button(btn_frame, text="📊 Generate Reconciliation Report", bootstyle="success-outline", command=self.generate_report).pack(side=RIGHT)
        # -----------------------------

        # Logs
        log_frame = ttk.Labelframe(self.tab_auto, text="System Alerts", padding=10)
        log_frame.pack(fill=BOTH, expand=True, padx=10, pady=5)

        self.log_area = Text(log_frame, bg="#1e1e1e", fg="#ffffff", font=("Segoe UI", 10), state="normal", bd=0)
        self.log_area.pack(fill=BOTH, expand=True, side=LEFT)
        
        scroll = ttk.Scrollbar(log_frame, command=self.log_area.yview)
        scroll.pack(side=RIGHT, fill=Y)
        self.log_area.config(yscrollcommand=scroll.set)

    def create_stat_card(self, title, value, bootstyle="info"):
        frame = ttk.Frame(self.stats_frame)
        frame.pack(side=LEFT, expand=True, fill=X)
        ttk.Label(frame, text=title, font=("Segoe UI", 10, "bold")).pack()
        lbl = ttk.Label(frame, text=value, font=("Segoe UI", 28, "bold"), bootstyle=bootstyle)
        lbl.pack()
        return lbl

    # ==========================================
    # TAB 2: MANUAL BUILDER (Requested Feature)
    # ==========================================
    def build_manual_tab(self):
        instructions = ttk.Label(self.tab_manual, text="Select PDF files, arrange them in the exact order you want (1, 2, 3...), and click Merge.", font=("Segoe UI", 10))
        instructions.pack(pady=10, padx=10, anchor="w")

        # Controls
        ctrl_frame = ttk.Frame(self.tab_manual)
        ctrl_frame.pack(fill=X, padx=10)
        
        ttk.Button(ctrl_frame, text="➕ Add Documents", bootstyle="primary", command=self.manual_add_files).pack(side=LEFT, padx=5)
        ttk.Button(ctrl_frame, text="❌ Remove Selected", bootstyle="danger-outline", command=self.manual_remove_file).pack(side=LEFT, padx=5)
        ttk.Button(ctrl_frame, text="🗑️ Clear All", bootstyle="secondary", command=lambda: self.manual_listbox.delete(0, END)).pack(side=RIGHT, padx=5)

        # List and Up/Down Arrows
        list_frame = ttk.Frame(self.tab_manual)
        list_frame.pack(fill=BOTH, expand=True, padx=10, pady=10)

        self.manual_listbox = Listbox(list_frame, selectmode=SINGLE, font=("Segoe UI", 11), bg="#2b2b2b", fg="white", selectbackground="#007bff", bd=0, highlightthickness=1)
        self.manual_listbox.pack(side=LEFT, fill=BOTH, expand=True)

        arrow_frame = ttk.Frame(list_frame)
        arrow_frame.pack(side=RIGHT, fill=Y, padx=10)
        ttk.Button(arrow_frame, text="▲ Move Up", bootstyle="info", command=self.move_up).pack(pady=10, fill=X)
        ttk.Button(arrow_frame, text="▼ Move Down", bootstyle="info", command=self.move_down).pack(pady=10, fill=X)

        # Action
        action_frame = ttk.Frame(self.tab_manual)
        action_frame.pack(fill=X, padx=10, pady=10)
        
        ttk.Button(action_frame, text="⚡ MERGE SELECTED DOCUMENTS", bootstyle="success", width=30, command=self.execute_manual_merge).pack(pady=10)

    # --- Manual Logic ---
    def manual_add_files(self):
        files = filedialog.askopenfilenames(title="Select PDFs to Merge", filetypes=[("PDF Files", "*.pdf")])
        for f in files:
            self.manual_listbox.insert(END, f)

    def manual_remove_file(self):
        selection = self.manual_listbox.curselection()
        if selection:
            self.manual_listbox.delete(selection[0])

    def move_up(self):
        selection = self.manual_listbox.curselection()
        if not selection or selection[0] == 0: return
        idx = selection[0]
        val = self.manual_listbox.get(idx)
        self.manual_listbox.delete(idx)
        self.manual_listbox.insert(idx - 1, val)
        self.manual_listbox.select_set(idx - 1)

    def move_down(self):
        selection = self.manual_listbox.curselection()
        if not selection or selection[0] == self.manual_listbox.size() - 1: return
        idx = selection[0]
        val = self.manual_listbox.get(idx)
        self.manual_listbox.delete(idx)
        self.manual_listbox.insert(idx + 1, val)
        self.manual_listbox.select_set(idx + 1)

    def execute_manual_merge(self):
        files = self.manual_listbox.get(0, END)
        if len(files) < 2:
            messagebox.showwarning("Warning", "Please select at least 2 files to merge.")
            return

        save_path = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF Files", "*.pdf")], title="Save Merged PDF As...")
        if not save_path: return

        try:
            merger = PdfWriter()
            for pdf in files:
                merger.append(pdf)
            merger.write(save_path)
            merger.close()
            messagebox.showinfo("Success", f"Documents merged successfully!\nSaved to: {save_path}")
            self.manual_listbox.delete(0, END)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to merge documents:\n{str(e)}")


    # ==========================================
    # BACKGROUND PIPELINE LOGIC
    # ==========================================

    def generate_report(self):
        """Called when the Report button is clicked"""
        self.logger.text_widget.insert(END, "Generating Excel Report... please wait.\n", "INFO")
        self.update_idletasks() # Force UI refresh
        
        try:
            filepath = self.pipeline.generate_reconciliation_report()
            if filepath:
                messagebox.showinfo("Report Ready", f"Reconciliation Report saved successfully to:\n\n{filepath}")
            else:
                messagebox.showwarning("No Data", "No PO data found in the database. Process some files first.")
        except Exception as e:
            messagebox.showerror("Report Error", f"Failed to generate report:\n{e}")

    def start_stats_heartbeat(self):
        self.update_stats()
        self.after(2000, self.start_stats_heartbeat)

    def update_stats(self):
        try:
            self.lbl_pending.config(text=str(self.pipeline.db.get_pending_count()))
            self.lbl_merged.config(text=str(self.pipeline.db.get_merged_count()))
            self.lbl_quarantine.config(text=str(self.pipeline.db.get_quarantined_count()))
        except: pass

    def start_pipeline_thread(self):
        self.thread = threading.Thread(target=self.run_pipeline)
        self.thread.daemon = True 
        self.thread.start()

    def run_pipeline(self):
        while True:
            try:
                self.pipeline.run()
                time.sleep(5) 
            except Exception as e:
                logger.error(f"Pipeline Error: {e}", exc_info=False)
                time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gui", action="store_true", help="Launch Graphical Interface")
    args = parser.parse_args()

    app = MergerApp()
    app.mainloop()