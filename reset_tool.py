import os
import shutil
import time

# --- CONFIGURATION ---
SYSTEM_NAME = "Abic and Mani CA Automation System"

# Files/Folders to wipe
TARGETS = {
    "Database": "merger_state.db",
    "Logs": "merger_system.log",
    "Archives": "archive",
    "Quarantine": "quarantine",
    
    # --- SAFE ZONE (Commented out to PREVENT deletion) ---
    # "Merged Output": "Merged_PDFs", 
    # "Cache": "groq_cache"
}

def clean_system():
    print("=======================================================")
    print(f"   {SYSTEM_NAME.upper()}")
    print("           FACTORY RESET UTILITY")
    print("=======================================================")
    print("⚠️  WARNING: This will delete system history/logs.")
    print("   The following will be wiped:")
    for name, path in TARGETS.items():
        print(f"   - {name} ({path})")
        
    print("\n   [SAFE] The following are NOT touched:")
    print("   - Input Folders (Purchase_order, etc.)")
    print("   - Merged_PDFs (Final Output)")
    print("=======================================================")
    
    confirm = input("Type 'RESET' to confirm: ").strip()
    
    if confirm != "RESET":
        print("❌ Action Cancelled.")
        time.sleep(2)
        return

    print("\n🧹 Cleaning system...")
    
    for name, path in TARGETS.items():
        if os.path.exists(path):
            try:
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                    os.makedirs(path) # Recreate empty folder
                print(f"✅ Deleted {name}")
            except PermissionError:
                print(f"❌ ERROR: Cannot delete {path}. Please close the Main App first!")
            except Exception as e:
                print(f"❌ ERROR: {e}")
        else:
            if os.path.isdir(path) or "." not in path:
                os.makedirs(path, exist_ok=True)
                print(f"✅ Created clean {name} folder")
            else:
                print(f"ℹ️  {name} was already clean")

    print(f"\n✨ {SYSTEM_NAME} is ready for a new batch!")
    input("Press Enter to exit...")

if __name__ == "__main__":
    clean_system()