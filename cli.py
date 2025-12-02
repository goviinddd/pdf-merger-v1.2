import sys
import logging
import argparse
import time
from pathlib import Path

# Ensure Python finds the 'src' module
sys.path.append(str(Path(__file__).parent))

# --- REMOVE IMPORTS FROM HERE ---

def setup_logging(debug_mode: bool):
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
    logging.getLogger("pdfminer").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("ultralytics").setLevel(logging.WARNING) # Clean up YOLO logs

def main():
    parser = argparse.ArgumentParser(description="Automated PDF Merger V1")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Sleep interval")
    
    args = parser.parse_args()

    # 1. Setup Environment FIRST
    setup_logging(args.debug)
    logger = logging.getLogger(__name__)
    
    # 2. Import Modules AFTER logging is setup
    # This ensures we see the "YOLO loaded" messages
    from src.core.pipeline import PipelineOrchestrator

    logger.info("="*50)
    logger.info("   AUTOMATED PDF MERGER SYSTEM V1.1 (YOLO)   ")
    logger.info("="*50)

    try:
        orchestrator = PipelineOrchestrator()
        
        if args.loop:
            logger.info(f"Starting DAEMON mode...")
            while True:
                orchestrator.run()
                time.sleep(args.interval)
        else:
            logger.info("Starting SINGLE PASS mode.")
            orchestrator.run()
            logger.info("Run Complete. Exiting.")

    except KeyboardInterrupt:
        logger.info("STOP command received.")
    except Exception as e:
        logger.critical(f"Fatal System Crash: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()