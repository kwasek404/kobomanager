#!/usr/bin/env python3

import logging
import sys
import signal
import argparse
import os

from lib.kobomanager import KoboManager  # Import from the lib package

def setup_logging(log_level=logging.INFO):
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
        stream=sys.stdout,
    )

def signal_handler(sig, frame):
    logging.info("Closing app...")
    sys.exit(0)

def parse_arguments():
    parser = argparse.ArgumentParser(description="KoboManager Application")
    parser.add_argument(
        "--config",
        default=os.path.expanduser("~/.config/kobomanager/"), # Changed default path
        help="Path to the configuration directory", # Changed help message
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser.parse_args()

def main():
    args = parse_arguments()
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level)
    signal.signal(signal.SIGINT, signal_handler)

    config_dir = args.config # Changed to config_dir
    app = KoboManager(config_dir) # Changed to config_dir
    return app.run()

if __name__ == "__main__":
    sys.exit(main())
