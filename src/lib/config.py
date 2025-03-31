import configparser
import logging
import os
import sys
import getpass
import glob

class Config:
    DEFAULT_CONFIG = {
        "DEVICE": {
            "path": "/run/media/{current_user}/KOBOeReader",
            "db": ".kobo/KoboReader.sqlite",
            "sdcard": "/run/media/{current_user}/23E1-32E8"
        },
        "LIBRARY": {
            "db": "~/.config/kobomanager/kobomanager.sqlite",
            "paths": [
                '"~/Documents"'
            ],
            "transferable_formats": [
                '"epub"',
                '"mobi"',
                '"azw3"',
                '"cbz"',
                '"pdf"'
            ]
        }
    }
    config_filename = "kobomanager.conf"

    def __init__(self, config_dir):
        self.config_dir = config_dir
        self.config_path = os.path.join(self.config_dir, self.config_filename)
        self.config = configparser.ConfigParser()
        self.current_user = getpass.getuser()
        self.load_config()

    def load_config(self):
        """Loads the configuration from the file, creating a default one if it doesn't exist."""
        if not os.path.isdir(self.config_dir):
            logging.warning(f"Config directory not found: {self.config_dir}. Creating it.")
            os.makedirs(self.config_dir, exist_ok=True)

        if not self.check_config_file():
            logging.warning(f"Config file not found: {self.config_path}. Creating a default one.")
            self.create_default_config()

        try:
            self.config.read(self.config_path)
        except configparser.Error as e:
            logging.error(f"Error reading config file: {e}")
            sys.exit(1)

        self.check_config()

    def check_config(self):
        """Checks if the config file has all required options."""
        for section, options in self.DEFAULT_CONFIG.items():
            if section not in self.config:
                logging.error(f"Missing section '{section}' in config file: {self.config_path}")
                sys.exit(1)
            for option in options:
                if option not in self.config[section]:
                    logging.error(f"Missing option '{option}' in section '{section}' in config file: {self.config_path}")
                    sys.exit(1)

    def create_default_config(self):
        """Creates a default configuration file."""
        self.config.read_dict(self.DEFAULT_CONFIG)
        # Removed: self.config["KOBOMANAGER"]["db"] = os.path.join(self.config_dir, self.config["KOBOMANAGER"]["db"])
        # Now the db path is relative, as it's just "kobomanager.sqlite"


        # Try to find the device path dynamically
        device_path = self.find_kobo_device_path()
        if device_path:
            self.config["DEVICE"]["path"] = device_path
        else:
            self.config["DEVICE"]["path"] = self.config["DEVICE"]["path"].format(current_user=self.current_user)

        # Try to find the sdcard path dynamically
        sdcard_path = self.find_sdcard_path()
        if sdcard_path:
            self.config["DEVICE"]["sdcard"] = sdcard_path
        else:
            self.config["DEVICE"]["sdcard"] = self.config["DEVICE"]["sdcard"].format(current_user=self.current_user)

        try:
            with open(self.config_path, "w") as configfile:
                self.config.write(configfile)
        except OSError as e:
            logging.error(f"Error creating config file: {e}")
            sys.exit(1)

    def find_kobo_device_path(self):
        """Attempts to find the Kobo device's mount point."""
        # Common mount point patterns
        patterns = [
            f"/run/media/{self.current_user}/KOBOeReader/.kobo",  # Most common
            f"/media/{self.current_user}/KOBOeReader/.kobo",  # Another common pattern
        ]
        for pattern in patterns:
            matches = glob.glob(pattern)
            if matches:
                return os.path.dirname(matches[0])  # Return the parent directory
        return None

    def find_sdcard_path(self):
        """Attempts to find the SD card's mount point."""
        # Common mount point patterns for SD cards
        patterns = [
            f"/run/media/{self.current_user}/*-*",  # Most common
            f"/media/{self.current_user}/*-*",  # Another common pattern
        ]
        for pattern in patterns:
            matches = glob.glob(pattern)
            if matches:
                # Assuming the first match is the SD card
                return matches[0]
        return None

    def check_config_file(self):
        """Checks if the config file exists."""
        return os.path.exists(self.config_path)
