import logging

from lib.config import Config
from lib.kobodevice import KoboDevice
from lib.library import Library
from lib.transfermanager import TransferManager


class KoboManager:
    def __init__(self, config_dir):
        self.config = Config(config_dir)
        self.device = KoboDevice(self.config)
        self.library = Library(self.config)
        self.transfer_manager = TransferManager(self.config, self.library, self.device)

    def run(self):
        logging.info("Starting KoboManager...")
        if not self.device.check_device_path():
            return 1
        if not self.device.check_device_db():
            return 1
        if not self.device.check_sdcard_path():
            return 1
        if not self.device.create__sdcard_kobomanager_directory():
            return 1
        if not self.device.connect():
            return 1
        if not self.library.connect():
            return 1
        if not self.library.scan_library():
            return 1

        # Transfer books
        self.transfer_manager.transfer_books()

        # Check for read books and update the local library and delete file from sdcard
        books = self.library.get_all_books_with_details()
        for file_path, file_name, file_extension in books:
            self.device.mark_book_as_read_in_kobo(self.library, file_path, file_name, file_extension)

        if not self.device.disconnect():
            return 1
        if not self.library.disconnect():
            return 1

        logging.info("KoboManager finished.")
        return 0
