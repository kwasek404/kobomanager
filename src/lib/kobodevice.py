import logging
import os
import sqlite3
from pathlib import Path


class KoboDevice:
    def __init__(self, config):
        self.config = config
        self.device_path = self.config.config.get("DEVICE", "path")
        self.device_db = self.config.config.get("DEVICE", "db")
        self.db_path = os.path.join(self.device_path, self.device_db)
        self.device_sdcard = self.config.config.get("DEVICE", "sdcard")
        self.sdcard_path = os.path.join(self.device_sdcard, "kobomanager")
        self.conn = None

    def check_sdcard_path(self):
        """Checks if the device sdcard path exists."""
        if not os.path.exists(self.device_sdcard):
            logging.error(f"Kobo device not found at: {self.device_sdcard}")
            return False
        return True

    def create__sdcard_kobomanager_directory(self):
        """Creates the kobomanager directory if it doesn't exist."""
        try:
            if not os.path.exists(self.sdcard_path):
                os.makedirs(self.sdcard_path, exist_ok=True)
                logging.info(f"Created directory: {self.sdcard_path}")
            else:
                logging.info(f"Directory already exists: {self.sdcard_path}")
            return True
        except OSError as e:
            logging.error(f"Error creating directory {self.sdcard_path}: {e}")
            return False

    def check_device_path(self):
        """Checks if the device path exists."""
        if not os.path.exists(self.device_path):
            logging.error(f"Kobo device not found at: {self.device_path}")
            return False
        return True

    def check_device_db(self):
        """Checks if the Kobo database exists."""
        if not os.path.exists(self.db_path):
            logging.error(
                f"Incompatible Kobo device. Database not found at: {self.db_path}"
            )
            return False
        return True

    def connect(self):
        """Connects to the Kobo database."""
        if not self.check_device_db():
            return False
        try:
            self.conn = sqlite3.connect(self.db_path)
            logging.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logging.error(f"Error connecting to database: {e}")
            return False

    def disconnect(self):
        """Disconnects from the Kobo database."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logging.info("Disconnected from database.")
            return True
        return False

    def execute_query(self, query, params=None):
        """Executes a query on the Kobo database."""
        if not self.conn:
            logging.error("Not connected to database.")
            return None
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error executing query: {e}")
            return None

    def get_books(self):
        """Gets all books from the Kobo database, excluding default books."""
        query = """
            SELECT ContentID, ReadStatus
            FROM content
            WHERE
                ContentType IN (1, 6, 14, 15, 16, 19) AND ContentID LIKE 'file:///mnt/sd/kobomanager/%'
        """
        return self.execute_query(query)

    def book_exists_and_unread(self, library, file_path, file_name, file_extension):
        """Checks if a book exists in the Kobo database and is unread."""
        # Find correct library path
        correct_library_path = None
        for lib_path in library.library_paths:
            if file_path.startswith(lib_path):
                correct_library_path = lib_path
                break
        if correct_library_path is None:
            logging.error(f"File path {file_path} is not in any library path.")
            return False
        relative_path = Path(file_path).relative_to(correct_library_path)
        content_id = f"file:///mnt/sd/kobomanager/{relative_path}/{file_name}.{file_extension}"
        query = """
            SELECT ReadStatus
            FROM content
            WHERE ContentID = ?
        """
        result = self.execute_query(query, (content_id,))
        if result:
            read_status = result[0][0]
            logging.info(
                f"Book {content_id} exists in Kobo database. Read status: {read_status}"
            )
            return read_status == 0  # Return True if unread (ReadStatus == 0)
        else:
            logging.info(f"Book {content_id} does not exist in Kobo database.")
            return False

    def mark_book_as_read_in_kobo(self, library, file_path, file_name, file_extension):
        """Checks if a book is marked as read in the Kobo database and updates the local library and deletes file from sdcard."""
        # Find correct library path
        correct_library_path = None
        for lib_path in library.library_paths:
            if file_path.startswith(lib_path):
                correct_library_path = lib_path
                break
        if correct_library_path is None:
            logging.error(f"File path {file_path} is not in any library path.")
            return False
        relative_path = Path(file_path).relative_to(correct_library_path)
        content_id = f"file:///mnt/sd/kobomanager/{relative_path}/{file_name}.{file_extension}"
        query = """
            SELECT ReadStatus
            FROM content
            WHERE ContentID = ?
        """
        result = self.execute_query(query, (content_id,))
        if result:
            read_status = result[0][0]
            if read_status != 0:
                logging.info(
                    f"Book {content_id} is marked as read in Kobo database. Updating local library and deleting file from sdcard."
                )
                library.mark_book_as_read(file_path, file_name, file_extension)
                # Delete the file from the SD card
                file_to_delete = os.path.join(
                    self.sdcard_path, relative_path, f"{file_name}.{file_extension}"
                )
                try:
                    if os.path.exists(file_to_delete):
                        os.remove(file_to_delete)
                        logging.info(f"Deleted file from SD card: {file_to_delete}")
                    else:
                        logging.warning(f"File not found on SD card: {file_to_delete}")
                except OSError as e:
                    logging.error(f"Error deleting file {file_to_delete}: {e}")
            else:
                logging.info(f"Book {content_id} is not marked as read in Kobo database.")
        else:
            logging.info(f"Book {content_id} not found in Kobo database.")
