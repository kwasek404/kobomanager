import os
import shutil
import logging
import json
import zipfile
import rarfile
from pathlib import Path
import time


class TransferManager:
    def __init__(self, config, library, device):
        self.config = config
        self.library = library
        self.device = device
        self.sdcard_path = self.device.sdcard_path
        self.transferable_formats = [
            f".{f}"
            for f in json.loads(
                self.config.config.get("LIBRARY", "transferable_formats")
            )
        ]

    def _bytes_to_mb(self, bytes_value):
        """Converts bytes to megabytes."""
        return round(bytes_value / (1024 * 1024), 2)

    def get_available_space(self):
        """Gets the available space on the SD card."""
        try:
            statvfs = os.statvfs(self.device.device_sdcard)
            available_space_bytes = statvfs.f_frsize * statvfs.f_bavail
            available_space_mb = self._bytes_to_mb(available_space_bytes)
            logging.debug(f"Available space on SD card: {available_space_mb} MB")
            return available_space_bytes
        except FileNotFoundError:
            logging.error(f"SD card path not found: {self.device.device_sdcard}")
            return 0
        except Exception as e:
            logging.error(f"Error getting available space: {e}")
            return 0

    def extract_archive(
        self, archive_path, archive_name, archive_extension, destination_dir
    ):
        """Extracts files from a ZIP or RAR archive to the SD card."""
        try:
            if archive_extension == "zip":
                with zipfile.ZipFile(
                    os.path.join(
                        archive_path, archive_name + "." + archive_extension
                    ),
                    "r",
                ) as zip_ref:
                    for member in zip_ref.namelist():
                        destination_path = os.path.join(destination_dir, member)
                        os.makedirs(
                            os.path.dirname(destination_path), exist_ok=True
                        )
                        with zip_ref.open(member) as source, open(
                            destination_path, "wb"
                        ) as target:
                            shutil.copyfileobj(source, target)
            elif archive_extension == "rar":
                with rarfile.RarFile(
                    os.path.join(
                        archive_path, archive_name + "." + archive_extension
                    ),
                    "r",
                ) as rar_ref:
                    for member in rar_ref.namelist():
                        destination_path = os.path.join(destination_dir, member)
                        os.makedirs(
                            os.path.dirname(destination_path), exist_ok=True
                        )
                        with rar_ref.open(member) as source, open(
                            destination_path, "wb"
                        ) as target:
                            shutil.copyfileobj(source, target)
            logging.info(
                f"Extracted archive: {os.path.join(archive_path, archive_name + '.' + archive_extension)} to {destination_dir}"
            )
            return True
        except (zipfile.BadZipFile, rarfile.Error) as e:
            logging.error(
                f"Error extracting archive {os.path.join(archive_path, archive_name + '.' + archive_extension)}: {e}"
            )
            return False

    def transfer_book(self, file_path, file_name, file_extension, destination_dir):
        """Transfers a single book to the SD card, preserving directory structure."""
        full_file_path = self.library.get_book_full_path(
            file_path, file_name, file_extension
        )

        try:
            os.makedirs(destination_dir, exist_ok=True)
            if file_extension.lower() in ["zip", "rar"]:
                if not self.extract_archive(
                    file_path,
                    file_name,
                    file_extension,
                    destination_dir,
                ):
                    return False
            else:
                destination_path = os.path.join(
                    destination_dir, file_name + "." + file_extension
                )
                shutil.copy2(full_file_path, destination_path)
            logging.info(f"Transferred: {full_file_path} to {destination_dir}")
            return True
        except Exception as e:
            logging.error(f"Error transferring {full_file_path}: {e}")
            return False

    def transfer_books(self):
        """Transfers books from the library to the SD card, checking space per directory."""
        books = self.library.get_all_books_with_details(only_transferable=True)
        available_space = self.get_available_space()

        # Group books by directory
        books_by_directory = {}
        for file_path, file_name, file_extension in books:
            # Find correct library path
            correct_library_path = None
            for lib_path in self.library.library_paths:
                if file_path.startswith(lib_path):
                    correct_library_path = lib_path
                    break
            if correct_library_path is None:
                logging.error(f"File path {file_path} is not in any library path.")
                continue
            relative_path = Path(file_path).relative_to(correct_library_path)
            destination_dir = os.path.join(self.sdcard_path, relative_path)
            if destination_dir not in books_by_directory:
                books_by_directory[destination_dir] = []
            books_by_directory[destination_dir].append(
                (file_path, file_name, file_extension)
            )

        for destination_dir, books_in_dir in books_by_directory.items():
            directory_size = 0
            for file_path, file_name, file_extension in books_in_dir:
                directory_size += self.library.get_book_size(
                    file_path, file_name, file_extension
                )

            if directory_size > available_space:
                logging.warning(
                    f"Not enough space on SD card for directory: {destination_dir}. Required: {self._bytes_to_mb(directory_size)} MB, Available: {self._bytes_to_mb(available_space)} MB. Skipping directory."
                )
                continue

            # Recheck available space before each book transfer
            available_space = self.get_available_space()
            if available_space < min(
                self.library.get_book_size(file_path, file_name, file_extension)
                for file_path, file_name, file_extension in books_in_dir
            ):
                logging.warning(
                    f"Not enough space on SD card for any book in directory: {destination_dir}. Available: {self._bytes_to_mb(available_space)} MB. Skipping directory."
                )
                continue

            for file_path, file_name, file_extension in books_in_dir:
                # Check if book exists and is unread before transfer
                if self.device.book_exists_and_unread(
                    self.library, file_path, file_name, file_extension
                ):
                    logging.info(
                        f"Skipping already existing or read book: {self.library.get_book_full_path(file_path, file_name, file_extension)}"
                    )
                    continue

                # Recheck available space before each book transfer
                current_book_size = self.library.get_book_size(
                    file_path, file_name, file_extension
                )
                if current_book_size > available_space:
                    logging.warning(
                        f"Not enough space on SD card for book: {file_name}. Required: {self._bytes_to_mb(current_book_size)} MB, Available: {self._bytes_to_mb(available_space)} MB. Skipping book."
                    )
                    continue

                if file_extension.lower() not in [
                    f[1:] for f in self.transferable_formats
                ]:
                    logging.info(
                        f"Skipping file with unsupported format: {self.library.get_book_full_path(file_path, file_name, file_extension)}"
                    )
                    continue

                if self.transfer_book(
                    file_path, file_name, file_extension, destination_dir
                ):
                    # Wait 1 second, to make sure that the filesystem has time to update
                    time.sleep(1)
                    available_space = self.get_available_space()

        logging.info("Finished transferring books.")
