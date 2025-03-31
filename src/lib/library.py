import logging
import os
import sqlite3
import json
import shutil


class Library:
  def __init__(self, config):
    self.config = config
    self.library_db_path = os.path.expanduser(
        self.config.config.get("LIBRARY", "db")
    )
    self.library_paths = [
        os.path.expanduser(p)
        for p in json.loads(self.config.config.get("LIBRARY", "paths"))
    ]
    self.conn = None
    self.supported_formats = [
        ".epub",
        ".mobi",
        ".pdf",
        ".azw3",
        ".cbz",
        ".zip",
        ".rar",
    ]
    self.transferable_formats = [
        f".{f}"
        for f in json.loads(
            self.config.config.get("LIBRARY", "transferable_formats")
        )
    ]

  def connect(self):
    """Connects to the library database. Initializes if the database is new."""
    try:
      # Ensure the directory exists
      db_dir = os.path.dirname(self.library_db_path)
      if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

      # Check if the database file exists
      db_exists = os.path.exists(self.library_db_path)

      self.conn = sqlite3.connect(self.library_db_path)
      logging.info(f"Connected to library database: {self.library_db_path}")

      # If the database is newly created, initialize it
      if not db_exists:
        logging.info(f"New library database created. Initializing...")
        if not self.initialize():
          return False

      return True
    except sqlite3.Error as e:
      logging.error(f"Error connecting to library database: {e}")
      self.conn = None
      return False

  def initialize(self):
    """Initializes the library database."""
    logging.info("Library database initialized.")
    queries = [
        """
        CREATE TABLE IF NOT EXISTS books (
          file_path TEXT,
          file_name TEXT,
          file_extension TEXT,
          read BOOLEAN DEFAULT FALSE,
          deleted BOOLEAN DEFAULT FALSE,
          transferable BOOLEAN DEFAULT FALSE,
          UNIQUE(file_path, file_name, file_extension)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_books_read ON books(read);",
        "CREATE INDEX IF NOT EXISTS idx_books_path_name ON books(file_path, file_name);",
        "CREATE INDEX IF NOT EXISTS idx_books_deleted ON books(deleted);",
        "CREATE INDEX IF NOT EXISTS idx_books_transferable ON books(transferable);",
    ]
    for query in queries:
      if not self.execute_query(query):
        return False
    return True

  def disconnect(self):
    """Disconnects from the library database."""
    if self.conn:
      self.conn.close()
      self.conn = None
      logging.info("Disconnected from library database.")
      return True
    return False

  def execute_query(self, query, params=None):
    """Executes a query on the library database."""
    if not self.conn:
      logging.error("Not connected to library database.")
      return False
    try:
      cursor = self.conn.cursor()
      if params:
        cursor.execute(query, params)
      else:
        cursor.execute(query)

      if query.lstrip().upper().startswith("SELECT"):
        return cursor.fetchall()
      else:
        self.conn.commit()
        return cursor.rowcount
    except sqlite3.Error as e:
      logging.error(f"Error executing query: {e}")
      return False

  def scan_library(self):
    """Scans the library paths for ebook files and adds/updates them in the database."""
    logging.info("Scanning library paths for ebook files...")

    # Get all books from the database
    all_books = self.get_all_books()
    processed_books = set()  # Track processed books to avoid duplicates

    for library_path in self.library_paths:
      expanded_path = os.path.expanduser(library_path)
      if not os.path.exists(expanded_path):
        logging.warning(f"Library path does not exist: {expanded_path}")
        continue

      for root, _, files in os.walk(expanded_path):
        for file in files:
          file_lower = file.lower()
          file_name = os.path.splitext(file)[0]
          file_extension = os.path.splitext(file)[1][1:]
          full_file_path = os.path.join(
              root, file_name + "." + file_extension
          )
          book_key = (root, file_name)

          if file_lower.endswith(tuple(self.supported_formats)):
            transferable = file_extension.lower() in [
                f[1:] for f in self.transferable_formats
            ]
            if os.path.exists(full_file_path):
              if book_key not in processed_books:
                if book_key in all_books:
                  all_books.remove(book_key)
                if self.add_or_update_book(
                    root, file_name, file_extension, transferable
                ):
                  logging.info(
                      f"Added book to library: {full_file_path}"
                  )
                processed_books.add(book_key)
            else:
              if book_key in all_books:
                result = self.execute_query(
                    "SELECT deleted FROM books WHERE file_path = ? AND file_name = ?",
                    (root, file_name),
                )
                if result and result[0][0] == False:
                  logging.warning(
                      f"Book not found on disk: {full_file_path}"
                  )
                self.execute_query(
                    "UPDATE books SET deleted = TRUE WHERE file_path = ? AND file_name = ?",
                    (root, file_name),
                )

    # Books that remain in all_books do not exist on disk
    for file_path, file_name in all_books:
      # Check if the book is already marked as deleted
      result = self.execute_query(
          "SELECT deleted FROM books WHERE file_path = ? AND file_name = ?",
          (file_path, file_name),
      )
      if result and result[0][0] == False:
        logging.warning(
            f"Book not found on disk: {os.path.join(file_path, file_name)}"
        )
      self.execute_query(
          "UPDATE books SET deleted = TRUE WHERE file_path = ? AND file_name = ?",
          (file_path, file_name),
      )

    logging.info("Finished scanning library paths.")
    return True

  def add_or_update_book(self, file_path, file_name, file_extension, transferable):
    """Adds a book to the library database or updates its status if it exists."""
    try:
      query = """
        INSERT OR IGNORE INTO books (file_path, file_name, file_extension, deleted, transferable)
        VALUES (?, ?, ?, FALSE, ?)
      """
      rows_affected = self.execute_query(
          query, (file_path, file_name, file_extension, transferable)
      )

      # If the book already exists, set deleted to FALSE
      if rows_affected == 0:
        result = self.execute_query(
            "SELECT deleted FROM books WHERE file_path = ? AND file_name = ?",
            (file_path, file_name),
        )
        if result and result[0][0] == True:
          logging.info(
              f"Book restored: {os.path.join(file_path, file_name + '.' + file_extension)}"
          )
        self.execute_query(
            "UPDATE books SET deleted = FALSE, transferable = ? WHERE file_path = ? AND file_name = ?",
            (transferable, file_path, file_name),
        )

      return rows_affected > 0
    except Exception as e:
      logging.error(
          f"Error adding/updating book {os.path.join(file_path, file_name + '.' + file_extension)}: {e}"
      )
      return False

  def get_all_books(self):
    """Gets all books from the database."""
    query = "SELECT file_path, file_name FROM books"
    result = self.execute_query(query)
    if result is not False:
      return set(tuple(row) for row in result)
    return set()

  def get_all_books_with_details(self, only_transferable=False):
    """Gets all books from the database with full details."""
    if only_transferable:
      query = "SELECT file_path, file_name, file_extension FROM books WHERE deleted = FALSE AND transferable = TRUE"
    else:
      query = "SELECT file_path, file_name, file_extension FROM books WHERE deleted = FALSE"
    result = self.execute_query(query)
    if result is not False:
      return [tuple(row) for row in result]
    return []

  def get_book_full_path(self, file_path, file_name, file_extension):
    """Gets the full path of a book."""
    return os.path.join(file_path, file_name + "." + file_extension)

  def get_book_size(self, file_path, file_name, file_extension):
    """Gets the size of a book file."""
    full_path = self.get_book_full_path(file_path, file_name, file_extension)
    if os.path.exists(full_path):
      return os.path.getsize(full_path)
    return 0

  def mark_book_as_read(self, file_path, file_name, file_extension):
    """Marks a book as read in the local library database."""
    query = "UPDATE books SET read = TRUE WHERE file_path = ? AND file_name = ? AND file_extension = ?"
    return self.execute_query(
        query, (file_path, file_name, file_extension)
    )
