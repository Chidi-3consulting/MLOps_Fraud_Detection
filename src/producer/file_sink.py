import os
import io
import json
import time
import csv
from typing import Optional, List


class FileSink:
    """Simple local file sink for JSONL or CSV with size-based rotation.

    Usage:
        sink = FileSink(path='out/transactions.jsonl', fmt='jsonl', max_bytes=10*1024*1024)
        sink.write(transaction_dict)
    """

    def __init__(self, path: str, fmt: str = 'jsonl', max_bytes: int = 10 * 1024 * 1024):
        self.path = path
        self.fmt = fmt.lower()
        self.max_bytes = int(max_bytes)
        self._ensure_dir()
        self._open_file()

    def _ensure_dir(self):
        d = os.path.dirname(self.path)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

    def _open_file(self):
        # open file depending on format
        # For JSONL we use binary append; for CSV use text append with newline handling
        # Always open text mode for simplicity and consistent writes
        existed = os.path.exists(self.path) and os.path.getsize(self.path) > 0
        self.fp = open(self.path, 'a', newline='', encoding='utf-8')
        if self.fmt == 'jsonl':
            self.csv_fieldnames = None
            self.csv_writer = None
        else:
            # CSV path
            self.csv_writer = csv.writer(self.fp)
            # csv_fieldnames is preserved between rotations; if file is new and we already
            # know the fieldnames, write header now
            if not existed and getattr(self, 'csv_fieldnames', None):
                try:
                    if self.csv_writer is not None:
                        fn = list(self.csv_fieldnames) if self.csv_fieldnames else []
                        if fn:
                            self.csv_writer.writerow(fn)
                            self.fp.flush()
                except Exception:
                    pass

    def _should_rotate(self) -> bool:
        try:
            self.fp.flush()
            size = os.path.getsize(self.path)
            return size >= self.max_bytes
        except Exception:
            return False

    def _rotate(self):
        try:
            self.fp.close()
        except Exception:
            pass
        ts = time.strftime('%Y%m%d-%H%M%S')
        new_name = f"{self.path}.{ts}"
        try:
            os.rename(self.path, new_name)
        except Exception:
            # fallback: copy content-less rotate
            pass
        self._open_file()

    def write(self, record: dict):
        """Write a single record to the sink.

        For JSONL, writes JSON per line. For CSV, writes a JSON-encoded row as fallback.
        """
        try:
            if self.fmt == 'jsonl':
                line = json.dumps(record, default=str, ensure_ascii=False) + '\n'
                self.fp.write(line)
                # flush so data is durable for tests
                self.fp.flush()
            else:
                # CSV: write header on first write if needed, and preserve field order
                if getattr(self, 'csv_fieldnames', None) is None:
                    # Define fieldnames from record keys in deterministic order
                    self.csv_fieldnames = list(record.keys())
                    try:
                        if self.csv_writer is not None:
                            fn = list(self.csv_fieldnames)
                            if fn:
                                self.csv_writer.writerow(fn)
                    except Exception:
                        pass
                # create row in the same order
                fn = list(self.csv_fieldnames) if self.csv_fieldnames else list(record.keys())
                row = [self._serialize_value(record.get(k)) for k in fn]
                try:
                    if self.csv_writer is not None:
                        self.csv_writer.writerow(row)
                except Exception:
                    # fallback: write raw JSON if csv writer fails
                    self.fp.write(json.dumps(record, default=str, ensure_ascii=False) + '\n')
                self.fp.flush()

            if self._should_rotate():
                self._rotate()

        except Exception:
            # Don't raise from sink to avoid breaking producer; caller should log if needed
            try:
                # attempt to reopen on next write
                self._open_file()
            except Exception:
                pass

    def close(self):
        try:
            self.fp.close()
        except Exception:
            pass

    def _serialize_value(self, value):
        if value is None:
            return ''
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return str(value)
        return value
