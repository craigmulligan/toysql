from pathlib import Path
import os


class Pager:
    """
    Abstracts a file into pages
    This allows you to get and set pages (chunks) of data.
    """

    def __init__(self, file_path: str, page_size=4096):
        file_name = Path(file_path)
        file_name.touch(exist_ok=True)
        self.f = open(file_name, "rb+")
        self.page_size = page_size
        self.num_pages = len(self)

        # TODO check for corrupt file.
        # file_length % PAGE_SIZE != 0

    def read(self, i: int) -> bytes:
        if i >= self.num_pages:
            self.num_pages += 1
        self.f.seek(i * self.page_size)
        page = bytearray(self.f.read(self.page_size))
        return page

    def write(self, page_number: int, page: bytearray):
        self.f.seek(page_number * self.page_size)
        self.f.write(page)
        self.f.flush()
        return page

    def __len__(self):
        return int(self.__sizeof__() / self.page_size)

    def __sizeof__(self):
        self.f.seek(0, os.SEEK_END)
        return self.f.tell()
