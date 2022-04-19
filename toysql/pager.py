from pathlib import Path
import os
from toysql.exceptions import PageNotFoundException

PageNumber = int
Page = bytearray


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
        # TODO check for corrupt file.
        # file_length % PAGE_SIZE != 0

    def new(self) -> PageNumber:
        """
        Requests a new page
        """
        page = bytearray(b"".ljust(self.page_size, b"\0"))
        page_number = len(self)
        self.write(page_number, page)

        return page_number

    def read(self, page_number: PageNumber) -> Page:
        if page_number is None or page_number >= len(self):
            raise PageNotFoundException(f"page_number: {page_number} not found")

        self.f.seek(page_number * self.page_size)
        page = bytearray(self.f.read(self.page_size))

        return page

    def write(self, page_number: PageNumber, page: Page):
        self.f.seek(page_number * self.page_size)
        self.f.write(page)
        self.f.flush()

        return page

    def __len__(self) -> int:
        size = self.s()
        return int(size / self.page_size)

    def s(self) -> float:
        self.f.seek(0, os.SEEK_END)
        return self.f.tell()
