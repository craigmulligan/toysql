from pathlib import Path
import os
from toysql.page import Page, PageType
from toysql.exceptions import PageNotFoundException

PageNumber = int


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

        if self.is_corrupt():
            raise Exception(f"{file_path} is corrupted")

    def is_corrupt(self) -> bool:
        """
        Checks if the current file is not exact page size * blocks.
        """
        return self.size() % self.page_size != 0

    def new(self) -> PageNumber:
        """
        Requests a new page
        """
        page_number = len(self)
        page = Page(PageType.leaf, page_number, page_size=self.page_size)
        self.write(page)

        return page_number

    def read(self, page_number: PageNumber) -> Page:
        if page_number is None or page_number >= len(self):
            raise PageNotFoundException(f"page_number: {page_number} not found")

        self.f.seek(page_number * self.page_size)
        return Page.from_bytes(self.f.read(self.page_size))

    def write(self, page: Page):
        self.f.seek(page.page_number * self.page_size)
        self.f.write(page.to_bytes())
        self.f.flush()

        return page

    def __len__(self) -> int:
        current = self.f.tell()
        size = self.size()
        l = int(size / self.page_size)
        self.f.seek(current)
        return l

    def size(self) -> float:
        self.f.seek(0, os.SEEK_END)
        return self.f.tell()
