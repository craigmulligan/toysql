class DuplicateKeyException(Exception):
    pass


class NotFoundException(Exception):
    pass


class TableFoundException(NotFoundException):
    pass


class PageNotFoundException(NotFoundException):
    pass


class LexingException(Exception):
    pass


class ParsingException(Exception):
    pass
