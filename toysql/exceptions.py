class DuplicateKeyException(Exception):
    pass

class NotFoundException(Exception):
    pass

class CellNotFoundException(NotFoundException):
    pass

class PageNotFoundException(NotFoundException):
    pass


class LexingException(Exception):
    pass


class ParsingException(Exception):
    pass


class PageFullException(Exception):
    pass
