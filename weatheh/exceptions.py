class GroundhogError(Exception):
    pass


class GroundhogDownloadError(GroundhogError):
    def __init__(self, message):
        super().__init__(message)
