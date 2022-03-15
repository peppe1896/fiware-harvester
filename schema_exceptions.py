class Schema_exception(Exception):

    def __init__(self, error_msg):
        self._message = error_msg

    def __str__(self):
        return "\033[31m"+"Raised schema_exception:\n\t"+self._message + "\033[30m"

    def get_error(self):
        return self._message