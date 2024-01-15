'''
This is the utils for handling the exception raised in dtable-events
'''

# Exception for dtable-io
class DTableIOException(Exception):
    pass



class BaseSizeExceedsLimitError(DTableIOException):

    def __str__(self):
        return "The base size exceeds the limits."
