import inspect


def get_caller_info():
    frame = inspect.currentframe().f_back.f_back
    caller_name = frame.f_code.co_name
    line_number = frame.f_lineno
    return caller_name, line_number
