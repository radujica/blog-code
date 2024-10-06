def send_event():
    ...


def processing_step():
    # read, process, write
    send_event()  # <- important to do ONLY if write succeeded
