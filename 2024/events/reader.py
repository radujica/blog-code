def update_latest_marker():
    # timestamp, or version, etc.
    ...


def processing_step():
    # read, process
    # write <- implement s.t. when re-triggered but the data is already processed, it will not create duplicates
    update_latest_marker()  # <- important to do after the write
