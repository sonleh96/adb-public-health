def separate_into_breakpoints(total, step):
    """
    Separate a number into equal breakpoints of 20.
    
    :param total: The total number to be separated.
    :return: A list of breakpoints.
    """
    breakpoints = list(range(0, total + 1, step))
    
    # Ensure the last breakpoint is exactly the total if it's not already included
    if breakpoints[-1] != total:
        breakpoints.append(total)
    
    return breakpoints
