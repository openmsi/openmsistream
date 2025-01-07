" Various message helpers "


def get_message_length(msg):
    """
    returns length of a message
    """
    #
    # This accounting seems imprecise and incomplete.
    #
    keylen = 0
    vallen = 0
    totlen = 0
    if hasattr(msg, "key"):
        try:
            keylen = len(bytes(msg.key))
        except TypeError:
            try:
                keylen = len(msg.key)
            except TypeError:
                pass
    if hasattr(msg, "value"):
        try:
            vallen = len(bytes(msg.value))
        except TypeError:
            try:
                vallen = len(msg.value)
            except TypeError:
                pass
    try:
        totlen = len(bytes(msg))
    except TypeError:
        try:
            totlen = len(msg)
        except TypeError:
            pass
    return max(keylen + vallen, totlen)
