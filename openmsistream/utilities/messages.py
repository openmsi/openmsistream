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
            except:
                pass
    if keylen == 0 and vallen == 0:
        return len(msg)
    return keylen + vallen
