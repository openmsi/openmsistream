" Various message helpers "
def get_message_length(msg):
    #
    # This accounting seems imprecise and incomplete.
    #
    keylen = 0
    vallen = 0
    if hasattr(msg, "key"):
        try:
            keylen = len(bytes(msg.key))
        except TypeError:
            keylen = len(msg.key)
    if hasattr(msg, "value"):
        try:
            vallen = len(bytes(msg.value))
        except TypeError:
            vallen = len(msg.value)
    if keylen == 0 and vallen == 0:
        return len(msg)
    else:
        return keylen + vallen

