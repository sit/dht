def str2bigint(s):
    a = map(ord, s)
    v = 0L
    for d in a:
	v = v << 8
	v = v | d
    return v

class bigint(long):
    def __new__(cls, val = None):
	if isinstance(val, str):
	    return long.__new__(cls, str2bigint(val))
	else:
	    return long.__new__(cls, val)

    def __hex__(self):
	return long.__hex__(self).lower()[2:-1]
    def __str__(self):
	return hex(self)

def pack_bigint(p, v):
    a = []
    while v > 0:
	a.append(chr(v & 0xFF))
	v = v >> 8
    # Ensure that remote end will decode as posititive
    if len(a) and ord(a[-1]) & 0x80:
	a.append(chr(0))
    # Pad out to multiple of 4 bytes.
    while len(a) % 4 != 0:
	a.append(chr(0))
    a.reverse()
    p.pack_opaque(''.join(a))

def unpack_bigint(u):
    s = u.unpack_opaque()
    return bigint(s)
