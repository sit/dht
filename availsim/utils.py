import random
import sha

def random_id (nbits = 20):
    return random.randrange (0, 2**nbits)

def random_interval (mean, sd):
    return max (0, int (random.gauss (mean, sd)))

def str2chordID (s):
    newID = 0L
    for c in s.lower ():
        oc = ord(c)
        if (c >= '0' and c <= '9'):
            newID = (newID << 4) | (oc - ord('0'))
        elif (c >= 'a' and c <= 'f'):
            newID = (newID << 4) | (oc - ord('a') + 10)
        else:
            raise ValueError, "Invalid character '%c'" % c
    return newID
    
def make_chordID (ip, port, vnode):
    """Create a chordID the same way that the C++ implementation does."""
    return sha.sha("%s.%d.%d" % (ip, port, vnode)).hexdigest ()

# Taken from cb's ls.py
def size_rounder(bytes):                ### express byte size as 4-char string
    K, M, G = pow(2.0,10), pow(2.0,20), pow(2.0,30)
    if   bytes <= 9999:    sz =  '%4d'    % bytes
    elif bytes < 99.5 * K: sz =  '%3.2gK' % float(bytes/K)
    elif bytes < 100  * K: sz =  '100K'
    elif bytes < 995  * K: sz =  '%3.3gK' % float(bytes/K)
    elif bytes < .995 * M: sz = ('%3.2gM' % float(bytes/M)) [1:]
    elif bytes < 1024 * K: sz =  '%3.2gM' % float(bytes/M)
    elif bytes < 99.5 * M: sz =  '%3.2gM' % float(bytes/M)
    elif bytes < 100  * M: sz =  '100M'
    elif bytes < 995  * M: sz =  '%3.3gM' % float(bytes/M)
    elif bytes < .995 * G: sz = ('%3.2gG' % float(bytes/G)) [1:]
    elif bytes < 1024 * M: sz =  '%3.2gG' % float(bytes/G)
    elif bytes < 99.5 * G: sz =  '%3.2gG' % float(bytes/G)
    elif bytes < 100  * G: sz =  '100G'
    else:                  sz =  '%3.3gG' % float(bytes/G)
    return sz
