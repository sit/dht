#!/usr/bin/env python

def fact (n):
    assert n >= 0
    if n == 0:
	return 1
    return n * fact (n - 1)

def choose (n, k):
    return fact (n) / ( fact (n - k) * fact (k) )

def ec_avail (args):
    reconstruct = int (args[0])
    total       = int (args[1])
    failprob    = float (args[2])

    prob = 0.0
    # At least reconstruct are available.
    for i in xrange (reconstruct, total+1):
	prob += choose (total, i) * ((1 - failprob) ** i) * (failprob ** (total - i))
    return prob


def main ():
    import sys
    print ec_avail (sys.argv[1:])

if __name__ == '__main__':
    main ()
