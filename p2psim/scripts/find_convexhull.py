#!/usr/bin/env python

"""find_convexhull.py

"""


import sys, convexhull, re, string

# need to get a set of points
if __name__ == '__main__':
    try:
        data = sys.argv[1]
    except IndexError:
        data = "sample.eps" #die?

    # read the file
    f=open(data, 'r')

    P = []
    l = f.readline()
    while l != "":
      #print l
      s = re.split( '\s+', l, 2)
      if s[0] != "#" and s[1] != "":
        P.append((string.atof(s[0]), string.atof(s[1])))
      l = f.readline()


    f.close()

    H = convexhull.convexHull(P)

    for p in H:
#	print '%f,%f' % (p[0],p[1])
	print p[0], p[1]
