#include <sys/time.h>
#include <assert.h>
#include <chord.h>
#include <chord_util.h>

#define MAX_INT 0x7fffffff


str 
gettime()
{
  str buf ("");
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  buf = strbuf (" %d.%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));
  return buf;
}

int 
uniform_random(double a, double b)
{
  double f;
  int c = random();

  if (c == MAX_INT) c--;
  f = (b - a)*((double)c)/((double)MAX_INT);

  return (int)(a + f);
}

sfs_ID
incID (sfs_ID &n)
{
  sfs_ID s = n + 1;
  sfs_ID b (1);
  b = b << NBIT;
  if (s >= b)
    return s - b;
  else
    return s;
}

sfs_ID
decID (sfs_ID &n)
{
  sfs_ID p = n - 1;
  sfs_ID b (1);
  b = b << NBIT;
  if (p < 0)
    return p + b;
  else
    return p;
}

sfs_ID
successorID (sfs_ID &n, int p)
{
  sfs_ID s;
  sfs_ID t (1);
  sfs_ID b (1);
  
  b = b << NBIT;
  s = n;
  sfs_ID t1 = t << p;
  s = s + t1;
  if (s >= b)
    s = s - b;
  return s;
}

sfs_ID
predecessorID (sfs_ID &n, int p)
{
  sfs_ID s;
  sfs_ID t (1);
  sfs_ID b (1);
  
  b = b << NBIT;
  s = n;
  sfs_ID t1 = t << p;
  s = s - t1;
  if (s < 0)
    s = s + b;
  return s;
}

// XXX use operator overloading?
bool
gt_or_eq (sfs_ID &n, sfs_ID &n1)
{
  sfs_ID b (1);
  b = (b << NBIT);
  if (((n - n1) >= 0) && ((n - n1) < b))
    return true;
  if (((n - n1) <= 0) && ((n1 - n) > b))
    return true;
  return false;
}

bool
gt (sfs_ID &n, sfs_ID &n1)
{
  sfs_ID b (1);
  b = (b << NBIT);
  if (((n - n1) > 0) && ((n - n1) < b))
    return true;
  if (((n - n1) < 0) && ((n1 - n) > b))
    return true;
  return false;
}

#if 0
// Check whether n in (a,b) on the circle.
bool
between (sfs_ID &a, sfs_ID &b, sfs_ID &n)
{
  bool r;
  bool f = gt_or_eq (b, a);
  if ((!f && (gt_or_eq (b, n) || gt_or_eq (n, a)))
      || (f && gt_or_eq (b, n) && gt_or_eq (n, a)))
    r = true;
  else
    r = false;
  // warnx << n << " between( " << a << ", " <<  b << "): " <<  r << "\n";
  return r;
}
#endif

// Check whether n in (a,b) on the circle.
bool
between (sfs_ID &a, sfs_ID &b, sfs_ID &n)
{

  if (b == a)  return n == a;
  bool f = (b - a) > 0;  
  bool r;
  if (f) {
    r = (n >= a) && (n <= b);
  } else {
    r = (n >= a) || (n <= b);
  }
  // warnx << n << " between( " << a << ", " <<  b << "): " <<  r << "\n";
  return r;
}

// Check whether n is (a, b) absolutely.
bool
betweenabs (sfs_ID &a, sfs_ID &b, sfs_ID &n)
{
  bool r = n >= a && b >= n;
  //  warnx << n << " between( " << a << ", " <<  b << "): " <<  r << "\n";
  return r;
}

sfs_ID
diff(sfs_ID a, sfs_ID b) 
{
  sfs_ID diff = (b - a);
  if (diff > 0) return diff;
  else return (bigint(1) << 160) - diff;
}

