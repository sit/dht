#include <sys/time.h>
#include <assert.h>
#include <chord.h>
#include <chord_util.h>

#define MAX_INT 0x7fffffff

static FILE *LOG = NULL;

void
warnt(char *msg) {
  
  if (LOG == NULL) {
    char *filename = getenv("LOG_FILE");
    if (filename == NULL) 
      LOG = fopen("/tmp/dhash.log", "a");
    else if (filename[0] == '-')
      LOG = stdout;
    else
      LOG = fopen(filename, "a");
  }
  str time = gettime(); 
  fprintf(LOG, "%s %s\n", time.cstr(), msg);

}

str 
gettime()
{
  str buf ("");
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  buf = strbuf (" %d:%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));
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

// Check whether n in (a,b) on the circle.
bool
between (sfs_ID &a, sfs_ID &b, sfs_ID &n)
{
  bool r;
  if (a == b) {
    r = 1;
  } else if (a < b) {
    r = (n > a) && (n < b);
  } else {
    r = (n > a) || (n < b);
  }
  // warnx << n << " between( " << a << ", " <<  b << "): " <<  r << "\n";
  return r;
}

bool
betweenlefincl (sfs_ID &a, sfs_ID &b, sfs_ID &n)
{
  bool r;
  if ((a == b) && (n == a)) {
    r = 1;
  } else if (a < b) {
    r = (n >= a) && (n < b);
  } else {
    r = (n >= a) || (n < b);
  }
  // warnx << n << " between( " << a << ", " <<  b << "): " <<  r << "\n";
  return r;
}

bool
betweenrightincl (sfs_ID &a, sfs_ID &b, sfs_ID &n)
{
  bool f = (b - a) > 0;  
  bool r;
  if ((a == b) && (n == a)) {
    r = 1;
  } if (a < b) {
    r = (n > a) && (n <= b);
  } else {
    r = (n > a) || (n <= b);
  }
  // warnx << n << " between( " << a << ", " <<  b << "): " <<  r << "\n";
  return r;
}

sfs_ID
diff(sfs_ID a, sfs_ID b) 
{
  sfs_ID diff = (b - a);
  if (diff > 0) return diff;
  else return (bigint(1) << 160) - diff;
}

