#include <async.h>
#include <keyauxdb.h>

// WARNING: This implementation does NO LOCKING and may be unsafe!
// (Fortunately, the intended use is single reader/writer in adbd.)

static const u_int32_t kasz (sizeof (keyaux_t));
keyauxdb::keyauxdb (str f) :
  filename (f), 
  fd (-1),
  base (NULL),
  baselen (0)
{
  fd = open (filename.cstr (), O_CREAT|O_RDWR|O_APPEND, 0664);
  if (fd < 0)
    fatal << "keyauxdb (" << f << "): open: " << strerror (errno) << "\n"; 
}

keyauxdb::~keyauxdb ()
{
  close (fd);
  if (base)
    free (base);
}

u_int32_t
keyauxdb::addkey (const chordID &k, u_int32_t aux)
{
  keyaux_t o;
  keyaux_marshall (k, aux, &o);
  int r = write (fd, (char *) &o, sizeof (o));
  assert (r == sizeof (o));
  off_t pos = lseek (fd, 0, SEEK_CUR);
  return pos / kasz;
}

const keyaux_t *
keyauxdb::getkeys (u_int32_t recno, u_int32_t count, u_int32_t *avail)
{
  off_t len = lseek (fd, 0, SEEK_END);
  if (len < 0)
    fatal << "keyauxdb::getkeys (" << filename << "): lseek END: " << strerror (errno) << "\n";

  if (recno >= len / kasz) {
    *avail = 0;
    return NULL;
  }
  u_int32_t a (count);
  if (recno + count > (len / kasz))
    a = (len / kasz) - recno;
  if (baselen < a * kasz) {
    // Is free+malloc cheaper than realloc?
    free (base);
    baselen = a * kasz;
    base = (keyaux_t *) malloc (baselen);
  }
  (void) lseek (fd, recno * kasz, SEEK_SET);

  u_int32_t toread = a * kasz;
  int remain (toread);
  while (remain > 0) {
    size_t sz = (remain > 16384 ? 16384 : remain);
    int rlen = read (fd, (char *) base + (toread - remain), sz);
    if (rlen < 0) {
      warn ("read: %m\n");
      break;
    }
    remain -= rlen;
    if (rlen == 0)
      break;
  }
  assert (remain == 0);
  *avail = a;
  return base;
}

void
keyauxdb::sync (void)
{
  int r = fsync (fd);
  if (r < 0)
    warn ("fsync: %m\n");
}
