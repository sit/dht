#include "modlogger.h"

// Statics
int modlogger::logfd = 2;

modlogger::modlogger (char *module)
{
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  fmt ("%d.%06d ", int (ts.tv_sec), int (ts.tv_nsec/1000));
  cat (module).cat (": ");
}

const modlogger &
modlogger::operator() (const char *fmt, ...) const
{
  va_list ap;
  va_start (ap, fmt);
  vfmt (fmt, ap);
  va_end (ap);
  return *this;
}

modlogger::~modlogger ()
{
  int saved_errno = errno;
  uio->output (logfd);
  errno = saved_errno;
}
