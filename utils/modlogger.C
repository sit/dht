#include "modlogger.h"

// Statics
int modlogger::logfd = 2;
int modlogger::maxprio = modlogger::INFO;

modlogger::modlogger (char *module, int p) : prio (p)
{
  /* Don't expect to be outputting, so don't make syscall */
  if (prio > maxprio)
    return;
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
  if (prio > maxprio)
    return;
  int saved_errno = errno;
  uio->output (logfd);
  errno = saved_errno;
}
