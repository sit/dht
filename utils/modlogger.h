#ifndef _MODLOGGER_H_
#define _MODLOGGER_H_

#include <str.h>

// Concept stolen from libasync's err.[Ch]
class modlogger : public strbuf {
 private:
  static int logfd;
  static int maxprio;

  int prio;
  
 public:
  enum priority {
    CRIT  = -1,
    WARNING  = 0,
    INFO  = 1,
    TRACE = 2
  };
  explicit modlogger (const char *module, int prio = INFO);
  ~modlogger ();
  const modlogger &operator () (const char *fmt, ...) const
    __attribute__ ((format (printf, 2, 3)));

  static void setlogfd (int fd) { logfd = fd; }
  static void setmaxprio (int p) { maxprio = p; }
};

#endif /* !_MODLOGGER_H_ */
