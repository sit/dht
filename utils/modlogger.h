#ifndef _MODLOGGER_H_
#define _MODLOGGER_H_

#include <str.h>

// Concept stolen from libasync's err.[Ch]
class modlogger : public strbuf {
 private:
  static int logfd;
  
 public:
  explicit modlogger (char *module);
  ~modlogger ();  
  const modlogger &operator () (const char *fmt, ...) const
    __attribute__ ((format (printf, 2, 3)));

  static void setlogfd (int fd) { logfd = fd; }
};

#endif /* !_MODLOGGER_H_ */
