#ifndef NNTP_H
#define NNTP_H

#include <group.h>

struct nntp;

typedef struct c_jmp_entry {
  const char *cmd;
  int len;
  cbs fn;

  c_jmp_entry (const char *_cmd, cbs _fn) : cmd (_cmd), fn (_fn) {
    len = strlen (cmd);
  };
} c_jmp_entry_t;

struct nntp {
  int s;
  strbuf out;
  suio post;
  group cur_group;

  nntp (int _s);
  ~nntp ();
  void command (void);
  void output (void);
  void add_cmd (const char *, cbs);
  void cmd_hello (str);
  void cmd_list (str);
  void cmd_group (str);
  void cmd_over (str);
  void cmd_article (str);
  void cmd_post (str);
  void read_post (char *, char *);
  void cmd_quit (str);
  void cmd_help (str);
  void cmd_ihave (str);

  vec<c_jmp_entry_t> cmd_table;
};

#endif /* NNTP_H */
