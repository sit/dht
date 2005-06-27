%#include <chord_types.h>

struct noauth_log_entry {
  opaque data<>;
};

struct noauth_block {
  noauth_log_entry blocks<>;
};


