#ifndef INCL_DEF
#define INCL_DEF

#ifndef MAX_INT
#define MAX_INT  0x7fffffff
#endif

#ifndef TRUE
#define TRUE  1
#define FALSE 0
#endif

typedef int ID;

#define OPTIMIZATION

#define NUM_BITS     24 /* 24 */

#define AVG_PKT_DELAY    50  /* in ms */
#define PROC_REQ_PERIOD  500
#define STABILIZE_PERIOD 30000
#define LEAVE_WAIT       (STABILIZE_PERIOD << 1)

/* maximum simulation time */
#define MAX_TIME     5e+07 /* in ms */   

/* used in node.c */
#define POWER_HASH_TABLE 10
#define HASH_SIZE        100000

/* used in event.c */
#define MAX_NUM_ENTRIES  4096
#define ENTRY_TUNIT 100 /* msec */ 

#define MAX_NUM_DOCS    1000
#define MAX_NUM_FINGERS 20

#endif 





