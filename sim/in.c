#include <stdio.h>
#include <ctype.h>
#include <malloc.h>

#include "incl.h"

#define EOL          0xa
#define MAX_CMD_SIZE 128

void ignoreCommentLine(FILE *fp)
{
  char ch;

  while ((ch = getc(fp)) != EOL);
}

void readLine(FILE *fp)
{
  char  cmd[MAX_CMD_SIZE];
  int   arg1, arg2, arg3, i, nodeId;
  Node *n;

  fscanf(fp, "%s %d %d", cmd, &arg1, &arg2);

  for (i = 0; i < strlen(cmd); i++) 
   cmd[i] = tolower(cmd[i]);

  if (strcmp(cmd, "join") == 0) {
    /* arg1 - nodeId; arg2 - time */
    if (arg1 >= (1 << NUM_BITS)) {
      printf("Node id=%d too large; it should be smaller than %d; Ignored\n", 
	     arg1, (1 << NUM_BITS));
      
    } else {
      nodeId = getRandomNodeId(); /* this _should_ be done before addNode */
      if (!getNode(arg1)) {
	n = addNode(arg1);
	genEvent(arg1, join, (void *)newInt(nodeId), arg2);
      } else
	printf("join ignored %d\n", arg1);
    }
  } else if (strcmp(cmd, "fail") == 0) {
    /* arg1 - nodeId; arg2 - time */
    if (getNode(arg1))
      genEvent(arg1, faultyNode, (void *)newInt(arg2), arg2);
  } else if (strcmp(cmd, "leave") == 0) {
    /* arg1 - nodeId; arg2 - time */
    if (getNode(arg1))
      genEvent(arg1, leave, (void *)newInt(arg2), arg2);
  } else if (strcmp(cmd, "find") == 0) {
    fscanf(fp, "%d", &arg3);
    /* arg1 - nodeId; arg2 - argId; arg3 - time */
    if (getNode(arg1))
      genEvent(arg1, findDocument, (void *)newInt(arg2), arg3);
  } else if (strcmp(cmd, "insert") == 0) {
    fscanf(fp, "%d", &arg3);
    /* arg1 - nodeId; arg2 - argId; arg3 - time */
    if (getNode(arg1))
      genEvent(arg1, insertDocument, (void *)newInt(arg2), arg3);
  } else {
    printf("command \"%s\" not known!\n", cmd);
    panic("");
  }

  fscanf(fp, "\n");
}

void readInputFile(char *file)
{
  FILE *fp;
  char ch;

  if ((fp = fopen(file, "r")) == NULL) {
    printf("%s: file open error.\n", file);
    panic("");
  }

  while (!feof(fp)) {
    if ((ch = getc(fp)) == '#') {
      ignoreCommentLine(fp);
      continue;
    } else
      ungetc(ch, fp);
    readLine(fp);
  }
}
  
