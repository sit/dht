/*
 *
 * Copyright (C) 2001 Ion Stoica (istoica@cs.berkeley.edu)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

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
      genEvent(arg1, nodeFailure, (void *)newInt(arg2), arg2);
  } else if (strcmp(cmd, "leave") == 0) {
    /* arg1 - nodeId; arg2 - time */
    if (getNode(arg1))
      genEvent(arg1, leave, (void *)newInt(arg2), arg2);
  } else if (strcmp(cmd, "netfail") == 0) {
    /* arg1 - any active node; arg2 - percentage of node failing; 
       arg3 - time */
    fscanf(fp, "%d", &arg3);
    if (getNode(arg1))
      genEvent(arg1, netFailure, (void *)newInt(arg2), arg3);
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
  } else if (strcmp(cmd, "exit") == 0) {
    /* arg1 - time */
    genEvent(0, exitSim, NULL, arg1);
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

  
