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

#ifndef INCL_TRAFFIC_GEN
#define INCL_TRAFFIC_GEN

typedef struct _nodeStructGen {
  int id;
#define PRESENT  1
#define ABSENT   0
  int status;
} NodeGen;


#define MAX_NUM_NODES 10000

NodeGen *Nodes;
int  *Docs;
int  NumNodes = 0;  /* number of nodes in teh network */
int  NumDocs  = 0;

void readInputFileGen(char *file);
void ignoreCommentLineGen(FILE *fp);
void readLineGen(FILE *fp);
void allocData(int numNodes, int numDocs);
int getNodeGen();
int insertNodeGen();
void deleteNodeGen(int nodeId);
int getDocGen();
int insertDoc();
void deleteDocGen(int docId);
void events(int numEvents, int avgEventInt, 
	    int wJoin, int wLeave, int wFail, 
	    int wInsertDoc, int wFindDoc, 
	    int *time);

#endif
