#include <stdlib.h>

#include "docs.h"

/* insert specified document at the head of document list 
 *   return TRUE if document successfully inserted, and FALSE otherwise
 *   if document already in the list, do nothing and return TRUE
 */
int insertDocInList(DocList *docList, Document *doc)
{
  Document *tmp;

  if (docList->size >= MAX_NUM_DOCS) 
    /* no room available in document list */
    return FALSE;

  /* insert document at the head of the list */
  tmp = docList->head;
  docList->head = doc;
  doc->next = tmp;

  return TRUE;
}

/* find document with key docId */
Document *findDocInList(DocList *docList, DocId docId)
{
  Document *doc;

  for (doc = docList->head; doc; doc = doc->next)
    if (doc->id == docId)
      return doc;

  return NULL;
}

