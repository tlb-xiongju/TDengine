/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "thash.h"
#include "taoserror.h"
#include "os.h"
#include "tlog.h"

// the add ref count operation may trigger the warning if the reference count is greater than the MAX_WARNING_REF_COUNT
#define MAX_WARNING_REF_COUNT    10000
#define HASH_MAX_CAPACITY        (1024 * 1024 * 16)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)
#define HASH_INDEX(v, c)         ((v) & ((c)-1))

#define HASH_NEED_RESIZE(_h) ((_h)->size >= (_h)->capacity * HASH_DEFAULT_LOAD_FACTOR)

#define GET_HASH_NODE_KEY(_n)  ((char*)(_n) + sizeof(SHashNode) + (_n)->dataLen)
#define GET_HASH_NODE_DATA(_n) ((char*)(_n) + sizeof(SHashNode))
#define GET_HASH_PNODE(_n)     ((SHashNode *)((char*)(_n) - sizeof(SHashNode)))

#define FREE_HASH_NODE(_n) \
  do {                     \
    tfree(_n);             \
  } while (0);

typedef struct SHashEntry {
  int32_t    num;      // number of elements in current entry
  SRWLatch   latch;    // entry latch
  SHashNode *next;
} SHashEntry;

typedef struct SHashObj {
  SHashEntry     **hashList;
  size_t           capacity;     // number of slots
  size_t           size;         // number of elements in hash table
  _hash_fn_t       hashFp;       // hash function
  _equal_fn_t      equalFp;      // equal function
  _hash_free_fn_t  freeFp;       // hash node free callback function
  SRWLatch         lock;         // read-write spin lock
  SHashLockTypeE   type;         // lock type
  bool             enableUpdate; // enable update
  SArray          *pMemBlock;    // memory block allocated for SHashEntry
  _hash_before_fn_t callbackFp;  // function invoked before return the value to caller
} SHashObj;

/*
 * Function definition
 */
static FORCE_INLINE void taosHashWLock(SHashObj *pHashObj) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }
  taosWLockLatch(&pHashObj->lock);
}

static FORCE_INLINE void taosHashWUnlock(SHashObj *pHashObj) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }

  taosWUnLockLatch(&pHashObj->lock);
}

static FORCE_INLINE void taosHashRLock(SHashObj *pHashObj) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }

  taosRLockLatch(&pHashObj->lock);
}

static FORCE_INLINE void taosHashRUnlock(SHashObj *pHashObj) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }

  taosRUnLockLatch(&pHashObj->lock);
}

static FORCE_INLINE void taosHashEntryWLock(const SHashObj *pHashObj, SHashEntry* pe) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }
  taosWLockLatch(&pe->latch);
}

static FORCE_INLINE void taosHashEntryWUnlock(const SHashObj *pHashObj, SHashEntry* pe) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }

  taosWUnLockLatch(&pe->latch);
}

static FORCE_INLINE void taosHashEntryRLock(const SHashObj *pHashObj, SHashEntry* pe) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }

  taosRLockLatch(&pe->latch);
}

static FORCE_INLINE void taosHashEntryRUnlock(const SHashObj *pHashObj, SHashEntry* pe) {
  if (pHashObj->type == HASH_NO_LOCK) {
    return;
  }

  taosRUnLockLatch(&pe->latch);
}

static FORCE_INLINE int32_t taosHashCapacity(int32_t length) {
  int32_t len = MIN(length, HASH_MAX_CAPACITY);

  int32_t i = 4;
  while (i < len) i = (i << 1u);
  return i;
}

static FORCE_INLINE SHashNode *
doSearchInEntryList(SHashObj *pHashObj, SHashEntry *pe, const void *key, size_t keyLen, uint32_t hashVal) {
  SHashNode *pNode = pe->next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) &&
        ((*(pHashObj->equalFp))(GET_HASH_NODE_KEY(pNode), key, keyLen) == 0) &&
        pNode->removed == 0) {
      assert(pNode->hashVal == hashVal);
      break;
    }

    pNode = pNode->next;
  }

  return pNode;
}

/**
 * resize the hash list if the threshold is reached
 *
 * @param pHashObj
 */
static void taosHashTableResize(SHashObj *pHashObj);

/**
 * allocate and initialize a hash node
 *
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    data to be stored in hash node
 * @param dsize    size of data
 * @return         SHashNode
 */
static SHashNode *doCreateHashNode(const void *key, size_t keyLen, const void *pData, size_t dsize, uint32_t hashVal);

/**
 * update the hash node
 *
 * @param pHashObj   hash table object
 * @param pe         hash table entry to operate on
 * @param prev       previous node
 * @param pNode      the old node with requested key
 * @param pNewNode   the new node with requested key
 */
static FORCE_INLINE void doUpdateHashNode(SHashObj *pHashObj, SHashEntry* pe, SHashNode* prev, SHashNode *pNode, SHashNode *pNewNode) {
  assert(pNode->keyLen == pNewNode->keyLen);

  atomic_sub_fetch_32(&pNode->refCount, 1);
  if (prev != NULL) {
    prev->next = pNewNode;
  } else {
    pe->next = pNewNode;
  }

  if (pNode->refCount <= 0) {
    pNewNode->next = pNode->next;
    FREE_HASH_NODE(pNode);
  } else {
    pNewNode->next = pNode;
    pe->num++;
    atomic_add_fetch_64(&pHashObj->size, 1);
  }
}

/**
 * insert the hash node at the front of the linked list
 *
 * @param pHashObj   hash table object
 * @param pNode      the old node with requested key
 */
static void pushfrontNodeInEntryList(SHashEntry *pEntry, SHashNode *pNode);

/**
 * Check whether the hash table is empty or not.
 *
 * @param pHashObj the hash table object
 * @return if the hash table is empty or not
 */
static FORCE_INLINE bool taosHashTableEmpty(const SHashObj *pHashObj);

/**
 *
 * @param pHashObj
 * @return
 */
static FORCE_INLINE bool taosHashTableEmpty(const SHashObj *pHashObj) {
  return taosHashGetSize(pHashObj) == 0;
}

SHashObj *taosHashInit(size_t capacity, _hash_fn_t fn, bool update, SHashLockTypeE type) {
  if (fn == NULL) {
    assert(0);
    return NULL;
  }

  if (capacity == 0) {
    capacity = 4;
  }

  SHashObj *pHashObj = (SHashObj *)calloc(1, sizeof(SHashObj));
  if (pHashObj == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  // the max slots is not defined by user
  pHashObj->capacity = taosHashCapacity((int32_t)capacity);

  pHashObj->equalFp = memcmp;
  pHashObj->hashFp  = fn;
  pHashObj->type = type;
  pHashObj->enableUpdate = update;

  ASSERT((pHashObj->capacity & (pHashObj->capacity - 1)) == 0);

  pHashObj->hashList = (SHashEntry **)calloc(pHashObj->capacity, sizeof(void *));
  if (pHashObj->hashList == NULL) {
    free(pHashObj);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pHashObj->pMemBlock = taosArrayInit(8, sizeof(void *));
  if (pHashObj->pMemBlock == NULL) {
    free(pHashObj->hashList);
    free(pHashObj);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *p = calloc(pHashObj->capacity, sizeof(SHashEntry));
  if (p == NULL) {
    taosArrayDestroy(pHashObj->pMemBlock);
    free(pHashObj->hashList);
    free(pHashObj);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    pHashObj->hashList[i] = (void *)((char *)p + i * sizeof(SHashEntry));
  }

  taosArrayPush(pHashObj->pMemBlock, &p);
  return pHashObj;
}

void taosHashSetEqualFp(SHashObj *pHashObj, _equal_fn_t fp) {
  if (pHashObj != NULL && fp != NULL) {
    pHashObj->equalFp = fp;
  }
}

void taosHashSetFreeFp(SHashObj *pHashObj, _hash_free_fn_t fp) {
  if (pHashObj != NULL && fp != NULL) {
    pHashObj->freeFp = fp;
  }
}

int32_t taosHashGetSize(const SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return 0;
  }
  return (int32_t)atomic_load_64(&pHashObj->size);
}

int32_t taosHashPut(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t size) {
  if (pHashObj == NULL || key == NULL || keyLen == 0) {
    return -1;
  }

  uint32_t   hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);
  SHashNode *pNewNode = doCreateHashNode(key, keyLen, data, size, hashVal);
  if (pNewNode == NULL) {
    return -1;
  }

  // need the resize process, write lock applied
  if (HASH_NEED_RESIZE(pHashObj)) {
    taosHashWLock(pHashObj);
    taosHashTableResize(pHashObj);
    taosHashWUnlock(pHashObj);
  }

  // disable resize
  taosHashRLock(pHashObj);

  int32_t     slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  taosHashEntryWLock(pHashObj, pe);

  SHashNode *pNode = pe->next;
#if 0
  if (pe->num > 0) {
    assert(pNode != NULL);
  } else {
    assert(pNode == NULL);
  }
#endif

  SHashNode* prev = NULL;
  while (pNode) {
    if ((pNode->keyLen == keyLen) &&
        (*(pHashObj->equalFp))(GET_HASH_NODE_KEY(pNode), key, keyLen) == 0 &&
        pNode->removed == 0) {
      assert(pNode->hashVal == hashVal);
      break;
    }

    prev = pNode;
    pNode = pNode->next;
  }

  if (pNode == NULL) {
    // no data in hash table with the specified key, add it into hash table
    pushfrontNodeInEntryList(pe, pNewNode);
    assert(pe->next != NULL);

    taosHashEntryWUnlock(pHashObj, pe);

    // enable resize
    taosHashRUnlock(pHashObj);
    atomic_add_fetch_64(&pHashObj->size, 1);

    return 0;
  } else {
    // not support the update operation, return error
    if (pHashObj->enableUpdate) {
      doUpdateHashNode(pHashObj, pe, prev, pNode, pNewNode);
    } else {
      FREE_HASH_NODE(pNewNode);
    }

    taosHashEntryWUnlock(pHashObj, pe);

    // enable resize
    taosHashRUnlock(pHashObj);
    return pHashObj->enableUpdate ? 0 : -1;
  }
}

static void* taosHashGetImpl(SHashObj *pHashObj, const void *key, size_t keyLen, void** d, int32_t* size, bool addRef);

void *taosHashGet(SHashObj *pHashObj, const void *key, size_t keyLen) {
  void* p = NULL;
  return taosHashGetImpl(pHashObj, key, keyLen, &p, 0, false);
}

int32_t taosHashGetDup(SHashObj *pHashObj, const void *key, size_t keyLen, void *destBuf) {
  terrno = 0;
  /*char* p = */taosHashGetImpl(pHashObj, key, keyLen, &destBuf, 0, false);
  return terrno;
}

int32_t taosHashGetDup_m(SHashObj *pHashObj, const void *key, size_t keyLen, void **destBuf, int32_t* size) {
  terrno = 0;

  /*char* p = */taosHashGetImpl(pHashObj, key, keyLen, destBuf, size, false);
  return terrno;
}

void* taosHashGetImpl(SHashObj *pHashObj, const void *key, size_t keyLen, void** d, int32_t* size, bool addRef) {
  if (pHashObj == NULL || taosHashTableEmpty(pHashObj) || keyLen == 0 || key == NULL) {
    return NULL;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  // only add the read lock to disable the resize process
  taosHashRLock(pHashObj);

  int32_t     slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  // no data, return directly
  if (atomic_load_32(&pe->num) == 0) {
    taosHashRUnlock(pHashObj);
    return NULL;
  }

  char *data = NULL;
  taosHashEntryRLock(pHashObj, pe);

#if 0
  if (pe->num > 0) {
    assert(pe->next != NULL);
  } else {
    assert(pe->next == NULL);
  }
#endif

  SHashNode *pNode = doSearchInEntryList(pHashObj, pe, key, keyLen, hashVal);
  if (pNode != NULL) {
    if (pHashObj->callbackFp != NULL) {
      pHashObj->callbackFp(GET_HASH_NODE_DATA(pNode));
    }

    if (size != NULL) {
      if (*d == NULL) {
        *size =  pNode->dataLen;
        *d = calloc(1, *size);
        if (*d == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return NULL;
        }
      } else if (*size < pNode->dataLen) {
        *size =  pNode->dataLen;
        char* tmp = realloc(*d, *size);
        if (tmp == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return NULL;
        }

        *d = tmp;
      }
    }

    if (addRef) {
      atomic_add_fetch_16(&pNode->refCount, 1);
    }

    if (*d != NULL) {
      memcpy(*d, GET_HASH_NODE_DATA(pNode), pNode->dataLen);
    }

    data = GET_HASH_NODE_DATA(pNode);
  }

  taosHashEntryRUnlock(pHashObj, pe);
  taosHashRUnlock(pHashObj);

  return data;
}

int32_t taosHashRemoveWithData(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t dsize) {
  if (pHashObj == NULL || taosHashTableEmpty(pHashObj) || key == NULL || keyLen == 0) {
    return -1;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  // disable the resize process
  taosHashRLock(pHashObj);

  int32_t     slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  taosHashEntryWLock(pHashObj, pe);

  // double check after locked
  if (pe->num == 0) {
    assert(pe->next == NULL);

    taosHashEntryWUnlock(pHashObj, pe);
    taosHashRUnlock(pHashObj);
    return -1;
  }

  int code = -1;
  SHashNode *pNode = pe->next;
  SHashNode *prevNode = NULL;

  while (pNode) {
    if ((pNode->keyLen == keyLen) &&
        ((*(pHashObj->equalFp))(GET_HASH_NODE_KEY(pNode), key, keyLen) == 0) &&
        pNode->removed == 0) {
      code = 0;  // it is found

      atomic_sub_fetch_32(&pNode->refCount, 1);
      pNode->removed = 1;
      if (pNode->refCount <= 0) {
        if (prevNode == NULL) {
          pe->next = pNode->next;
        } else {
          prevNode->next = pNode->next;
        }

        if (data) memcpy(data, GET_HASH_NODE_DATA(pNode), dsize);

        pe->num--;
        atomic_sub_fetch_64(&pHashObj->size, 1);
        FREE_HASH_NODE(pNode);
      }
    } else {
      prevNode = pNode;
      pNode = pNode->next;
    }
  }

  taosHashEntryWUnlock(pHashObj, pe);
  taosHashRUnlock(pHashObj);

  return code;
}

int32_t taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen) {
  return taosHashRemoveWithData(pHashObj, key, keyLen, NULL, 0);
}

void taosHashClear(SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return;
  }

  SHashNode *pNode, *pNext;

  taosHashWLock(pHashObj);

  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    SHashEntry *pEntry = pHashObj->hashList[i];
    if (pEntry->num == 0) {
      assert(pEntry->next == NULL);
      continue;
    }

    pNode = pEntry->next;
    assert(pNode != NULL);

    while (pNode) {
      pNext = pNode->next;
      FREE_HASH_NODE(pNode);

      pNode = pNext;
    }

    pEntry->num = 0;
    pEntry->next = NULL;
  }

  pHashObj->size = 0;
  taosHashWUnlock(pHashObj);
}

// the input paras should be SHashObj **, so the origin input will be set by tfree(*pHashObj)
void taosHashCleanup(SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return;
  }

  taosHashClear(pHashObj);
  tfree(pHashObj->hashList);

  // destroy mem block
  size_t memBlock = taosArrayGetSize(pHashObj->pMemBlock);
  for (int32_t i = 0; i < memBlock; ++i) {
    void *p = taosArrayGetP(pHashObj->pMemBlock, i);
    tfree(p);
  }

  taosArrayDestroy(pHashObj->pMemBlock);
  free(pHashObj);
}

// for profile only
int32_t taosHashGetMaxOverflowLinkLength(const SHashObj *pHashObj){
  if (pHashObj == NULL || taosHashTableEmpty(pHashObj)) {
    return 0;
  }

  int32_t num = 0;

  taosHashRLock((SHashObj*) pHashObj);
  for (int32_t i = 0; i < pHashObj->size; ++i) {
    SHashEntry *pEntry = pHashObj->hashList[i];

    // fine grain per entry lock is not held since this is used
    // for profiling only and doesn't need an accurate count.
    if (num < pEntry->num) {
      num = pEntry->num;
    }
  }

  taosHashRUnlock((SHashObj*) pHashObj);
  return num;
}

void taosHashTableResize(SHashObj *pHashObj) {
  if (!HASH_NEED_RESIZE(pHashObj)) {
    return;
  }

  int32_t newCapacity = (int32_t)(pHashObj->capacity << 1u);
  if (newCapacity > HASH_MAX_CAPACITY) {
//    uDebug("current capacity:%zu, maximum capacity:%d, no resize applied due to limitation is reached",
//           pHashObj->capacity, HASH_MAX_CAPACITY);
    return;
  }

  int64_t st = taosGetTimestampUs();
  void *pNewEntryList = realloc(pHashObj->hashList, sizeof(void *) * newCapacity);
  if (pNewEntryList == NULL) {
//    uDebug("cache resize failed due to out of memory, capacity remain:%zu", pHashObj->capacity);
    return;
  }

  pHashObj->hashList = pNewEntryList;

  size_t inc = newCapacity - pHashObj->capacity;
  void * p = calloc(inc, sizeof(SHashEntry));

  for (int32_t i = 0; i < inc; ++i) {
    pHashObj->hashList[i + pHashObj->capacity] = (void *)((char *)p + i * sizeof(SHashEntry));
  }

  taosArrayPush(pHashObj->pMemBlock, &p);

  pHashObj->capacity = newCapacity;
  for (int32_t idx = 0; idx < pHashObj->capacity; ++idx) {
    SHashEntry *pe = pHashObj->hashList[idx];
    SHashNode *pNode;
    SHashNode *pNext;
    SHashNode *pPrev = NULL;

    if (pe->num == 0) {
      assert(pe->next == NULL);
      continue;
    }

    pNode = pe->next;

    assert(pNode != NULL);

    while (pNode != NULL) {
      int32_t newIdx = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
      pNext = pNode->next;
      if (newIdx != idx) {
        pe->num -= 1;
        if (pPrev == NULL) {
          pe->next = pNext;
        } else {
          pPrev->next = pNext;
        }

        SHashEntry *pNewEntry = pHashObj->hashList[newIdx];
        pushfrontNodeInEntryList(pNewEntry, pNode);
      } else {
        pPrev = pNode;
      }
      pNode = pNext;
    }
  }

  int64_t et = taosGetTimestampUs();

//  uDebug("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", (int32_t)pHashObj->capacity,
//         ((double)pHashObj->size) / pHashObj->capacity, (et - st) / 1000.0);
}

SHashNode *doCreateHashNode(const void *key, size_t keyLen, const void *pData, size_t dsize, uint32_t hashVal) {
  SHashNode *pNewNode = malloc(sizeof(SHashNode) + keyLen + dsize);

  if (pNewNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pNewNode->keyLen  = (uint32_t)keyLen;
  pNewNode->hashVal = hashVal;
  pNewNode->dataLen = (uint32_t)dsize;
  pNewNode->refCount= 1;
  pNewNode->removed = 0;
  pNewNode->next    = NULL;

  memcpy(GET_HASH_NODE_DATA(pNewNode), pData, dsize);
  memcpy(GET_HASH_NODE_KEY(pNewNode), key, keyLen);

  return pNewNode;
}

void pushfrontNodeInEntryList(SHashEntry *pEntry, SHashNode *pNode) {
  assert(pNode != NULL && pEntry != NULL);

  pNode->next = pEntry->next;
  pEntry->next = pNode;

  pEntry->num += 1;
}

size_t taosHashGetMemSize(const SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return 0;
  }

  return (pHashObj->capacity * (sizeof(SHashEntry) + sizeof(void*))) + sizeof(SHashNode) * taosHashGetSize(pHashObj) + sizeof(SHashObj);
}

void *taosHashGetKey(void *data, size_t* keyLen) {
  SHashNode * node = GET_HASH_PNODE(data);
  if (keyLen != NULL) {
    *keyLen = node->keyLen;
  }

  return GET_HASH_NODE_KEY(node);
}

// release the pNode, return next pNode, and lock the current entry
static void *taosHashReleaseNode(SHashObj *pHashObj, void *p, int *slot) {
  SHashNode *pOld = (SHashNode *)GET_HASH_PNODE(p);
  SHashNode *prevNode = NULL;

  *slot = HASH_INDEX(pOld->hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[*slot];

  taosHashEntryWLock(pHashObj, pe);

  SHashNode *pNode = pe->next;
  while (pNode) {
    if (pNode == pOld)
      break;

    prevNode = pNode;
    pNode = pNode->next;
  }

  if (pNode) {
    pNode = pNode->next;
    while (pNode) {
      if (pNode->removed == 0) break;
      pNode = pNode->next;
    }

    atomic_sub_fetch_32(&pOld->refCount, 1);
    if (pOld->refCount <=0) {
      if (prevNode) {
        prevNode->next = pOld->next;
      } else {
        pe->next = pOld->next;
      }

      pe->num--;
      atomic_sub_fetch_64(&pHashObj->size, 1);
      FREE_HASH_NODE(pOld);
    }
  } else {
//    uError("pNode:%p data:%p is not there!!!", pNode, p);
  }

  return pNode;
}

void *taosHashIterate(SHashObj *pHashObj, void *p) {
  if (pHashObj == NULL) return NULL;

  int  slot = 0;
  char *data = NULL;

  // only add the read lock to disable the resize process
  taosHashRLock(pHashObj);

  SHashNode *pNode = NULL;
  if (p) {
    pNode = taosHashReleaseNode(pHashObj, p, &slot);
    if (pNode == NULL) {
      SHashEntry *pe = pHashObj->hashList[slot];
      taosHashEntryWUnlock(pHashObj, pe);

      slot = slot + 1;
    }
  }

  if (pNode == NULL) {
    for (; slot < pHashObj->capacity; ++slot) {
      SHashEntry *pe = pHashObj->hashList[slot];

      taosHashEntryWLock(pHashObj, pe);

      pNode = pe->next;
      while (pNode) {
        if (pNode->removed == 0) break;
        pNode = pNode->next;
      }

      if (pNode) break;

      taosHashEntryWUnlock(pHashObj, pe);
    }
  }

  if (pNode) {
    SHashEntry *pe = pHashObj->hashList[slot];

    uint16_t prevRef = atomic_load_16(&pNode->refCount);
    uint16_t afterRef = atomic_add_fetch_16(&pNode->refCount, 1);
    ASSERT(prevRef < afterRef);

    // the reference count value is overflow, which will cause the delete node operation immediately.
    if (prevRef > afterRef) {
      uError("hash entry ref count overflow, prev ref:%d, current ref:%d", prevRef, afterRef);
      // restore the value
      atomic_sub_fetch_16(&pNode->refCount, 1);
      data = NULL;
    } else {
      data = GET_HASH_NODE_DATA(pNode);
    }

    if (afterRef >= MAX_WARNING_REF_COUNT) {
      uWarn("hash entry ref count is abnormally high: %d", afterRef);
    }

    taosHashEntryWUnlock(pHashObj, pe);
  }

  taosHashRUnlock(pHashObj);
  return data;
}

void taosHashCancelIterate(SHashObj *pHashObj, void *p) {
  if (pHashObj == NULL || p == NULL) return;

  // only add the read lock to disable the resize process
  taosHashRLock(pHashObj);

  int slot;
  taosHashReleaseNode(pHashObj, p, &slot);

  SHashEntry *pe = pHashObj->hashList[slot];

  taosHashEntryWUnlock(pHashObj, pe);
  taosHashRUnlock(pHashObj);
}

//TODO remove it
void *taosHashAcquire(SHashObj *pHashObj, const void *key, size_t keyLen) {
  void* p = NULL;
  return taosHashGetImpl(pHashObj, key, keyLen, &p, 0, true);
}

void taosHashRelease(SHashObj *pHashObj, void *p) { taosHashCancelIterate(pHashObj, p); }
