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

#include "parToken.h"
#include "parUtil.h"

typedef struct SInsertParseSyntaxCxt {
  SParseContext* pComCxt;
  char*          pSql;
  SMsgBuf        msg;
  SHashObj*      pTargetDbs;  // element is SHashObj* pTargetTables
} SInsertParseSyntaxCxt;

typedef struct STableMetaSet {
  char      dbName[TSDB_DB_FNAME_LEN];
  SHashObj* pTableMeta;
  SHashObj* pTableVgroup;
} STableMetaSet;

typedef struct SInsertAnalyseSemanticCxt {
  SParseContext* pComCxt;
  SMsgBuf        msg;
  char           tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW];
  SHashObj*      pVgroups;
} SInsertAnalyseSemanticCxt;

typedef struct SInsertDataBlocks {
  uint32_t size;
  uint32_t allocSize;
  char*    pData;
} SInsertDataBlocks;

// required syntax: 'TAGS (tag1_value, ...)', pToken -> 'TAGS', pSql -> '('
static int32_t parseTagsClauseSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SInsertTableClause* pClause) {
  if (TK_TAGS != pToken->type) {
    return buildSyntaxErrMsg(&pCxt->msg, "TAGS is expected", pToken->z);
  }

  NEXT_TOKEN(pCxt->pSql, *pToken);
  if (TK_NK_LP != pToken->type) {
    return buildSyntaxErrMsg(&pCxt->msg, "( is expected", pToken->z);
  }

  pClause->pTagValues = taosArrayInit(taosArrayGetSize(pClause->pTags), sizeof(STagVal));

  SToken nextToken;
  while (1) {
    NEXT_TOKEN_WITH_PREV(pCxt->pSql, *pToken);
    if (0 == pToken->n) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
    }
    if (TK_NK_RP == pToken->type) {
      break;
    }

    do {
      NEXT_TOKEN(pCxt->pSql, nextToken);
      if (0 == nextToken.n) {
        return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
      }
      if (TK_NK_COMMA == nextToken.type || TK_NK_RP == nextToken.type) {
        break;
      }
      pToken->n += nextToken.n;
    } while (1);

    STagVal val = {.pData = pToken->z, .nData = pToken->n};
    taosArrayPush(pClause->pTagValues, &val);

    if (TK_NK_RP == nextToken.type) {
      break;
    }
  }

  NEXT_TOKEN(pCxt->pSql, *pToken);
  return TSDB_CODE_SUCCESS;
}

// optional syntax: '(col_name, ...)', pToken -> '(', pSql -> 'col_name'
static int32_t parseBoundColumnsSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SArray** pBoundCols) {
  if (TK_NK_LP != pToken->type) {
    return TSDB_CODE_SUCCESS;
  }

  *pBoundCols = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SSqlStr));
  while (1) {
    NEXT_TOKEN(pCxt->pSql, *pToken);
    if (TK_NK_RP == pToken->type) {
      break;
    }
    if (0 == pToken->n) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
    }

    SSqlStr colStr = {.z = pToken->z, .n = pToken->n};
    taosArrayPush(*pBoundCols, &colStr);
  }
  NEXT_TOKEN(pCxt->pSql, *pToken);
  return TSDB_CODE_SUCCESS;
}

// required syntax: [dbname.]tablename
static int32_t parseTableNameSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SSqlStr* pDb, SSqlStr* pTable) {
  SToken inputToken = *pToken;
  NEXT_TOKEN(pCxt->pSql, *pToken);
  if (TK_NK_DOT == pToken->type) {
    pDb->z = inputToken.z;
    pDb->n = inputToken.n;
    NEXT_TOKEN(pCxt->pSql, *pToken);
    pTable->z = pToken->z;
    pTable->n = pToken->n;
    NEXT_TOKEN(pCxt->pSql, *pToken);
  } else {
    if (NULL == pCxt->pComCxt->db) {
      return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_DB_NOT_SPECIFIED);
    }
    pDb->z = pCxt->pComCxt->db;
    pDb->n = strlen(pCxt->pComCxt->db);
    pTable->z = inputToken.z;
    pTable->n = inputToken.n;
  }
  return TSDB_CODE_SUCCESS;
}

// optional syntax: 'USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)', pToken -> 'USING', pSql -> 'stb_name'
static int32_t parseUsingClauseSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SInsertTableClause* pClause) {
  if (TK_USING != pToken->type) {
    return TSDB_CODE_SUCCESS;
  }

  // pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, *pToken);
  int32_t code = parseTableNameSyntax(pCxt, pToken, &pClause->usignDb, &pClause->usignStable);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseBoundColumnsSyntax(pCxt, pToken, &pClause->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = parseTagsClauseSyntax(pCxt, pToken, pClause);
  }
  return code;
}

// required syntax: '(col1_value, ...)', pToken -> '(', pSql -> 'col1_value'
static int32_t parseRowSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SInsertTableClause* pClause) {
  SArray* pRow = taosArrayInit(taosArrayGetSize(pClause->pCols), sizeof(SColVal));
  if (NULL == pRow) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArrayPush(pClause->pRows, &pRow);

  SToken nextToken;
  while (1) {
    NEXT_VALID_TOKEN(pCxt->pSql, *pToken);
    if (0 == pToken->n) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
    }
    if (TK_NK_RP == pToken->type) {
      break;
    }

    do {
      NEXT_VALID_TOKEN(pCxt->pSql, nextToken);
      if (0 == nextToken.n) {
        return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
      }
      if (TK_NK_COMMA == nextToken.type || TK_NK_RP == nextToken.type) {
        break;
      }
      pToken->n += nextToken.n;
    } while (1);

    SColVal val = {.cid = pToken->type, .value.pData = pToken->z, .value.nData = pToken->n};
    taosArrayPush(pRow, &val);

    if (TK_NK_RP == nextToken.type) {
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t parseValuesClauseSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SInsertTableClause* pClause) {
  pClause->pRows = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  if (NULL == pClause->pRows) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  while (TSDB_CODE_SUCCESS == code) {
    int32_t index = 0;
    NEXT_TOKEN_KEEP_SQL(pCxt->pSql, *pToken, index);
    if (TK_NK_LP != pToken->type) {
      break;
    }
    pCxt->pSql += index;
    code = parseRowSyntax(pCxt, pToken, pClause);
  }
  return code;
}

// pSql -> csv_file_path
static int32_t parseFileClauseSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SInsertTableClause* pClause) {
  NEXT_TOKEN(pCxt->pSql, *pToken);
  if (0 == pToken->n || (TK_NK_STRING != pToken->type && TK_NK_ID != pToken->type)) {
    return buildSyntaxErrMsg(&pCxt->msg, "file path is required following keyword FILE", pToken->z);
  }
  pClause->file.z = pToken->z;
  pClause->file.n = pToken->n;
  return TSDB_CODE_SUCCESS;
}

// required syntax: {VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path}
static int32_t parseDataClauseSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SInsertTableClause* pClause) {
  switch (pToken->type) {
    case TK_VALUES:
      return parseValuesClauseSyntax(pCxt, pToken, pClause);
    case TK_FILE:
      return parseFileClauseSyntax(pCxt, pToken, pClause);
    default:
      break;
  }
  return buildSyntaxErrMsg(&pCxt->msg, "keyword VALUES or FILE is expected", pToken->z);
}

static int32_t pushInsertClause(SArray* pInsertTables, SNode* pClause) {
  return NULL == taosArrayPush(pInsertTables, &pClause) ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS;
}

static int32_t assignInsertTableClause(SInsertParseSyntaxCxt* pCxt, SSqlStr* pDb, SSqlStr* pTable,
                                       SInsertTableClause** pClause) {
  SHashObj* pTargetTables = NULL;
  void*     p = taosHashGet(pCxt->pTargetDbs, pDb->z, pDb->n);
  if (NULL == p) {
    pTargetTables = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    taosHashPut(pCxt->pTargetDbs, pDb->z, pDb->n, &pTargetTables, POINTER_BYTES);
  } else {
    pTargetTables = *(SHashObj**)p;
  }
  return TSDB_CODE_SUCCESS;
}

// tb_name
//   [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//   [(field1_name, ...)]
//   VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
static int32_t parseInsertTableSyntax(SInsertParseSyntaxCxt* pCxt, SToken* pToken, SInsertValuesStmt* pStmt) {
  SInsertTableClause* pClause = (SInsertTableClause*)nodesMakeNode(QUERY_NODE_INSERT_TABLE_CLAUSE);
  if (NULL == pClause) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = parseTableNameSyntax(pCxt, pToken, &pClause->targetDb, &pClause->targetTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseUsingClauseSyntax(pCxt, pToken, pClause);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = parseBoundColumnsSyntax(pCxt, pToken, &pClause->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = parseUsingClauseSyntax(pCxt, pToken, pClause);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = parseDataClauseSyntax(pCxt, pToken, pClause);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = pushInsertClause(pStmt->pInsertTables, (SNode*)pClause);
  }

  return code;
}

static void destoryTableMetaSet(STableMetaSet* pSet) {
  taosHashCleanup(pSet->pTableMeta);
  taosHashCleanup(pSet->pTableVgroup);
  taosMemoryFree(pSet);
}

static int32_t createDbHash(SInsertValuesStmt* pStmt, SHashObj** pDbHash) {
  *pDbHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == *pDbHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static void destoryDbHash(SHashObj* pDbHash) {
  void* p = taosHashIterate(pDbHash, NULL);
  while (NULL != p) {
    destoryTableMetaSet(*(STableMetaSet**)p);
    p = taosHashIterate(pDbHash, p);
  }
  taosHashCleanup(pDbHash);
}

static int32_t assignTableMetaSet(SHashObj* pDbHash, const char* pKey, int32_t keyLen, int32_t acctId,
                                  int32_t estimatedTableNum, STableMetaSet** pOutput) {
  void* p = taosHashGet(pDbHash, pKey, keyLen);
  if (NULL == p) {
    STableMetaSet* pSet = taosMemoryMalloc(sizeof(STableMetaSet));
    if (NULL == pSet) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    strncpy(pSet->dbName, pKey, keyLen);
    pSet->pTableMeta =
        taosHashInit(estimatedTableNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    pSet->pTableVgroup =
        taosHashInit(estimatedTableNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (NULL == pSet->pTableMeta || NULL == pSet->pTableVgroup) {
      destoryTableMetaSet(pSet);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosHashPut(pDbHash, pKey, keyLen, &pSet, POINTER_BYTES);
    *pOutput = pSet;
  } else {
    *pOutput = *(STableMetaSet**)p;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t updateTableMetaKey(SHashObj* pTableHash, const char* pKey, int32_t keyLen, int32_t tableNo) {
  void* p = taosHashGet(pTableHash, pKey, keyLen);
  if (NULL == p) {
    SArray* pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(int32_t));
    if (NULL == pArray) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pArray, &tableNo);
    taosHashPut(pTableHash, pKey, keyLen, &pArray, POINTER_BYTES);
  } else {
    SArray* pArray = *(SArray**)p;
    taosArrayPush(pArray, &tableNo);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyForInsertValues(SInsertParseSyntaxCxt* pCxt, SInsertValuesStmt* pStmt, SHashObj* pDbHash) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nTargetTables = taosArrayGetSize(pStmt->pInsertTables);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < nTargetTables; ++i) {
    SInsertTableClause* pClause = (SInsertTableClause*)taosArrayGetP(pStmt->pInsertTables, i);
    if (NULL != pClause->pTags && taosArrayGetSize(pClause->pTags) != taosArrayGetSize(pClause->pTagValues)) {
      code = generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
      break;
    }

    STableMetaSet* pSet = NULL;
    code = assignTableMetaSet(pDbHash, pClause->targetDb.z, pClause->targetDb.n, pCxt->pComCxt->acctId, nTargetTables,
                              &pSet);
    if (TSDB_CODE_SUCCESS == code) {
      code = updateTableMetaKey(pSet->pTableVgroup, pClause->targetTable.z, pClause->targetTable.n, i);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (NULL != pClause->usignDb.z) {
        code = updateTableMetaKey(pSet->pTableMeta, pClause->usignStable.z, pClause->usignStable.n, i);
      } else {
        code = updateTableMetaKey(pSet->pTableMeta, pClause->targetTable.z, pClause->targetTable.n, i);
      }
    }
  }
  return code;
}

static int32_t buildTablesReq(int32_t acctId, const char* pDbName, SHashObj* pTable, SArray* pDbReq, SArray* pPos,
                              int32_t* pReqStartNo) {
  STablesReq req = {.pTables = taosArrayInit(taosHashGetSize(pTable), sizeof(SName))};
  if (NULL == req.pTables) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  snprintf(req.dbFName, sizeof(req.dbFName), "%d.%s", acctId, pDbName);

  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = acctId};
  strcpy(name.dbname, pDbName);
  name.dbname[strlen(pDbName)] = '\0';
  void* p = taosHashIterate(pTable, NULL);
  while (NULL != p) {
    size_t      keyLen = 0;
    const char* pKey = taosHashGetKey(p, &keyLen);
    strncpy(name.tname, pKey, keyLen);
    name.tname[keyLen] = '\0';
    taosArrayPush(req.pTables, &name);
    SArray* pTablesPos = *(SArray**)p;
    int32_t nTargetTables = taosArrayGetSize(pTablesPos);
    for (int32_t i = 0; i < nTargetTables; ++i) {
      taosArraySet(pPos, *(int32_t*)taosArrayGet(pTablesPos, i), pReqStartNo);
    }
    ++(*pReqStartNo);
    p = taosHashIterate(pTable, p);
  }

  taosArrayPush(pDbReq, &req);
  return TSDB_CODE_SUCCESS;
}

static int32_t buildUserAuthReq(const char* pUser, int32_t acctId, const char* pDbName, SArray* pAuthReq) {
  SUserAuthInfo auth = {.type = AUTH_TYPE_WRITE};
  strcpy(auth.user, pUser);
  snprintf(auth.dbFName, sizeof(auth.dbFName), "%d.%s", acctId, pDbName);
  taosArrayPush(pAuthReq, &auth);
  return TSDB_CODE_SUCCESS;
}

static int32_t initCatalogReq(SInsertValuesStmt* pStmt, SHashObj* pDbHash, SCatalogReq* pCatalogReq) {
  int32_t targetDbNum = taosHashGetSize(pDbHash);
  int32_t targetTableNum = taosArrayGetSize(pStmt->pInsertTables);
  pStmt->pTableMetaPos = taosArrayInit(targetTableNum, sizeof(int32_t));
  pStmt->pTableVgroupPos = taosArrayInit(targetTableNum, sizeof(int32_t));
  pCatalogReq->pTableMeta = taosArrayInit(targetDbNum, sizeof(STablesReq));
  pCatalogReq->pTableHash = taosArrayInit(targetDbNum, sizeof(STablesReq));
  pCatalogReq->pUser = taosArrayInit(targetDbNum, sizeof(SUserAuthInfo));
  if (NULL == pStmt->pTableMetaPos || NULL == pStmt->pTableVgroupPos || NULL == pCatalogReq->pTableMeta ||
      NULL == pCatalogReq->pTableHash || NULL == pCatalogReq->pUser) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArraySetSize(pStmt->pTableMetaPos, targetTableNum);
  taosArraySetSize(pStmt->pTableVgroupPos, targetTableNum);
  return TSDB_CODE_SUCCESS;
}

static int32_t setCatalogReq(SInsertParseSyntaxCxt* pCxt, SInsertValuesStmt* pStmt, SHashObj* pDbHash,
                             SCatalogReq* pCatalogReq) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t metaReqNo = 0;
  int32_t vgroupReqNo = 0;
  void*   p = taosHashIterate(pDbHash, NULL);
  while (NULL != p && TSDB_CODE_SUCCESS == code) {
    STableMetaSet* pSet = *(STableMetaSet**)p;
    code = buildTablesReq(pCxt->pComCxt->acctId, pSet->dbName, pSet->pTableMeta, pCatalogReq->pTableMeta,
                          pStmt->pTableMetaPos, &metaReqNo);
    if (TSDB_CODE_SUCCESS == code) {
      code = buildTablesReq(pCxt->pComCxt->acctId, pSet->dbName, pSet->pTableVgroup, pCatalogReq->pTableHash,
                            pStmt->pTableVgroupPos, &vgroupReqNo);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = buildUserAuthReq(pCxt->pComCxt->pUser, pCxt->pComCxt->acctId, pSet->dbName, pCatalogReq->pUser);
    }
    if (TSDB_CODE_SUCCESS == code) {
      p = taosHashIterate(pDbHash, p);
    }
  }
  return code;
}

static int32_t buildCatalogReqForInsertValues(SInsertParseSyntaxCxt* pCxt, SInsertValuesStmt* pStmt, SHashObj* pDbHash,
                                              SCatalogReq* pCatalogReq) {
  int32_t code = initCatalogReq(pStmt, pDbHash, pCatalogReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = setCatalogReq(pCxt, pStmt, pDbHash, pCatalogReq);
  }
  return code;
}

static int32_t checkAndbuildCatalogReq(SInsertParseSyntaxCxt* pCxt, SInsertValuesStmt* pStmt,
                                       SCatalogReq* pCatalogReq) {
  SHashObj* pDbHash = NULL;
  int32_t   code = createDbHash(pStmt, &pDbHash);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectMetaKeyForInsertValues(pCxt, pStmt, pDbHash);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCatalogReqForInsertValues(pCxt, pStmt, pDbHash, pCatalogReq);
  }
  destoryDbHash(pDbHash);
  return code;
}

static int32_t estimateNumOfTargetTables(int32_t sqlLen) { return sqlLen / 64; }

static int32_t initInsertValuesStmt(SInsertParseSyntaxCxt* pCxt, SInsertValuesStmt* pStmt) {
  pStmt->pInsertTables = taosArrayInit(estimateNumOfTargetTables(pCxt->pComCxt->sqlLen), POINTER_BYTES);
  if (NULL == pStmt->pInsertTables) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

// tb_name
//   [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//   [(field1_name, ...)]
//   VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
// [...];
static int32_t parseInsertBodySyntax(SInsertParseSyntaxCxt* pCxt, SQuery* pQuery) {
  SInsertValuesStmt* pStmt = (SInsertValuesStmt*)nodesMakeNode(QUERY_NODE_INSERT_VALUES_STMT);
  if (NULL == pStmt) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = initInsertValuesStmt(pCxt, pStmt);
  SToken  token;
  // for each table
  while (TSDB_CODE_SUCCESS == code) {
    // pSql -> tb_name ...
    NEXT_VALID_TOKEN(pCxt->pSql, token);

    // no data in the sql string anymore.
    if (token.n == 0) {
      if (token.type && pCxt->pSql[0]) {
        code = buildSyntaxErrMsg(&pCxt->msg, "invalid charactor in SQL", token.z);
        break;
      }

      if (0 == taosArrayGetSize(pStmt->pInsertTables)) {
        code = buildInvalidOperationMsg(&pCxt->msg, "no data in sql");
        break;
      }
      break;
    }
    code = parseInsertTableSyntax(pCxt, &token, pStmt);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pQuery->pRoot = (SNode*)pStmt;
  } else {
    nodesDestroyNode((SNode*)pStmt);
  }
  return code;
}

int32_t parseInsertSyntaxNew(SParseContext* pContext, SQuery** pQuery, SCatalogReq* pCatalogReq) {
  *pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == *pQuery) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SInsertParseSyntaxCxt context = {
      .pComCxt = pContext, .pSql = (char*)pContext->pSql, .msg = {.buf = pContext->pMsg, .len = pContext->msgLen}};
  int32_t code = skipInsertInto(&context.pSql, &context.msg);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseInsertBodySyntax(&context, *pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAndbuildCatalogReq(&context, (SInsertValuesStmt*)((*pQuery)->pRoot), pCatalogReq);
  }
  return code;
}

static int32_t getMetaData(SArray* pMeta, int32_t index, void** pData) {
  SMetaRes* pRes = taosArrayGet(pMeta, index);
  if (TSDB_CODE_SUCCESS != pRes->code) {
    return pRes->code;
  }
  *pData = pRes->pRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t findCol(const char* pColname, int32_t len, int32_t start, int32_t end, SSchema* pSchema) {
  while (start < end) {
    if (strlen(pSchema[start].name) == len && strncmp(pColname, pSchema[start].name, len) == 0) {
      return start;
    }
    ++start;
  }
  return -1;
}

static int32_t analyseColValImpl(SInsertAnalyseSemanticCxt* pCxt, uint8_t precision, const SSchema* pSchema,
                                 SToken* pToken, SColVal* pColVal) {
  if (isNullValue(pSchema->type, pToken)) {
    if (TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      return buildSyntaxErrMsg(&pCxt->msg, "primary timestamp should not be null", pToken->z);
    }
    pColVal->isNull = true;
    return TSDB_CODE_SUCCESS;
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {
      if ((pToken->type == TK_NK_BOOL || pToken->type == TK_NK_STRING) && (pToken->n != 0)) {
        if (strncmp(pToken->z, "true", pToken->n) == 0) {
          pColVal->value.i8 = TSDB_TRUE;
        } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
          pColVal->value.i8 = TSDB_FALSE;
        } else {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
        }
      } else if (pToken->type == TK_NK_INTEGER) {
        pColVal->value.i8 = (taosStr2Int64(pToken->z, NULL, 10) == 0) ? TSDB_FALSE : TSDB_TRUE;
      } else if (pToken->type == TK_NK_FLOAT) {
        pColVal->value.i8 = (taosStr2Double(pToken->z, NULL) == 0) ? TSDB_FALSE : TSDB_TRUE;
      } else {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid tinyint data", pToken->z);
      } else if (!IS_VALID_TINYINT(pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "tinyint data overflow", pToken->z);
      }
      pColVal->value.i8 = pColVal->value.i64;
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned tinyint data", pToken->z);
      } else if (!IS_VALID_UTINYINT(pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned tinyint data overflow", pToken->z);
      }
      pColVal->value.u8 = pColVal->value.u64;
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid smallint data", pToken->z);
      } else if (!IS_VALID_SMALLINT(pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "smallint data overflow", pToken->z);
      }
      pColVal->value.i16 = pColVal->value.i64;
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned smallint data", pToken->z);
      } else if (!IS_VALID_USMALLINT(pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned smallint data overflow", pToken->z);
      }
      pColVal->value.u16 = pColVal->value.u64;
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid int data", pToken->z);
      } else if (!IS_VALID_INT(pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "int data overflow", pToken->z);
      }
      pColVal->value.i32 = pColVal->value.i64;
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned int data", pToken->z);
      } else if (!IS_VALID_UINT(pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned int data overflow", pToken->z);
      }
      pColVal->value.u32 = pColVal->value.u64;
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid bigint data", pToken->z);
      } else if (!IS_VALID_BIGINT(pColVal->value.i64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "bigint data overflow", pToken->z);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned bigint data", pToken->z);
      } else if (!IS_VALID_UBIGINT(pColVal->value.u64)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned bigint data overflow", pToken->z);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      if (TSDB_CODE_SUCCESS != toDouble(pToken->z, pToken->n, &pColVal->value.d)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
      }
      if (((pColVal->value.d == HUGE_VAL || pColVal->value.d == -HUGE_VAL) && errno == ERANGE) ||
          pColVal->value.d > FLT_MAX || pColVal->value.d < -FLT_MAX || isinf(pColVal->value.d) ||
          isnan(pColVal->value.d)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
      }
      pColVal->value.f = pColVal->value.d;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      if (TSDB_CODE_SUCCESS != toDouble(pToken->z, pToken->n, &pColVal->value.d)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
      }
      if (((pColVal->value.d == HUGE_VAL || pColVal->value.d == -HUGE_VAL) && errno == ERANGE) ||
          isinf(pColVal->value.d) || isnan(pColVal->value.d)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
      }
      break;
    }
    case TSDB_DATA_TYPE_BINARY: {
      // Too long values will raise the invalid sql error message
      if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) {
        return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      break;  // todo
    }
    case TSDB_DATA_TYPE_JSON: {
      if (pToken->n > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        return buildSyntaxErrMsg(&pCxt->msg, "json string too long than 4095", pToken->z);
      }
      break;  // todo
    }
    case TSDB_DATA_TYPE_TIMESTAMP: {
      char* pStr = pColVal->value.pData;
      if (parseTime(&pStr, pToken, precision, &pColVal->value.ts, &pCxt->msg) != TSDB_CODE_SUCCESS) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid timestamp", pToken->z);
      }
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t analyseColVal(SInsertAnalyseSemanticCxt* pCxt, uint8_t precision, const SSchema* pSchema,
                             SColVal* pColVal) {
  SToken  token = {.type = pColVal->cid, .n = pColVal->value.nData, .z = pColVal->value.pData};
  int32_t code = checkAndTrimValue(&token, pCxt->tmpTokenBuf, &pCxt->msg);
  if (TSDB_CODE_SUCCESS == code) {
    code = analyseColValImpl(pCxt, precision, pSchema, &token, pColVal);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pColVal->cid = pSchema->colId;
    pColVal->type = pSchema->type;
  }
  return code;
}

static int32_t analyseRow(SInsertAnalyseSemanticCxt* pCxt, const STableMeta* pMeta, SArray* pRow, SArray* pRowSchema) {
  int32_t code = TSDB_CODE_SUCCESS;
  int16_t nCols = taosArrayGetSize(pRow);
  for (int16_t i = 0; TSDB_CODE_SUCCESS == code && i < nCols; ++i) {
    code = analyseColVal(pCxt, pMeta->tableInfo.precision,
                         getTableColumnSchema(pMeta) + *(int32_t*)taosArrayGet(pRowSchema, i),
                         (SColVal*)taosArrayGet(pRow, i));
  }
  return code;
}

static int32_t analyseMultiRows(SInsertAnalyseSemanticCxt* pCxt, const STableMeta* pMeta, SInsertTableClause* pClause,
                                SArray* pRowSchema) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nRows = taosArrayGetSize(pClause->pRows);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < nRows; ++i) {
    code = analyseRow(pCxt, pMeta, (SArray*)taosArrayGetP(pClause->pRows, i), pRowSchema);
  }
  return code;
}

static int32_t createRowSchemaWithBoundCols(SInsertAnalyseSemanticCxt* pCxt, SSchema* pSchemas, SArray* pBoundCols,
                                            SArray** pRowSchema) {
  *pRowSchema = taosArrayInit(taosArrayGetSize(pBoundCols), sizeof(int32_t));
  if (NULL == *pRowSchema) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t nCols = taosArrayGetSize(pBoundCols);
  int32_t lastColIdx = -1;  // last column found
  for (int32_t i = 0; i < nCols; ++i) {
    SSqlStr* pStr = taosArrayGet(pBoundCols, i);
    int32_t  startIndex = lastColIdx + 1;
    int32_t  index = findCol(pStr->z, pStr->n, startIndex, nCols, pSchemas);
    if (index < 0 && startIndex > 0) {
      index = findCol(pStr->z, pStr->n, 0, startIndex, pSchemas);
    }
    if (index < 0) {
      return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_INVALID_COLUMN, pStr->z);
    }
    if (pSchemas[index].flags & COL_EXT_HOLD) {
      return buildSyntaxErrMsg(&pCxt->msg, "duplicated column name", pStr->z);
    }
    pSchemas[index].flags |= COL_EXT_HOLD;
    taosArrayPush(*pRowSchema, &index);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createRowSchemaWithoutBoundCols(const STableMeta* pMeta, SArray** pRowSchema) {
  *pRowSchema = taosArrayInit(getNumOfColumns(pMeta), sizeof(int32_t));
  if (NULL == *pRowSchema) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSchema* pSchemas = getTableColumnSchema(pMeta);
  for (int32_t i = 0; i < pMeta->tableInfo.numOfColumns; ++i) {
    taosArrayPush(*pRowSchema, &i);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createRowSchema(SInsertAnalyseSemanticCxt* pCxt, const STableMeta* pMeta, SArray* pBoundCols,
                               SArray** pRowSchema) {
  if (NULL == pBoundCols) {
    return createRowSchemaWithoutBoundCols(pMeta, pRowSchema);
  }
  return createRowSchemaWithBoundCols(pCxt, getTableColumnSchema(pMeta), pBoundCols, pRowSchema);
}

static int32_t buildCreateTbReq(SInsertAnalyseSemanticCxt* pCxt, const STableMeta* pMeta, SInsertTableClause* pClause) {
  if (NULL == pClause->usignDb.z) {
    return TSDB_CODE_SUCCESS;
  }
  // todo build SVCreateTbReq
  return TSDB_CODE_SUCCESS;
}

static int32_t createInsertDataBlock(SInsertDataBlocks** pOutput) {
  SInsertDataBlocks* pBlocks = (SInsertDataBlocks*)taosMemoryCalloc(1, sizeof(SInsertDataBlocks));
  if (NULL == pBlocks) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pBlocks->allocSize = TSDB_PAYLOAD_SIZE;
  pBlocks->pData = taosMemoryMalloc(pBlocks->allocSize);
  if (NULL == pBlocks->pData) {
    taosMemoryFree(pBlocks);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  pBlocks->size = sizeof(SSubmitReq);

  *pOutput = pBlocks;
  return TSDB_CODE_SUCCESS;
}

static void destoryInsertDataBlocks(SInsertDataBlocks* pBlocks) {
  taosMemoryFree(pBlocks->pData);
  taosMemoryFree(pBlocks);
}

static int32_t assignInsertBlocks(SHashObj* pHash, const char* pKey, int32_t keyLen, SInsertDataBlocks** pBlocks) {
  void* p = taosHashGet(pHash, pKey, keyLen);
  if (NULL != p) {
    *pBlocks = *(SInsertDataBlocks**)p;
    return TSDB_CODE_SUCCESS;
  }

  SInsertDataBlocks* pDataBlocks = NULL;
  int32_t            code = createInsertDataBlock(&pDataBlocks);
  if (TSDB_CODE_SUCCESS == code) {
    code = taosHashPut(pHash, pKey, keyLen, &pDataBlocks, POINTER_BYTES);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pBlocks = pDataBlocks;
  } else {
    destoryInsertDataBlocks(pDataBlocks);
  }
  return code;
}

static int32_t buildRowsMsg(SInsertTableClause* pClause, SInsertDataBlocks* pBlocks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nRows = taosArrayGetSize(pClause->pRows);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < nRows; ++i) {
    STSRow2* pRow = (STSRow2*)(pBlocks->pData + pBlocks->size);
    code = tTSRowNew(NULL, (SArray*)taosArrayGetP(pClause->pRows, i), NULL, &pRow);
    if (TSDB_CODE_SUCCESS == code) {
      int32_t rLen = 0;
      TSROW_LEN(pRow, rLen);
      pBlocks->size += rLen;
    }
  }
  return code;
}

static int32_t buildMemoryRow(SInsertAnalyseSemanticCxt* pCxt, SVgroupInfo* pVg, SInsertTableClause* pClause) {
  SInsertDataBlocks* pBlocks = NULL;
  int32_t            code = assignInsertBlocks(pCxt->pVgroups, (const char*)&pVg->vgId, sizeof(pVg->vgId), &pBlocks);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRowsMsg(pClause, pBlocks);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t analyseDataOfOneClause(SInsertAnalyseSemanticCxt* pCxt, const STableMeta* pMeta, SVgroupInfo* pVg,
                                      SInsertTableClause* pClause) {
  SArray* pRowSchema;
  int32_t code = buildCreateTbReq(pCxt, pMeta, pClause);
  if (TSDB_CODE_SUCCESS == code) {
    code = createRowSchema(pCxt, pMeta, pClause->pCols, &pRowSchema);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = analyseMultiRows(pCxt, pMeta, pClause, pRowSchema);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildMemoryRow(pCxt, pVg, pClause);
  }
  return code;
}

static int32_t analyseInsertData(SInsertAnalyseSemanticCxt* pCxt, const SCatalogReq* pCatalogReq,
                                 const SMetaData* pMetaData, SInsertValuesStmt* pStmt) {
  STableMeta*  pMeta = NULL;
  SVgroupInfo* pVg = NULL;
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      nTargetTables = taosArrayGetSize(pStmt->pInsertTables);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < nTargetTables; ++i) {
    code = getMetaData(pMetaData->pTableMeta, *(int32_t*)taosArrayGet(pStmt->pTableMetaPos, i), (void**)&pMeta);
    if (TSDB_CODE_SUCCESS == code) {
      code = getMetaData(pMetaData->pTableHash, *(int32_t*)taosArrayGet(pStmt->pTableVgroupPos, i), (void**)&pVg);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = analyseDataOfOneClause(pCxt, pMeta, pVg, (SInsertTableClause*)taosArrayGetP(pStmt->pInsertTables, i));
    }
  }
  return code;
}

static int32_t buildVgDataBlocks(SInsertAnalyseSemanticCxt* pCxt, SArray** pVgBlocks) {
  // todo
  return TSDB_CODE_SUCCESS;
}

int32_t analyseInsert(SParseContext* pCxt, const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SQuery* pQuery) {
  SInsertAnalyseSemanticCxt cxt = {.pComCxt = pCxt};
  SArray*                   pVgBlocks = NULL;

  int32_t code = analyseInsertData(&cxt, pCatalogReq, pMetaData, (SInsertValuesStmt*)pQuery->pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildVgDataBlocks(&cxt, &pVgBlocks);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteToVnodeModifyOpStmt(pQuery, pVgBlocks);
  }
  return code;
}
