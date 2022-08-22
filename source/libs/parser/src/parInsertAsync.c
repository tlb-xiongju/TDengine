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
} SInsertParseSyntaxCxt;

typedef struct STableMetaSet {
  char      dbName[TSDB_DB_FNAME_LEN];
  SHashObj* pTableMeta;
  SHashObj* pTableVgroup;
} STableMetaSet;

typedef struct SInsertAnalyseSemanticCxt {
  SParseContext* pComCxt;
  SMsgBuf        msg;
  SHashObj*      pVgroups;
} SInsertAnalyseSemanticCxt;

typedef struct STableDataBlocks {
  uint32_t size;
  char*    pData;
} STableDataBlocks;

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

    SColVal val = {.value.pData = pToken->z, .value.nData = pToken->n};
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

static STableMetaSet* assignTableMetaSet(SHashObj* pDbHash, const char* pKey, int32_t keyLen, int32_t acctId,
                                         int32_t estimatedTableNum) {
  STableMetaSet* pSet = taosHashGet(pDbHash, pKey, keyLen);
  if (NULL == pSet) {
    pSet = taosMemoryMalloc(sizeof(STableMetaSet));
    if (NULL == pSet) {
      return NULL;
    }

    strncpy(pSet->dbName, pKey, keyLen);
    pSet->pTableMeta =
        taosHashInit(estimatedTableNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    pSet->pTableVgroup =
        taosHashInit(estimatedTableNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (NULL == pSet->pTableMeta || NULL == pSet->pTableVgroup) {
      destoryTableMetaSet(pSet);
      return NULL;
    }

    taosHashPut(pDbHash, pKey, keyLen, &pSet, POINTER_BYTES);
  }
  return pSet;
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

    STableMetaSet* pSet =
        assignTableMetaSet(pDbHash, pClause->targetDb.z, pClause->targetDb.n, pCxt->pComCxt->acctId, nTargetTables);
    if (NULL == pSet) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }

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

static int32_t buildTableReq(int32_t acctId, const char* pDbName, SHashObj* pTable, SArray* pDbReq, SArray* pPos) {
  STablesReq req = {.pTables = taosArrayInit(taosHashGetSize(pTable), sizeof(SName))};
  if (NULL == req.pTables) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  snprintf(req.dbFName, sizeof(req.dbFName), "%d.%s", acctId, pDbName);

  SName name = {.type = T_NAME_TABLE, .acctId = acctId};
  strcpy(name.dbname, pDbName);
  int32_t* p = taosHashIterate(pTable, NULL);
  while (NULL != p) {
    size_t      keyLen = 0;
    const char* pKey = taosHashGetKey(p, &keyLen);
    strncpy(name.tname, pKey, keyLen);
    taosArrayPush(req.pTables, &name);
    taosArrayPush(pPos, p);
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
  return TSDB_CODE_SUCCESS;
}

static int32_t setCatalogReq(SInsertParseSyntaxCxt* pCxt, SInsertValuesStmt* pStmt, SHashObj* pDbHash,
                             SCatalogReq* pCatalogReq) {
  int32_t        code = TSDB_CODE_SUCCESS;
  STableMetaSet* p = taosHashIterate(pDbHash, NULL);
  while (NULL != p && TSDB_CODE_SUCCESS == code) {
    code =
        buildTableReq(pCxt->pComCxt->acctId, p->dbName, p->pTableMeta, pCatalogReq->pTableMeta, pStmt->pTableMetaPos);
    if (TSDB_CODE_SUCCESS == code) {
      code = buildTableReq(pCxt->pComCxt->acctId, p->dbName, p->pTableVgroup, pCatalogReq->pTableHash,
                           pStmt->pTableVgroupPos);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = buildUserAuthReq(pCxt->pComCxt->pUser, pCxt->pComCxt->acctId, p->dbName, pCatalogReq->pUser);
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

  int32_t code = TSDB_CODE_SUCCESS;
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

static void initColVal(SSchema* pSchema, SColVal* pColVal) {
  pColVal->cid = pSchema->colId;
  pColVal->type = pSchema->type;
}

static int32_t createRowTemplateUseBoundCols(SInsertAnalyseSemanticCxt* pCxt, SSchema* pSchemas, SArray* pBoundCols,
                                             SArray** pRowTemplate) {
  *pRowTemplate = taosArrayInit(taosArrayGetSize(pBoundCols), sizeof(SColVal));
  if (NULL == *pRowTemplate) {
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
    SColVal colVal;
    initColVal(pSchemas + index, &colVal);
    taosArrayPush(*pRowTemplate, &colVal);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createRowTemplateUseSchema(const STableMeta* pMeta, SArray** pRowTemplate) {
  *pRowTemplate = taosArrayInit(getNumOfColumns(pMeta), sizeof(SColVal));
  if (NULL == *pRowTemplate) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSchema* pSchemas = getTableColumnSchema(pMeta);
  SColVal  colVal;
  for (int32_t i = 0; i < pMeta->tableInfo.numOfColumns; ++i) {
    initColVal(pSchemas + i, &colVal);
    taosArrayPush(*pRowTemplate, &colVal);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setColVal(SSqlStr* pSqlStr, SColVal* pColVal) {
  // todo
  return TSDB_CODE_SUCCESS;
}

static int32_t createRowTemplate(SInsertAnalyseSemanticCxt* pCxt, const STableMeta* pMeta, SArray* pBoundCols,
                                 SArray** pRowTemplate) {
  if (NULL == pBoundCols) {
    return createRowTemplateUseSchema(pMeta, pRowTemplate);
  }
  return createRowTemplateUseBoundCols(pCxt, getTableColumnSchema(pMeta), pBoundCols, pRowTemplate);
}

static int32_t analyseRowData(SArray* pRowStr, SArray* pRowTemplate, SArray* pIndex, STableDataBlocks* pBlocks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int16_t nCols = taosArrayGetSize(pRowStr);
  for (int16_t i = 0; TSDB_CODE_SUCCESS == code && i < nCols; ++i) {
    int16_t index = (NULL == pIndex ? i : *(int16_t*)taosArrayGet(pIndex, i));
    code = setColVal((SSqlStr*)taosArrayGet(pRowStr, i), (SColVal*)taosArrayGet(pRowTemplate, index));
  }
  STSRow2* pRow = (STSRow2*)(pBlocks->pData + pBlocks->size);
  if (TSDB_CODE_SUCCESS == code) {
    code = tTSRowNew(NULL, pRowTemplate, NULL, &pRow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    int32_t rowLen = 0;
    TSROW_LEN(pRow, rowLen);
    pBlocks->size += rowLen;
  }
  return code;
}

static int32_t comparColId(const void* l, const void* r) {
  const SColVal* pLeft = l;
  const SColVal* pRight = r;
  return pLeft->cid < pRight->cid ? -1 : (pLeft->cid > pRight->cid ? 1 : 0);
}

static int32_t createBoundColsIndex(SArray* pRowTemplate, SArray** pIndex) {
  int16_t nCols = taosArrayGetSize(pRowTemplate);
  *pIndex = taosArrayInit(nCols, sizeof(int16_t));
  if (NULL == *pIndex) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArraySetSize(*pIndex, nCols);
  taosArraySort(pRowTemplate, comparColId);
  for (int16_t i = 0; i < nCols; ++i) {
    SColVal* pColVal = taosArrayGet(pRowTemplate, i);
    int16_t* pPos = taosArrayGet(*pIndex, pColVal->value.i16);
    *pPos = i;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t analyseMultiRowsData(SInsertTableClause* pClause, SArray* pRowTemplate, STableDataBlocks* pBlocks) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* pIndex = NULL;
  if (NULL != pClause->pCols) {
    code = createBoundColsIndex(pRowTemplate, &pIndex);
  }
  int32_t nRows = taosArrayGetSize(pClause->pRows);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < nRows; ++i) {
    code = analyseRowData((SArray*)taosArrayGetP(pClause->pRows, i), pRowTemplate, pIndex, pBlocks);
  }
  return code;
}

static int32_t analyseDataOfOneClause(SInsertAnalyseSemanticCxt* pCxt, const STableMeta* pMeta, SVgroupInfo* pVg,
                                      SInsertTableClause* pClause) {
  SVCreateTbReq createReq = {0};
  if (NULL != pClause->usignDb.z) {
    // todo build SVCreateTbReq
  }
  SArray*           pRowTemplate;
  int32_t           code = createRowTemplate(pCxt, pMeta, pClause->pCols, &pRowTemplate);
  STableDataBlocks* pBlocks = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    // code = getDataBlockFromList(pCxt->pVgroups, &pVg->vgId, sizeof(pVg->vgId), TSDB_DEFAULT_PAYLOAD_SIZE,
    //                             sizeof(SSubmitBlk), getTableInfo(pMeta).rowSize, pMeta, &pBlocks, NULL, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = analyseMultiRowsData(pClause, pRowTemplate, pBlocks);
  }
  return code;
}

static int32_t analyseAllDataOfOneTable(SInsertAnalyseSemanticCxt* pCxt, const SMetaData* pMetaData,
                                        SInsertValuesStmt* pStmt, SVgroupInfo* pVg, SArray* pClausePosList) {
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pMeta = NULL;
  int32_t     nClauses = taosArrayGetSize(pClausePosList);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < nClauses; ++i) {
    int32_t clauseIndex = *(int32_t*)taosArrayGet(pClausePosList, i);
    code =
        getMetaData(pMetaData->pTableMeta, *(int32_t*)taosArrayGet(pStmt->pTableMetaPos, clauseIndex), (void**)&pMeta);
    if (TSDB_CODE_SUCCESS == code) {
      code = analyseDataOfOneClause(pCxt, pMeta, pVg,
                                    (SInsertTableClause*)taosArrayGetP(pStmt->pInsertTables, clauseIndex));
    }
  }
  return code;
}

int32_t analyseInsertData(SInsertAnalyseSemanticCxt* pCxt, const SCatalogReq* pCatalogReq, const SMetaData* pMetaData,
                          SQuery* pQuery) {
  SInsertValuesStmt* pStmt = (SInsertValuesStmt*)pQuery->pRoot;

  int32_t      code = TSDB_CODE_SUCCESS;
  SVgroupInfo* pVg = NULL;
  int32_t      nTargetTables = taosArrayGetSize(pMetaData->pTableHash);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < nTargetTables; ++i) {
    code = getMetaData(pMetaData->pTableHash, i, (void**)&pVg);
    if (TSDB_CODE_SUCCESS == code) {
      code = analyseAllDataOfOneTable(pCxt, pMetaData, pStmt, pVg, (SArray*)taosArrayGetP(pStmt->pTableVgroupPos, i));
    }
  }
  return code;
}

int32_t analyseInsert(SParseContext* pCxt, const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SQuery* pQuery) {
  SInsertAnalyseSemanticCxt cxt = {.pComCxt = pCxt};
  return analyseInsertData(&cxt, pCatalogReq, pMetaData, pQuery);
}
