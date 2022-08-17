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

// pSql -> tag1_value, ...)
static int32_t parseTagsClauseNew(SInsertParseContext* pCxt, SInsertTableClause* pClause) {
  pClause->pTagValues = taosArrayInit(taosArrayGetSize(pClause->pTags), sizeof(STagVal));

  SToken sToken;
  SToken sNextToken;
  while (1) {
    NEXT_TOKEN_WITH_PREV(pCxt->pSql, sToken);
    if (0 == sToken.n) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
    }
    if (TK_NK_RP == sToken.type) {
      break;
    }

    do {
      NEXT_TOKEN(pCxt->pSql, sNextToken);
      if (0 == sNextToken.n) {
        return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
      }
      if (TK_NK_COMMA == sNextToken.type || TK_NK_RP == sNextToken.type) {
        break;
      }
      sToken.n += sNextToken.n;
    } while (1);

    STagVal val = {.pData = sToken.z, .nData = sToken.n};
    taosArrayPush(pClause->pTagValues, &val);

    if (TK_NK_RP == sNextToken.type) {
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t parseBoundColumnsNew(SInsertParseContext* pCxt, SArray** pBoundCols) {
  *pBoundCols = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SSqlStr));

  SToken sToken;
  while (1) {
    NEXT_TOKEN(pCxt->pSql, sToken);
    if (TK_NK_RP == sToken.type) {
      break;
    }
    if (0 == sToken.n) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
    }

    SSqlStr colStr = {.z = sToken.z, .n = sToken.n};
    taosArrayPush(*pBoundCols, &colStr);
  }
}

// pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
static int32_t parseUsingClauseNew(SInsertParseContext* pCxt, SInsertTableClause* pClause) {
  SToken sToken;
  // pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);

  pClause->stable.z = sToken.z;
  pClause->stable.n = sToken.n;

  // pSql -> [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_NK_LP == sToken.type) {
    CHECK_CODE(parseBoundColumnsNew(pCxt, &pClause->pTags));
    NEXT_TOKEN(pCxt->pSql, sToken);
  }

  if (TK_TAGS != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "TAGS is expected", sToken.z);
  }
  // pSql -> (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_NK_LP != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "( is expected", sToken.z);
  }
  CHECK_CODE(parseTagsClauseNew(pCxt, pClause));

  return TSDB_CODE_SUCCESS;
}

static int32_t parseValuesClauseNew(SInsertParseContext* pCxt, SInsertTableClause* pClause) {
  STableComInfo tinfo = getTableInfo(pDataBlock->pTableMeta);
  int32_t       extendedRowSize = getExtendedRowSize(pDataBlock);
  CHECK_CODE(initRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));

  (*numOfRows) = 0;
  // char   tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
  SToken sToken;
  while (1) {
    int32_t index = 0;
    NEXT_TOKEN_KEEP_SQL(pCxt->pSql, sToken, index);
    if (TK_NK_LP != sToken.type) {
      break;
    }
    pCxt->pSql += index;

    if ((*numOfRows) >= maxRows || pDataBlock->size + extendedRowSize >= pDataBlock->nAllocSize) {
      int32_t tSize;
      CHECK_CODE(allocateMemIfNeed(pDataBlock, extendedRowSize, &tSize));
      ASSERT(tSize >= maxRows);
      maxRows = tSize;
    }

    bool gotRow = false;
    CHECK_CODE(parseOneRow(pCxt, pDataBlock, tinfo.precision, &gotRow, pCxt->tmpTokenBuf));
    if (gotRow) {
      pDataBlock->size += extendedRowSize;  // len;
    }

    NEXT_VALID_TOKEN(pCxt->pSql, sToken);
    if (TK_NK_COMMA == sToken.type) {
      return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
    } else if (TK_NK_RP != sToken.type) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", sToken.z);
    }

    if (gotRow) {
      (*numOfRows)++;
    }
  }

  if (0 == (*numOfRows) && (!TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT))) {
    return buildSyntaxErrMsg(&pCxt->msg, "no any data points", NULL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t parseInsertBodyNew(SInsertParseContext* pCxt) {
  // for each table
  while (1) {
    SToken sToken;
    // pSql -> tb_name ...
    NEXT_TOKEN(pCxt->pSql, sToken);

    // no data in the sql string anymore.
    if (sToken.n == 0) {
      if (sToken.type && pCxt->pSql[0]) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid charactor in SQL", sToken.z);
      }

      if (0 == pCxt->totalNum) {
        return buildInvalidOperationMsg(&pCxt->msg, "no data in sql");
      }
      break;
    }

    SInsertTableClause* pClause = (SInsertTableClause*)nodesMakeNode(QUERY_NODE_INSERT_TABLE_CLAUSE);
    if (NULL == pClause) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pClause->table.z = sToken.z;
    pClause->table.n = sToken.n;
    NEXT_TOKEN(pCxt->pSql, sToken);

    // USING clause
    if (TK_USING == sToken.type) {
      CHECK_CODE(parseUsingClauseNew(pCxt, pClause));
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_NK_LP == sToken.type) {
      // pSql -> field1_name, ...)
      CHECK_CODE(parseBoundColumnsNew(pCxt, &pClause->pCols));
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_USING == sToken.type) {
      CHECK_CODE(parseUsingClauseNew(pCxt, pClause));
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_VALUES == sToken.type) {
      // pSql -> (field1_value, ...) [(field1_value2, ...) ...]
      CHECK_CODE(parseValuesClause(pCxt, dataBuf));
      TSDB_QUERY_SET_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_INSERT);
      continue;
    }

    // FILE csv_file_path
    if (TK_FILE == sToken.type) {
      // pSql -> csv_file_path
      NEXT_TOKEN(pCxt->pSql, sToken);
      if (0 == sToken.n || (TK_NK_STRING != sToken.type && TK_NK_ID != sToken.type)) {
        return buildSyntaxErrMsg(&pCxt->msg, "file path is required following keyword FILE", sToken.z);
      }
      pClause->file.z = sToken.z;
      pClause->file.n = sToken.n;
      TSDB_QUERY_SET_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_FILE_INSERT);
      continue;
    }

    return buildSyntaxErrMsg(&pCxt->msg, "keyword VALUES or FILE is expected", sToken.z);
  }

  return TSDB_CODE_SUCCESS;
}
