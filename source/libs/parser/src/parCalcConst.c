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

#include "functionMgt.h"
#include "parInt.h"
#include "scalar.h"
#include "ttime.h"

typedef struct SCalcConstContext {
  SParseContext* pParseCxt;
  SMsgBuf        msgBuf;
  int32_t        code;
} SCalcConstContext;

static int32_t calcConstQuery(SCalcConstContext* pCxt, SNode* pStmt, bool subquery);

static int32_t calcConstSubquery(SCalcConstContext* pCxt, STempTableNode* pTempTable) {
  return calcConstQuery(pCxt, pTempTable->pSubquery, true);
}

static int32_t calcConstNode(SNode** pNode) {
  if (NULL == *pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNode*  pNew = NULL;
  int32_t code = scalarCalculateConstants(*pNode, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    *pNode = pNew;
  }
  return code;
}

static int32_t calcConstList(SNodeList* pList) {
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode*  pNew = NULL;
    int32_t code = scalarCalculateConstants(pNode, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(pNew);
    } else {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool isCondition(const SNode* pNode) {
  if (QUERY_NODE_OPERATOR == nodeType(pNode)) {
    return nodesIsComparisonOp((const SOperatorNode*)pNode);
  }
  return (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode));
}

static int32_t rewriteIsTrue(SNode* pSrc, SNode** pIsTrue) {
  SOperatorNode* pOp = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  if (NULL == pOp) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pOp->opType = OP_TYPE_IS_TRUE;
  pOp->pLeft = pSrc;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  *pIsTrue = (SNode*)pOp;
  return TSDB_CODE_SUCCESS;
}

static EDealRes doRewriteCondition(SNode** pNode, void* pContext) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    SNode* pParam = NULL;
    FOREACH(pParam, ((SLogicConditionNode*)*pNode)->pParameterList) {
      if (!isCondition(pParam)) {
        SNode* pIsTrue = NULL;
        if (TSDB_CODE_SUCCESS != rewriteIsTrue(pParam, &pIsTrue)) {
          ((SCalcConstContext*)pContext)->code = TSDB_CODE_OUT_OF_MEMORY;
          return DEAL_RES_ERROR;
        }
        REPLACE_NODE(pIsTrue);
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteCondition(SCalcConstContext* pCxt, SNode** pNode) {
  if (!isCondition(*pNode)) {
    return rewriteIsTrue(*pNode, pNode);
  }
  nodesRewriteExprPostOrder(pNode, doRewriteCondition, pCxt);
  return pCxt->code;
}

static int32_t calcConstCondition(SCalcConstContext* pCxt, SNode** pNode) {
  int32_t code = rewriteCondition(pCxt, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(pNode);
  }
  return code;
}

static int32_t rewriteConditionForFromTable(SCalcConstContext* pCxt, SNode* pTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pTable)) {
    case QUERY_NODE_TEMP_TABLE: {
      code = calcConstSubquery(pCxt, (STempTableNode*)pTable);
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoin = (SJoinTableNode*)pTable;
      code = rewriteConditionForFromTable(pCxt, pJoin->pLeft);
      if (TSDB_CODE_SUCCESS == code) {
        code = rewriteConditionForFromTable(pCxt, pJoin->pRight);
      }
      if (TSDB_CODE_SUCCESS == code && NULL != pJoin->pOnCond) {
        code = calcConstCondition(pCxt, &pJoin->pOnCond);
      }
      // todo empty table
      break;
    }
    default:
      break;
  }
  return code;
}

static int32_t calcConstFromTable(SCalcConstContext* pCxt, SNode* pTable) {
  return rewriteConditionForFromTable(pCxt, pTable);
}

static void rewriteConstCondition(SNode** pCond, bool* pAlwaysFalse) {
  if (QUERY_NODE_VALUE != nodeType(*pCond)) {
    return;
  }
  if (((SValueNode*)*pCond)->datum.b) {
    nodesDestroyNode(*pCond);
    *pCond = NULL;
  } else {
    *pAlwaysFalse = true;
  }
}

static int32_t calcConstStmtCondition(SCalcConstContext* pCxt, SNode** pCond, bool* pAlwaysFalse) {
  if (NULL == *pCond) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = rewriteCondition(pCxt, pCond);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(pCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    rewriteConstCondition(pCond, pAlwaysFalse);
  }
  return code;
}

static int32_t calcConstProject(SNode* pProject, bool dual, SNode** pNew) {
  SArray* pAssociation = NULL;
  if (NULL != ((SExprNode*)pProject)->pAssociation) {
    pAssociation = taosArrayDup(((SExprNode*)pProject)->pAssociation);
    if (NULL == pAssociation) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  char aliasName[TSDB_COL_NAME_LEN] = {0};
  strcpy(aliasName, ((SExprNode*)pProject)->aliasName);
  int32_t code = TSDB_CODE_SUCCESS;
  if (dual) {
    code = scalarCalculateConstantsFromDual(pProject, pNew);
  } else {
    code = scalarCalculateConstants(pProject, pNew);
  }
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_VALUE == nodeType(*pNew) && NULL != pAssociation) {
    strcpy(((SExprNode*)*pNew)->aliasName, aliasName);
    int32_t size = taosArrayGetSize(pAssociation);
    for (int32_t i = 0; i < size; ++i) {
      SNode** pCol = taosArrayGetP(pAssociation, i);
      nodesDestroyNode(*pCol);
      *pCol = nodesCloneNode(*pNew);
      if (NULL == *pCol) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
    }
  }
  taosArrayDestroy(pAssociation);
  return code;
}

static bool isUselessCol(SExprNode* pProj) {
  if (QUERY_NODE_FUNCTION == nodeType(pProj) && !fmIsScalarFunc(((SFunctionNode*)pProj)->funcId) &&
      !fmIsPseudoColumnFunc(((SFunctionNode*)pProj)->funcId)) {
    return false;
  }
  return NULL == ((SExprNode*)pProj)->pAssociation;
}

static SNode* createConstantValue() {
  SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == pVal) {
    return NULL;
  }
  pVal->node.resType.type = TSDB_DATA_TYPE_INT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  const int32_t val = 1;
  nodesSetValueNodeValue(pVal, (void*)&val);
  pVal->translate = true;
  return (SNode*)pVal;
}

static int32_t calcConstProjections(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  SNode* pProj = NULL;
  WHERE_EACH(pProj, pSelect->pProjectionList) {
    if (subquery && !pSelect->isDistinct && isUselessCol((SExprNode*)pProj)) {
      ERASE_NODE(pSelect->pProjectionList);
      continue;
    }
    SNode*  pNew = NULL;
    int32_t code = calcConstProject(pProj, (NULL == pSelect->pFromTable), &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(pNew);
    } else {
      return code;
    }
    WHERE_NEXT;
  }
  if (0 == LIST_LENGTH(pSelect->pProjectionList)) {
    return nodesListStrictAppend(pSelect->pProjectionList, createConstantValue());
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t calcConstGroupBy(SCalcConstContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = calcConstList(pSelect->pGroupByList);
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNode = NULL;
    FOREACH(pNode, pSelect->pGroupByList) {
      SNode* pGroupPara = NULL;
      FOREACH(pGroupPara, ((SGroupingSetNode*)pNode)->pParameterList) {
        if (QUERY_NODE_VALUE != nodeType(pGroupPara)) {
          return code;
        }
      }
    }
    NODES_DESTORY_LIST(pSelect->pGroupByList);
  }
  return code;
}

static int32_t calcConstSelectWithoutFrom(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  return calcConstProjections(pCxt, pSelect, subquery);
}

static int32_t calcConstSelectFrom(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  int32_t code = calcConstFromTable(pCxt, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstProjections(pCxt, pSelect, subquery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pSelect->pWhere, &pSelect->isEmptyResult);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSelect->pPartitionByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstNode(&pSelect->pWindow);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstGroupBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pSelect->pHaving, &pSelect->isEmptyResult);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstList(pSelect->pOrderByList);
  }
  return code;
}

static int32_t calcConstSelect(SCalcConstContext* pCxt, SSelectStmt* pSelect, bool subquery) {
  if (NULL == pSelect->pFromTable) {
    return calcConstSelectWithoutFrom(pCxt, pSelect, subquery);
  } else {
    return calcConstSelectFrom(pCxt, pSelect, subquery);
  }
}

static int32_t calcConstDelete(SCalcConstContext* pCxt, SDeleteStmt* pDelete) {
  int32_t code = calcConstFromTable(pCxt, pDelete->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstStmtCondition(pCxt, &pDelete->pWhere, &pDelete->deleteZeroRows);
  }
  return code;
}

static int32_t calcConstInsert(SCalcConstContext* pCxt, SInsertStmt* pInsert) {
  int32_t code = calcConstFromTable(pCxt, pInsert->pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = calcConstQuery(pCxt, pInsert->pQuery, false);
  }
  return code;
}

static int32_t calcConstQuery(SCalcConstContext* pCxt, SNode* pStmt, bool subquery) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      code = calcConstSelect(pCxt, (SSelectStmt*)pStmt, subquery);
      break;
    case QUERY_NODE_EXPLAIN_STMT:
      code = calcConstQuery(pCxt, ((SExplainStmt*)pStmt)->pQuery, subquery);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOp = (SSetOperator*)pStmt;
      code = calcConstQuery(pCxt, pSetOp->pLeft, false);
      if (TSDB_CODE_SUCCESS == code) {
        code = calcConstQuery(pCxt, pSetOp->pRight, false);
      }
      break;
    }
    case QUERY_NODE_DELETE_STMT:
      code = calcConstDelete(pCxt, (SDeleteStmt*)pStmt);
      break;
    case QUERY_NODE_INSERT_STMT:
      code = calcConstInsert(pCxt, (SInsertStmt*)pStmt);
      break;
    default:
      break;
  }
  return code;
}

static bool isEmptyResultQuery(SNode* pStmt) {
  bool isEmptyResult = false;
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      isEmptyResult = ((SSelectStmt*)pStmt)->isEmptyResult;
      break;
    case QUERY_NODE_EXPLAIN_STMT:
      isEmptyResult = isEmptyResultQuery(((SExplainStmt*)pStmt)->pQuery);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOp = (SSetOperator*)pStmt;
      isEmptyResult = isEmptyResultQuery(pSetOp->pLeft);
      if (isEmptyResult) {
        isEmptyResult = isEmptyResultQuery(pSetOp->pRight);
      }
      break;
    }
    default:
      break;
  }
  return isEmptyResult;
}

int32_t calculateConstant(SParseContext* pParseCxt, SQuery* pQuery) {
  SCalcConstContext cxt = {.pParseCxt = pParseCxt,
                           .msgBuf.buf = pParseCxt->pMsg,
                           .msgBuf.len = pParseCxt->msgLen,
                           .code = TSDB_CODE_SUCCESS};
  int32_t           code = calcConstQuery(&cxt, pQuery->pRoot, false);
  if (TSDB_CODE_SUCCESS == code && isEmptyResultQuery(pQuery->pRoot)) {
    pQuery->execMode = QUERY_EXEC_MODE_EMPTY_RESULT;
  }
  return code;
}
