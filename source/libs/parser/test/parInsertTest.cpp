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

#include <gtest/gtest.h>

#include "mockCatalog.h"
#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

// syntax:
// INSERT INTO
//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
class ParserInsertTest : public ParserTestBase {};

// INSERT INTO tb_name [(field1_name, ...)] VALUES (field1_value, ...)
TEST_F(ParserInsertTest, singleTableSingleRowTest) {
  useDb("root", "test");

  run("INSERT INTO t1 VALUES (now, 1, 'beijing', 3, 4, 5)");

  run("INSERT INTO t1 (ts, c1, c2, c3, c4, c5) VALUES (now, 1, 'beijing', 3, 4, 5)");
}

// INSERT INTO tb_name VALUES (field1_value, ...)(field1_value, ...)
TEST_F(ParserInsertTest, singleTableMultiRowTest) {
  useDb("root", "test");

  run("INSERT INTO t1 VALUES (now, 1, 'beijing', 3, 4, 5)"
      "(now+1s, 2, 'shanghai', 6, 7, 8)"
      "(now+2s, 3, 'guangzhou', 9, 10, 11)");
}

// INSERT INTO tb1_name VALUES (field1_value, ...) tb2_name VALUES (field1_value, ...)
TEST_F(ParserInsertTest, multiTableSingleRowTest) {
  useDb("root", "test");

  run("INSERT INTO st1s1 VALUES (now, 1, 'beijing') st1s2 VALUES (now, 10, '131028')");
}

// INSERT INTO tb1_name VALUES (field1_value, ...) tb2_name VALUES (field1_value, ...)
TEST_F(ParserInsertTest, multiTableMultiRowTest) {
  useDb("root", "test");

  run("INSERT INTO "
      "st1s1 VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou') "
      "st1s2 VALUES (now, 10, '131028')(now+1s, 20, '132028')");
}

// INSERT INTO
//    tb1_name USING st1_name [(tag1_name, ...)] TAGS (tag1_value, ...) VALUES (field1_value, ...)
//    tb2_name USING st2_name [(tag1_name, ...)] TAGS (tag1_value, ...) VALUES (field1_value, ...)
TEST_F(ParserInsertTest, autoCreateTableTest) {
  useDb("root", "test");

  run("INSERT INTO st1s1 USING st1 TAGS(1, 'wxy', now) "
      "VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou')");

  run("INSERT INTO st1s1 USING st1 (tag1, tag2) TAGS(1, 'wxy') (ts, c1, c2) "
      "VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou')");

  run("INSERT INTO st1s1 (ts, c1, c2) USING st1 (tag1, tag2) TAGS(1, 'wxy') "
      "VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou')");

  run("INSERT INTO "
      "st1s1 USING st1 (tag1, tag2) TAGS(1, 'wxy') (ts, c1, c2) VALUES (now, 1, 'beijing') "
      "st1s2 (ts, c1, c2) USING st1 TAGS(2, 'abc', now) VALUES (now+1s, 2, 'shanghai')");
}

TEST_F(ParserInsertTest, performance) {
  useDb("root", "test");

  const int32_t subTableNum = 4000;
  const int32_t insertRows = 8000;
  const int32_t interlaceRows = insertRows / subTableNum;

  g_mockCatalogService->createTableBuilder("test", "st10", TSDB_SUPER_TABLE, 5, 1)
      .setPrecision(TSDB_TIME_PRECISION_MILLI)
      .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
      .addColumn("c1", TSDB_DATA_TYPE_INT)
      .addColumn("c2", TSDB_DATA_TYPE_INT)
      .addColumn("c3", TSDB_DATA_TYPE_INT)
      .addColumn("c4", TSDB_DATA_TYPE_INT)
      .addTag("tag1", TSDB_DATA_TYPE_INT)
      .done();
  for (int32_t i = 1; i <= subTableNum; ++i) {
    g_mockCatalogService->createSubTable("test", "st10", "st1s" + to_string(i), 1);
  }

  int32_t sqlCount = 1000;
  while (sqlCount--) {
    string sql("INSERT INTO ");
    for (int32_t i = 1; i <= subTableNum; ++i) {
      sql.append("st1s" + to_string(i) + " VALUES ");
      for (int32_t j = 0; j < interlaceRows; ++j) {
        sql.append("(now+" + to_string(j) + "s, " + to_string(j) + ", " + to_string(j + 1) + ", " + to_string(j + 2) +
                   ", " + to_string(j + 3) + ") ");
      }
    }
    run(sql);
  }
}

}  // namespace ParserTest
