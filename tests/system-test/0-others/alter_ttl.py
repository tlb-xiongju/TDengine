from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

PRIMARY_COL = "ts"

INT_COL = "c_int"
BINT_COL = "c_bint"
SINT_COL = "c_sint"
TINT_COL = "c_tint"
FLOAT_COL = "c_float"
DOUBLE_COL = "c_double"
BOOL_COL = "c_bool"
TINT_UN_COL = "c_utint"
SINT_UN_COL = "c_usint"
BINT_UN_COL = "c_ubint"
INT_UN_COL = "c_uint"
BINARY_COL = "c_binary"
NCHAR_COL = "c_nchar"
TS_COL = "c_ts"

INT_TAG = "t_int"

## insert data args：
TIME_STEP = 10000
NOW = int(datetime.timestamp(datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
DB1     = "db1"
DB2     = "db2"
DB3     = "db3"
DB4     = "db4"
STB_PRE = "stb"
CTB_PRE = "ct"
NTB_PRE = "nt"

"""
varified alter parm ttl,but table does not delete
"""
from ...pytest.util.common import DataSet
from ...pytest.util.log import *
from ...pytest.util.sql import *
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    def __create_stb(self, stb=f"{STB_PRE}1", dbname=DBNAME):
        create_stb_sql = f'''create table {dbname}.{stb}(
                {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        tdSql.execute(create_stb_sql)

    def __create_ntb(self, ntbnum=1, dbname=DBNAME):
        for i in range(ntbnum):
            create_ntb_sql = f'''create table {dbname}.{NTB_PRE}{i+1}(
                    {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
            tdSql.execute(create_ntb_sql)

    def __create_ctb(self, stb=f"{STB_PRE}1", ctb_num=20, dbname=DBNAME):
        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.{CTB_PRE}{i+1} using {dbname}.{stb} tags ( {i+1} )')

    def __insert_data(self, rows, ctb_num=20, dbname=DBNAME, ntb_num=1):
        tdLog.printNoPrefix("==========step: start insert data into tables now.....")
        data = DataSet()
        data.get_order_set(rows)

        for i in range(rows):

            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.utint_data[i]},
                {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}
            '''

            [tdSql.execute( f"insert into {dbname}.{NTB_PRE}{m+1} values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )" ) for m in range(ntb_num)]

            [tdSql.execute( f"insert into {dbname}.{CTB_PRE}{n+1} values ( {NOW - i * TIME_STEP}, {row_data} )" ) for n in range(ctb_num)]

    def run():
        # create db and table, alter ttl
        tdSql.prepare(dbname=DB1)
        tdSql.execute()
        # creare db set ttl = 1
        # create only stable
        # create only stable and child table
        # create only normal table
        # create mix stb and ntb
        # create mix stb/ntb/ctb
        # only create tb has no data
        # create tb and insert data
        # table has wal
        pass


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())