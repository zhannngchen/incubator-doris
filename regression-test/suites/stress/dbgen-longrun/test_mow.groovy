// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.nio.file.Files
import java.nio.file.Paths
import java.net.URL
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

suite("test_mow", "db_gen") {
    // get doris-db from s3
    def dirPath = context.file.parent
    def fileName = "doris-dbgen"
    def fileUrl = "http://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/doris-dbgen"
    def filePath = Paths.get(dirPath, fileName)
    if (!Files.exists(filePath)) {
        new URL(fileUrl).withInputStream { inputStream ->
            Files.copy(inputStream, filePath)
        }
        def file = new File(dirPath + "/" + fileName)
        file.setExecutable(true)
    }

    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://locahost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://locahost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    String feHttpAddress = context.config.feHttpAddress
    def http_port = feHttpAddress.substring(feHttpAddress.indexOf(":") + 1)

    String realDb = context.config.getDbNameByFile(context.file)
    String user = context.config.jdbcUser
    String password = context.config.jdbcPassword

    def bulkSize = 10000 // batch size 
    def tableName = "mow_table"

    def create_table_with_seq = """
        CREATE TABLE `${tableName}` (
                `c0` varchar(100) NOT NULL,
                `c1` int NULL,
                `c2` text NULL ,
                `c3` int NULL,
                `c4` text NULL,
                `c5` text NULL,
                `c6` varchar(20) NULL,
                `c7` varchar(50) NULL,
                `c8` varchar(50) NULL,
                `c9` date NULL,
                `c10` varchar(50) NULL,
                `c11` date NULL,
                `c12` text NULL,
                `c13` text NULL,
                `c14` bitmap NULL,
                `c15` map<int, int> NULL,
                `c16` varchar(10) NULL,
                `c17` tinyint(4) NULL,
                `c18` struct<s_id:int(11), s_date:datetime> NULL,
                `c19` varchar(50) NULL,
                `c20` varchar(50) NULL,
                `c21` datetime NULL,
                `c22` datetime NULL)
                ENGINE=OLAP
                UNIQUE KEY(`c0`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`c0`) BUCKETS 8
                PROPERTIES (
                    "replication_num" = "3",
                    "enable_unique_key_merge_on_write" = "true",
                    "function_column.sequence_col" = "c11"
                ); """ 

    def create_table_without_seq = """
        CREATE TABLE `${tableName}` (
                `c0` varchar(100) NOT NULL,
                `c1` int NULL,
                `c2` text NULL ,
                `c3` int NULL,
                `c4` text NULL,
                `c5` text NULL,
                `c6` varchar(20) NULL,
                `c7` varchar(50) NULL,
                `c8` varchar(50) NULL,
                `c9` date NULL,
                `c10` varchar(50) NULL,
                `c11` date NULL,
                `c12` text NULL,
                `c13` text NULL,
                `c14` bitmap NULL,
                `c15` map<int, int> NULL,
                `c16` varchar(10) NULL,
                `c17` tinyint(4) NULL,
                `c18` struct<s_id:int(11), s_date:datetime> NULL,
                `c19` varchar(50) NULL,
                `c20` varchar(50) NULL,
                `c21` datetime NULL,
                `c22` datetime NULL)
                ENGINE=OLAP
                UNIQUE KEY(`c0`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`c0`) BUCKETS 8
                PROPERTIES (
                    "replication_num" = "3",
                    "enable_unique_key_merge_on_write" = "true"
                );
        """

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql create_table_without_seq

    def sucess = new AtomicBoolean()
    def finish = new AtomicBoolean()
    sucess.set(true)
    finish.set(false)

    // count(*)
    Thread.startDaemon {
        while (sucess.get()) {
            try {
                def total = sql """ select count(*)  from ${tableName} """
                logger.info("total:" + total)
            } catch (Throwable t) {
                log.error("failed to select count *", t)
            }
            Thread.sleep(5000)
        }
    }

    // truncate table to avoid overfilling the disk
    Thread.startDaemon {
        def i = 0
        while (sucess.get()) {
            Thread.sleep(3600*6*1000) // drop table every 6 hours
            try {
                sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
                if (i % 2 == 0) {
                    sql create_table_with_seq
                } else {
                    sql create_table_without_seq
                }
                i++
                logger.info("drop table")
            } catch (Throwable t) {
                log.error("failed to drop table", t)
            }
        }
    }

    // delete predicate
    Thread.startDaemon {
        while (true) {
            try {
                sql """ delete  from ${tableName} where c0 < 'abcd'"""
            } catch (Throwable t) {
                log.error("faild to delete", t)
            }
            Thread.sleep(600000)
        }
    }

    // check duplicate keys
    def checkDupKey = { table, keys, count ->
        while (count < 0 || count-- > 0) {
            try {
                def res = sql """ select ${keys}, count(*) a from ${table} group by ${keys} having a > 1 """
                logger.info("dup key result:" + res)
                // result should be empty
                if (res[0]) {
                     sucess.set(false)
                     logger.error("duplicate key occur")
                     assertTrue(false)
                }
            } catch (Throwable t) {
                log.error("select duplicate key failed", t)
            }
            Thread.sleep(5000)
        }
    }

    def keyCols = "c0"
    Thread.startDaemon {
        checkDupKey(tableName, keyCols, -1)
    }

    // delete sign
    Thread.startDaemon {
        while (sucess.get()) {
            Thread.sleep(600000) // delete sign every 10m
            def cm
            def rows = 4 * bulkSize;
            if (password) {
                cm = """${context.file.parent}/doris-dbgen gen  --no-progress
                        --host ${sql_ip} --sql-port ${sql_port} --user ${user} --pass ${password} --database ${realDb}
                        --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                        --http-port ${http_port} --max-threads 4 --merge-type delete
                     """
            } else { 
                cm = """${context.file.parent}/doris-dbgen gen  --no-progress
                        --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb}
                        --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                        --http-port ${http_port} --max-threads 4 --merge-type delete
                     """
            }
            logger.info("command is: " + cm)
            def proc = cm.execute()
            def sout = new StringBuilder(), serr = new StringBuilder()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(1000*1000) // millisecond
            logger.info("std out: " + sout + "std err: " + serr)
        }
    }

    GetDebugPoint().enableDebugPointForAllBEs('Tablet.update_delete_bitmap_without_lock.random_failed', [percent: 0.1])


    // doris-dbgen load data
    def curRows = 0;
    while (sucess.get()) {
        def cm
        def rows = 4 * bulkSize;
        if (password) {
            cm = """${context.file.parent}/doris-dbgen gen  --no-progress
                    --host ${sql_ip} --sql-port ${sql_port} --user ${user} --pass ${password} --database ${realDb}
                    --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                    --http-port ${http_port} --max-threads 4
                 """
        } else { 
            cm = """${context.file.parent}/doris-dbgen gen  --no-progress
                    --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb}
                    --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                    --http-port ${http_port} --max-threads 4
                 """
        }
        logger.info("command is: " + cm)
        def proc = cm.execute()
        def sout = new StringBuilder(), serr = new StringBuilder()
        proc.consumeProcessOutput(sout, serr)
        proc.waitForOrKill(600*1000) // millisecond
        logger.info("std out: " + sout + "std err: " + serr)
        curRows += rows;
        logger.info("total load rows: " + curRows)
    }

    log.error("shouldn't come to here")
    assertTrue(false)
}
