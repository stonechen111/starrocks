// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorTableColumnKey;
import com.starrocks.connector.ConnectorTableColumnStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CachedStatisticStorageTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    public static final String DEFAULT_CREATE_TABLE_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `table_statistic_v1` (\n"
            + "  `table_id` bigint NOT NULL,\n"
            + "  `column_name` varchar(65530) NOT NULL,\n"
            + "  `db_id` bigint NOT NULL,\n"
            + "  `table_name` varchar(65530) NOT NULL,\n"
            + "  `db_name` varchar(65530) NOT NULL,\n"
            + "  `row_count` bigint NOT NULL,\n"
            + "  `data_size` bigint NOT NULL,\n"
            + "  `distinct_count` bigint NOT NULL,\n"
            + "  `null_count` bigint NOT NULL,\n"
            + "  `max` varchar(65530) NOT NULL,\n"
            + "  `min` varchar(65530) NOT NULL,\n"
            + "  `update_time` datetime NOT NULL\n"
            + "  )\n"
            + "ENGINE=OLAP\n"
            + "UNIQUE KEY(`table_id`,  `column_name`, `db_id`)\n"
            + "DISTRIBUTED BY HASH(`table_id`, `column_name`, `db_id`) BUCKETS 2\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"1\",\n"
            + "\"in_memory\" = \"false\"\n"
            + ");";

    public static void createStatisticsTable() throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
        } catch (DdlException e) {
            return;
        }
        starRocksAssert.useDatabase(StatsConstants.STATISTICS_DB_NAME);
        starRocksAssert.withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);

        createStatisticsTable();
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL COMMENT \"\",\n" +
                "  `v4` date NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Mocked
    StatisticExecutor statisticExecutor;

    @Test
    public void testGetColumnStatistic(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");

        new Expectations() {
            {
                cachedStatisticStorage.getColumnStatistic(table, "v1");
                result = ColumnStatistic.builder().setDistinctValuesCount(888).build();
                minTimes = 0;

                cachedStatisticStorage.getColumnStatistic(table, "v2");
                result = ColumnStatistic.builder().setDistinctValuesCount(999).build();
                minTimes = 0;

                cachedStatisticStorage.getColumnStatistic(table, "v3");
                result = ColumnStatistic.builder().setDistinctValuesCount(666).build();
                minTimes = 0;
            }
        };
        ColumnStatistic columnStatistic1 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v1");
        Assert.assertEquals(888, columnStatistic1.getDistinctValuesCount(), 0.001);

        ColumnStatistic columnStatistic2 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v2");
        Assert.assertEquals(999, columnStatistic2.getDistinctValuesCount(), 0.001);

        ColumnStatistic columnStatistic3 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v3");
        Assert.assertEquals(666, columnStatistic3.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testGetColumnStatistics(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder().setDistinctValuesCount(888).build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder().setDistinctValuesCount(999).build();

        new Expectations() {
            {
                cachedStatisticStorage.getColumnStatistics(table, ImmutableList.of("v1", "v2"));
                result = ImmutableList.of(columnStatistic1, columnStatistic2);
                minTimes = 0;
            }
        };
        List<ColumnStatistic> columnStatistics = Deencapsulation
                .invoke(cachedStatisticStorage, "getColumnStatistics", table, ImmutableList.of("v1", "v2"));
        Assert.assertEquals(2, columnStatistics.size());
        Assert.assertEquals(888, columnStatistics.get(0).getDistinctValuesCount(), 0.001);
        Assert.assertEquals(999, columnStatistics.get(1).getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testGetHiveColumnStatistics(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "tpch", "region");

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder().setDistinctValuesCount(888).build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder().setDistinctValuesCount(999).build();
        ConnectorTableColumnStats connectorTableColumnStats1 =
                new ConnectorTableColumnStats(columnStatistic1, 5);
        ConnectorTableColumnStats connectorTableColumnStats2 =
                new ConnectorTableColumnStats(columnStatistic2, 5);

        new Expectations() {
            {
                cachedStatisticStorage.getConnectorTableStatistics(table, ImmutableList.of("r_regionkey", "r_name"));
                result = ImmutableList.of(connectorTableColumnStats1, connectorTableColumnStats2);
                minTimes = 0;
            }
        };
        List<ConnectorTableColumnStats> columnStatistics = Deencapsulation
                .invoke(cachedStatisticStorage, "getConnectorTableStatistics", table,
                        ImmutableList.of("r_regionkey", "r_name"));
        Assert.assertEquals(2, columnStatistics.size());
        Assert.assertEquals(888, columnStatistics.get(0).getColumnStatistic().getDistinctValuesCount(), 0.001);
        Assert.assertEquals(999, columnStatistics.get(1).getColumnStatistic().getDistinctValuesCount(), 0.001);
        Assert.assertEquals(5, columnStatistics.get(0).getRowCount());
        Assert.assertEquals(5, columnStatistics.get(1).getRowCount());
    }

    @Test
    public void testGetConnectorTableStatistics(@Mocked
                                                AsyncLoadingCache<ConnectorTableColumnKey,
                                                        Optional<ConnectorTableColumnStats>> connectorTableCachedStatistics,
                                                @Mocked
                                                CompletableFuture<Map<ConnectorTableColumnKey,
                                                        Optional<ConnectorTableColumnStats>>> res)
            throws ExecutionException, InterruptedException {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");
        List<ConnectorTableColumnKey> cacheKeys =
                ImmutableList.of(new ConnectorTableColumnKey(table.getUUID(), "c1"),
                        new ConnectorTableColumnKey(table.getUUID(), "c2"));

        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnKeyOptionalMap = Maps.newHashMap();
        columnKeyOptionalMap.put(new ConnectorTableColumnKey(table.getUUID(), "c1"),
                Optional.of(new ConnectorTableColumnStats(
                        new ColumnStatistic(0, 10, 0, 20, 5), 5)));
        columnKeyOptionalMap.put(new ConnectorTableColumnKey(table.getUUID(), "c2"),
                Optional.of(new ConnectorTableColumnStats(
                        new ColumnStatistic(0, 100, 0, 200, 50), 50)));

        new Expectations() {
            {
                connectorTableCachedStatistics.getAll(cacheKeys);
                result = res;
                minTimes = 0;

                res.isDone();
                result = true;
                minTimes = 0;

                res.get();
                result = columnKeyOptionalMap;
                minTimes = 0;
            }
        };

        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return true;
            }
        };

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();

        List<ConnectorTableColumnStats> connectorColumnStatistics = Deencapsulation
                .invoke(cachedStatisticStorage, "getConnectorTableStatistics", table,
                        ImmutableList.of("c1", "c2"));
        Assert.assertEquals(2, connectorColumnStatistics.size());

        if (!connectorColumnStatistics.get(0).isUnknown()) {
            Assert.assertEquals(5, connectorColumnStatistics.get(0).getRowCount());
            Assert.assertEquals(0, connectorColumnStatistics.get(0).getColumnStatistic().getMinValue(), 0.0001);
            Assert.assertEquals(10, connectorColumnStatistics.get(0).getColumnStatistic().getMaxValue(), 0.0001);
            Assert.assertEquals(0, connectorColumnStatistics.get(0).getColumnStatistic().getNullsFraction(),
                    0.0001);
            Assert.assertEquals(20, connectorColumnStatistics.get(0).getColumnStatistic().getAverageRowSize(),
                    0.0001);
            Assert.assertEquals(5, connectorColumnStatistics.get(0).getColumnStatistic().getDistinctValuesCount(),
                    0.0001);
        } else {
            Assert.assertEquals(-1, connectorColumnStatistics.get(0).getRowCount());
            Assert.assertEquals(Double.NEGATIVE_INFINITY, connectorColumnStatistics.get(0).getColumnStatistic().getMinValue(),
                    0.0001);
            Assert.assertEquals(Double.POSITIVE_INFINITY, connectorColumnStatistics.get(0).getColumnStatistic().getMaxValue(),
                    0.0001);
            Assert.assertEquals(0.0, connectorColumnStatistics.get(0).getColumnStatistic().getNullsFraction(),
                    0.0001);
            Assert.assertEquals(1.0, connectorColumnStatistics.get(0).getColumnStatistic().getAverageRowSize(),
                    0.0001);
            Assert.assertEquals(1.0, connectorColumnStatistics.get(0).getColumnStatistic().getDistinctValuesCount(),
                    0.0001);
        }
    }

    @Test
    public void testGetConnectorTableStatisticsSync(
            @Mocked AsyncLoadingCache<ConnectorTableColumnKey,
                    Optional<ConnectorTableColumnStats>> connectorTableCachedStatistics,
            @Mocked LoadingCache<ConnectorTableColumnKey,
                    Optional<ConnectorTableColumnStats>> connectorTableTableSyncCachedStatistics) {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");
        List<ConnectorTableColumnKey> cacheKeys =
                ImmutableList.of(new ConnectorTableColumnKey(table.getUUID(), "c1"),
                        new ConnectorTableColumnKey(table.getUUID(), "c2"));

        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnKeyOptionalMap = Maps.newHashMap();
        columnKeyOptionalMap.put(new ConnectorTableColumnKey(table.getUUID(), "c1"),
                Optional.of(new ConnectorTableColumnStats(
                        new ColumnStatistic(0, 10, 0, 20, 5), 5)));
        columnKeyOptionalMap.put(new ConnectorTableColumnKey(table.getUUID(), "c2"),
                Optional.of(new ConnectorTableColumnStats(
                        new ColumnStatistic(0, 100, 0, 200, 50), 50)));

        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return true;
            }
        };

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        List<ConnectorTableColumnStats> connectorColumnStatistics = cachedStatisticStorage.
                getConnectorTableStatisticsSync(table, ImmutableList.of("c1", "c2"));
        Assert.assertEquals(2, connectorColumnStatistics.size());

        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return false;
            }
        };
        connectorColumnStatistics = cachedStatisticStorage.
                getConnectorTableStatisticsSync(table, ImmutableList.of("c1", "c2"));
        Assert.assertEquals(2, connectorColumnStatistics.size());
        Assert.assertTrue(connectorColumnStatistics.get(0).getColumnStatistic().isUnknown());
        Assert.assertTrue(connectorColumnStatistics.get(1).getColumnStatistic().isUnknown());
    }

    @Test
    public void testExpireConnectorTableColumnStatistics() {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        try {
            cachedStatisticStorage.expireConnectorTableColumnStatistics(table, ImmutableList.of("c1", "c2"));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testLoadCacheLoadEmpty(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        Table table = db.getTable("t0");

        new Expectations() {
            {
                cachedStatisticStorage.getColumnStatistic(table, "v1");
                result = ColumnStatistic.unknown();
                minTimes = 0;
            }
        };
        ColumnStatistic columnStatistic =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v1");
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(0.0, columnStatistic.getNullsFraction(), 0.001);
        Assert.assertEquals(1.0, columnStatistic.getAverageRowSize(), 0.001);
        Assert.assertEquals(1.0, columnStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testConvert2ColumnStatistics() {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) db.getTable("t0");
        ColumnBasicStatsCacheLoader cachedStatisticStorage =
                Deencapsulation.newInstance(ColumnBasicStatsCacheLoader.class);

        TStatisticData statisticData = new TStatisticData();
        statisticData.setDbId(db.getId());
        statisticData.setTableId(table.getId());
        statisticData.setColumnName("v1");
        statisticData.setMax("123");
        statisticData.setMin("0");

        ColumnStatistic columnStatistic =
                Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(123, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(0, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMax("2021-05-21");
        statisticData.setMin("2021-05-20");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2021, 5, 21, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2021, 5, 20, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v1");
        statisticData.setMin("aa");
        statisticData.setMax("bb");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v1");
        statisticData.setMin("");
        statisticData.setMax("");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMin("");
        statisticData.setMax("");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMin("");
        statisticData.setMax("");
        statisticData.setRowCount(0);
        statisticData.setDataSize(0);
        statisticData.setNullCount(0);
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assert.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(0, columnStatistic.getAverageRowSize(), 0.001);
        Assert.assertEquals(0, columnStatistic.getNullsFraction(), 0.001);
    }
}
