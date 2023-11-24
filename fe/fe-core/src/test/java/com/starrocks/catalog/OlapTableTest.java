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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/OlapTableTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IndexDef;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;
import org.threeten.extra.PeriodDuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class OlapTableTest {

    @Test
    public void testSetIdForRestore() {
        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, KeysType.AGG_KEYS);
        List<Table> tables = db.getTables();
        final long id = 0;
        new MockUp<GlobalStateMgr>() {
            @Mock
            long getNextId() {
                return id;
            }
        };

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            ((OlapTable) table).resetIdsForRestore(GlobalStateMgr.getCurrentState(), db, 3);
        }
    }

    @Test
    public void testTableWithLocalTablet() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.META_VERSION;
            }
        };

        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, KeysType.AGG_KEYS);
        List<Table> tables = db.getTables();

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable tbl = (OlapTable) table;
            tbl.setIndexes(Lists.newArrayList(new Index("index", Lists.newArrayList("col"),
                    IndexDef.IndexType.BITMAP, "xxxxxx")));
            System.out.println("orig table id: " + tbl.getId());

            FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
            tbl.write(out);

            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());
            Table copiedTbl = OlapTable.read(in);
            System.out.println("copied table id: " + copiedTbl.getId());

            Assert.assertTrue(copiedTbl instanceof OlapTable);
            Partition partition = ((OlapTable) copiedTbl).getPartition(3L);
            MaterializedIndex newIndex = partition.getIndex(4L);
            for (Tablet tablet : newIndex.getTablets()) {
                Assert.assertTrue(tablet instanceof LocalTablet);
            }
            MvId mvId1 = new MvId(db.getId(), 10L);
            tbl.addRelatedMaterializedView(mvId1);
            MvId mvId2 = new MvId(db.getId(), 20L);
            tbl.addRelatedMaterializedView(mvId2);
            MvId mvId3 = new MvId(db.getId(), 30L);
            tbl.addRelatedMaterializedView(mvId3);
            Assert.assertEquals(Sets.newHashSet(10L, 20L, 30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId1);
            tbl.removeRelatedMaterializedView(mvId2);
            Assert.assertEquals(Sets.newHashSet(30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId3);
            Assert.assertEquals(Sets.newHashSet(), tbl.getRelatedMaterializedViews());
        }
    }

    @Test
    public void testCopyOnlyForQuery() {
        OlapTable olapTable = new OlapTable();
        olapTable.setHasDelete();

        OlapTable copied = new OlapTable();
        olapTable.copyOnlyForQuery(copied);

        Assert.assertEquals(olapTable.hasDelete(), copied.hasDelete());
        Assert.assertEquals(olapTable.hasForbitGlobalDict(), copied.hasForbitGlobalDict());
        Assert.assertEquals(olapTable, copied);
    }

    @Test
    public void testFilePathInfo() {
        OlapTable olapTable = new OlapTable();
        Assert.assertNull(olapTable.getDefaultFilePathInfo());
        Assert.assertNull(olapTable.getPartitionFilePathInfo(10));
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Assert.assertNull(olapTable.getDefaultFilePathInfo());
        Assert.assertNull(olapTable.getPartitionFilePathInfo(10));
    }

    @Test
    public void testNullDataCachePartitionDuration() {
        OlapTable olapTable = new OlapTable();
        Assert.assertNull(olapTable.getTableProperty() == null ? null : 
                olapTable.getTableProperty().getDataCachePartitionDuration());
    }

    public void testMVPartitionDurationTimeUintMismatchFailed() {
        OlapTable olapTable = new OlapTable();
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        new MockUp<TableProperty>() {
            @Mock
            PeriodDuration getDataCachePartitionDuration() {
                String durationStr = "7 hour";
                return TimeUtils.parseHumanReadablePeriodOrDuration(durationStr);
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            PartitionInfo getPartitionInfo() {
                PartitionInfo partitionInfo = new RangePartitionInfo();
                return partitionInfo;
            };
        };
        new MockUp<PartitionInfo>() {
            @Mock
            boolean isRangePartition() {
                return true;
            };
        };
        new MockUp<RangePartitionInfo>() {
            @Mock
            Range<PartitionKey> getRange(long partitionId) {
                PartitionKey p1 = new PartitionKey();
                p1.pushColumn(new DateLiteral(2022, 11, 1), PrimitiveType.DATE);

                PartitionKey p2 = new PartitionKey();
                p2.pushColumn(new DateLiteral(2022, 11, 2), PrimitiveType.DATE);

                return Range.openClosed(p1, p2);
            };
            @Mock
            boolean isPartitionedBy(PrimitiveType type) {
                return (type == PrimitiveType.DATE);
            };
        };
        Partition partition = new Partition(1, "p1", null, null);
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition));
    }

    @Test
    public void testMVPartitionDurationTimeUintMismatchSucceeded() {
        OlapTable olapTable = new OlapTable();
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        new MockUp<TableProperty>() {
            @Mock
            PeriodDuration getDataCachePartitionDuration() {
                String durationStr = "2 hour";
                return TimeUtils.parseHumanReadablePeriodOrDuration(durationStr);
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            PartitionInfo getPartitionInfo() {
                PartitionInfo partitionInfo = new RangePartitionInfo();
                return partitionInfo;
            };
        };
        new MockUp<PartitionInfo>() {
            @Mock
            boolean isRangePartition() {
                return true;
            };
        };
        new MockUp<RangePartitionInfo>() {
            @Mock
            Range<PartitionKey> getRange(long partitionId) {
                PartitionKey p1 = new PartitionKey();
                p1.pushColumn(new DateLiteral(2022, 11, 1), PrimitiveType.DATE);

                PartitionKey p2 = new PartitionKey();
                p2.pushColumn(new DateLiteral(2023, 11, 1), PrimitiveType.DATE);

                return Range.openClosed(p1, p2);
            };
            @Mock
            boolean isPartitionedBy(PrimitiveType type) {
                return (type == PrimitiveType.DATE);
            };
        };
        new MockUp<LocalDate>() {
            @Mock
            LocalDate now() {
                return LocalDate.of(2023, 10, 8);
            };
        };
        new MockUp<LocalDateTime>() {
            @Mock
            LocalDateTime now() {
                return LocalDateTime.of(2023, 10, 8, 11, 20, 34, 000000);
            };
        };

        Partition partition = new Partition(1, "p1", null, null);
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition));
    }
}
