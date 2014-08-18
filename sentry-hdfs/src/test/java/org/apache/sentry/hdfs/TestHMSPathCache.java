/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.HMSPathCache;
import org.apache.sentry.hdfs.HMSUpdate;
import org.junit.Test;
public class TestHMSPathCache {

  @Test
  public void testPathAddRemove() {
    HMSPathCache c = new HMSPathCache(null, 10000, 0, "/");
    c.addPathsForAuthzObj("db1", "/db1");
    c.addPathsForAuthzObj("db1.tbl1", new String[]{"/db1/tbl1", "/db1/tbl1/p11", "/db1/tbl1/p12"});
    c.addPathsForAuthzObj("db1.tbl2", new String[]{"/db1/tbl2"});

    assertEquals("db1.tbl1", c.retrieveAuthzObjFromPath("/db1/tbl1/p11", true));
    assertEquals("db1.tbl2", c.retrieveAuthzObjFromPath("/db1/tbl2", true));
    c.removePathsForAuthzObj("db1.tbl1", "/db1/tbl1", "/db1/tbl1/p11");
    assertEquals("db1", c.retrieveAuthzObjFromPath("/db1/tbl1/p11", false));
    assertNull(c.retrieveAuthzObjFromPath("/db1/tbl1/p11", true));
  }

  @Test
  public void testUpdateHandling() throws Exception {
    DummyHMSClient mock = new DummyHMSClient();
    Database db1 = mock.addDb("db1", "/db1");
    Table tbl11 = mock.addTable(db1, "tbl11", "/db1/tbl11");
    mock.addPartition(db1, tbl11, "/db1/tbl11/part111");
    mock.addPartition(db1, tbl11, "/db1/tbl11/part112");
    HMSPathCache hmsCache = new HMSPathCache(mock, 10000, 0, "/");

    // Trigger Initial refresh (full dump)
    hmsCache.handleUpdateNotification(new HMSUpdate(10, null));
    waitToCommit(hmsCache);
    assertEquals("db1.tbl11", hmsCache.retrieveAuthzObjFromPath("/db1/tbl11/part111", true));
    assertEquals("db1.tbl11", hmsCache.retrieveAuthzObjFromPath("/db1/tbl11/part112", true));

    // Handle update from HMS plugin
    HMSUpdate update = new HMSUpdate(11, null);
    update.addPathUpdate("db1.tbl12").addPath("/db1/tbl12").addPath("/db1/tbl12/part121");
    update.addPathUpdate("db1.tbl11").delPath("/db1/tbl11/part112");

    // Ensure JSON serialization is working :
    assertEquals(HMSUpdate.toJsonString(update), 
        HMSUpdate.toJsonString(
            HMSUpdate.fromJsonString(
                HMSUpdate.toJsonString(update))));

    hmsCache.handleUpdateNotification(update);
    waitToCommit(hmsCache);
    assertNull(hmsCache.retrieveAuthzObjFromPath("/db1/tbl11/part112", true));
    assertEquals("db1.tbl11", hmsCache.retrieveAuthzObjFromPath("/db1/tbl11/part112", false));
    assertEquals("db1.tbl12", hmsCache.retrieveAuthzObjFromPath("/db1/tbl12/part121", true));

    // Add more entries to HMS
    Table tbl13 = mock.addTable(db1, "tbl13", "/db1/tbl13");
    mock.addPartition(db1, tbl13, "/db1/tbl13/part131");

    // Simulate missed update (Send empty update with seqNum 13)
    // On missed update, refresh again
    hmsCache.handleUpdateNotification(new HMSUpdate(13, null));
    waitToCommit(hmsCache);
    assertEquals("db1.tbl13", hmsCache.retrieveAuthzObjFromPath("/db1/tbl13/part131", true));
  }

  @Test
  public void testGetUpdatesFromSrcCache() throws InterruptedException {
    DummyHMSClient mock = new DummyHMSClient();
    Database db1 = mock.addDb("db1", "/db1");
    Table tbl11 = mock.addTable(db1, "tbl11", "/db1/tbl11");
    mock.addPartition(db1, tbl11, "/db1/tbl11/part111");
    mock.addPartition(db1, tbl11, "/db1/tbl11/part112");

    // This would live in the Sentry Service
    HMSPathCache srcCache = new HMSPathCache(mock, 10000, 10, "/");

    // Trigger Initial full Image fetch
    srcCache.handleUpdateNotification(new HMSUpdate(10, null));
    waitToCommit(srcCache);

    // This entity would live in the NN plugin : a downstream cache with no updateLog
    HMSPathCache destCache = new HMSPathCache(mock, 10000, 0, "/");

    // Adapter to pull updates from upstream cache to downstream Cache
    DummyAdapter adapter = new DummyAdapter(destCache, srcCache);
    adapter.getDestToPullUpdatesFromSrc();
    waitToCommit(destCache);
    // Check if NN plugin received the updates from Sentry Cache
    assertEquals("db1.tbl11", destCache.retrieveAuthzObjFromPath("/db1/tbl11/part111", true));
    assertEquals("db1.tbl11", destCache.retrieveAuthzObjFromPath("/db1/tbl11/part112", true));

    // Create Upsteram HMS update
    HMSUpdate update = new HMSUpdate(11, null);
    update.addPathUpdate("db1.tbl12").addPath("/db1/tbl12").addPath("/db1/tbl12/part121");
    update.addPathUpdate("db1.tbl11").delPath("/db1/tbl11/part112");

    // Send Update to Upstream Cache
    srcCache.handleUpdateNotification(update);
    waitToCommit(srcCache);
    // Pull update to downstream Cache
    adapter.getDestToPullUpdatesFromSrc();
    waitToCommit(destCache);

    assertNull(destCache.retrieveAuthzObjFromPath("/db1/tbl11/part112", true));
    assertEquals("db1.tbl11", destCache.retrieveAuthzObjFromPath("/db1/tbl11/part112", false));
    assertEquals("db1.tbl12", destCache.retrieveAuthzObjFromPath("/db1/tbl12/part121", true));
  }

  @Test
  public void testPathSerialization() throws InterruptedException {
    DummyHMSClient mock = new DummyHMSClient();
    Database db1 = mock.addDb("db1", "/db1");
    Table tbl11 = mock.addTable(db1, "tbl11", "/db1/tbl11");
    mock.addPartition(db1, tbl11, "/db1/tbl11/part111");
    mock.addPartition(db1, tbl11, "/db1/tbl11/part112");
    HMSPathCache hmsCache = new HMSPathCache(mock, 10000, 0, "/");

    // Initial refresh
    hmsCache.handleUpdateNotification(new HMSUpdate(10, null));
    waitToCommit(hmsCache);
    String ser = hmsCache.serializeAllPaths();
    assertTrue("/[db1#db1[tbl11#db1.tbl11[part112#db1.tbl11[]part111#db1.tbl11[]]]]"
        .equals(ser)
        || "/[db1#db1[tbl11#db1.tbl11[part112#db1.tbl11[]part112#db1.tbl11[]]]]"
            .equals(ser));
  }

  private void waitToCommit(HMSPathCache hmsCache) throws InterruptedException {
    int counter = 0;
    while(!hmsCache.areAllUpdatesCommited()) {
      Thread.sleep(200);
      counter++;
      if (counter > 10000) {
        fail("Updates taking too long to commit !!");
      }
    }
  }
}
