/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.service.cache;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public class ExtendedMetastoreClient implements MetastoreClient {

  private final HiveMetaStoreClient client;
  public ExtendedMetastoreClient(HiveMetaStoreClient hmsClient) {
    this.client = hmsClient;
  }

  @Override
  public List<Database> getAllDatabases() {
    List<Database> retList = new ArrayList<Database>();
    try {
      for (String dbName : client.getAllDatabases()) {
        retList.add(client.getDatabase(dbName));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return retList;
  }

  @Override
  public List<Table> getAllTablesOfDatabase(Database db) {
    List<Table> retList = new ArrayList<Table>();
    try {
      for (String tblName : client.getAllTables(db.getName())) {
        retList.add(client.getTable(db.getName(), tblName));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return retList;
  }

  @Override
  public List<Partition> listAllPartitions(Database db, Table tbl) {
    try {
      return client.listPartitions(db.getName(), tbl.getTableName(),
          Short.MAX_VALUE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
