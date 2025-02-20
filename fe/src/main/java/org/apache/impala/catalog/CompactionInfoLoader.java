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

package org.apache.impala.catalog;

import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

/**
 * Class that manages the requests to the Hive Metastore for the latest compaction info.
 * If there is a later request for the same table and same partitions, it can piggyback
 * the response of the first request.
 */
public class CompactionInfoLoader {
  private static final Map<GetLatestCommittedCompactionInfoRequest,
      FutureTask<GetLatestCommittedCompactionInfoResponse>> requests_ =
      new ConcurrentHashMap<>();

  public static GetLatestCommittedCompactionInfoResponse getLatestCompactionInfo(
      CatalogServiceCatalog catalog, GetLatestCommittedCompactionInfoRequest request)
      throws CatalogException {
    FutureTask<GetLatestCommittedCompactionInfoResponse> reqTask =
        new FutureTask<>(() -> {
          try (
              MetaStoreClientPool.MetaStoreClient client = catalog.getMetaStoreClient()) {
            return client.getHiveClient().getLatestCommittedCompactionInfo(request);
          }
        });

    FutureTask<GetLatestCommittedCompactionInfoResponse> existingTask =
        requests_.putIfAbsent(request, reqTask);
    if (existingTask == null) {
      reqTask.run();
    } else {
      reqTask = existingTask;
    }

    try {
      return reqTask.get();
    } catch (Exception e) {
      throw new CatalogException("Error getting latest compaction info for "
              + request.getDbname() + "." + request.getTablename(), e);
    } finally {
      requests_.remove(request, reqTask);
    }
  }
}
