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

package org.apache.impala.catalog.metastore;

import static org.apache.impala.catalog.metastore.HmsApiNameEnum.GET_PARTITION_BY_NAMES;

import org.apache.hadoop.hive.metastore.api.GetFieldsRequest;
import org.apache.hadoop.hive.metastore.api.GetFieldsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetSchemaRequest;
import org.apache.hadoop.hive.metastore.api.GetSchemaResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsResponse;
import org.apache.hadoop.hive.metastore.api.SeedTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.SeedTxnIdRequest;
import org.apache.impala.catalog.*;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This class implements the HMS APIs that are served by CatalogD
 * and is exposed via {@link CatalogMetastoreServer}.
 * HMS APIs that are redirected to HMS can be found in {@link MetastoreServiceHandler}.
 *
 */
public class CatalogMetastoreServiceHandler extends MetastoreServiceHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(CatalogMetastoreServiceHandler.class);

  public CatalogMetastoreServiceHandler(CatalogOpExecutor catalogOpExecutor,
      boolean fallBackToHMSOnErrors) {
    super(catalogOpExecutor, fallBackToHMSOnErrors);
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest getTableRequest)
      throws MetaException, NoSuchObjectException, TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache()) {
      return super.get_table_req(getTableRequest);
    }

    try {
      LOG.trace("Received get_Table_req for {}. File metadata is {}",
          getTableRequest.getTblName(), getTableRequest.isGetFileMetadata());
      return CatalogHmsAPIHelper.getTableReq(catalog_, defaultCatalogName_,
          getTableRequest);
    } catch (Exception e) {
      // we catch the CatalogException and fall-back to HMS
      throwIfNoFallback(e, "get_table_req");
    }
    return super.get_table_req(getTableRequest);
  }

  /**
   * This is the main API which is used by Hive to get the partitions. In case of Hive it
   * pushes the pruning logic to HMS by sending over the expression which is used to
   * filter the partitions during query compilation. The expression is specific to Hive
   * and loaded in the runtime based on whether we have hive-exec jar in the classpath
   * or not. If the hive-exec jar is not present in the classpath, we fall-back to HMS
   * since Catalog has no way to deserialize the expression sent over by the client.
   */
  @Override
  public PartitionsByExprResult get_partitions_by_expr(
      PartitionsByExprRequest partitionsByExprRequest) throws TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache()) {
      return super.get_partitions_by_expr(partitionsByExprRequest);
    }

    try {
      // expressionProxy is null or if there were errors when loading the
      // PartitionExpressionProxy.
      if (expressionProxy_ != null) {
        return CatalogHmsAPIHelper.getPartitionsByExpr(
            catalog_, defaultCatalogName_, partitionsByExprRequest, expressionProxy_);
      } else {
        throw new CatalogException("PartitionExpressionProxy could not be initialized");
      }
    } catch (Exception e) {
      // we catch the CatalogException and fall-back to HMS
      throwIfNoFallback(e, HmsApiNameEnum.GET_PARTITION_BY_EXPR.apiName());
    }
    String tblName =
        partitionsByExprRequest.getDbName() + "." + partitionsByExprRequest.getTblName();
    LOG.info(String
        .format(HMS_FALLBACK_MSG_FORMAT, HmsApiNameEnum.GET_PARTITION_BY_EXPR.apiName(),
            tblName));
    return super.get_partitions_by_expr(partitionsByExprRequest);
  }

  /**
   * HMS API to get the partitions filtered by a provided list of names. The request
   * contains a list of partitions names which the client is interested in. Catalog
   * returns the partitions only for requested names. Additionally, this API also returns
   * the file-metadata for the returned partitions if the request has
   * {@code getFileMetadata} flag set. In case of errors, this API falls back to HMS if
   * {@code fallBackToHMSOnErrors_} is set.
   */
  @Override
  public GetPartitionsByNamesResult get_partitions_by_names_req(
      GetPartitionsByNamesRequest getPartitionsByNamesRequest) throws TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache()) {
      return super.get_partitions_by_names_req(getPartitionsByNamesRequest);
    }

    try {
      return CatalogHmsAPIHelper
          .getPartitionsByNames(catalog_, serverConf_, getPartitionsByNamesRequest);
    } catch (Exception ex) {
      throwIfNoFallback(ex, GET_PARTITION_BY_NAMES.apiName());
    }
    String tblName =
        getPartitionsByNamesRequest.getDb_name() + "." + getPartitionsByNamesRequest
            .getTbl_name();
    LOG.info(String.format(HMS_FALLBACK_MSG_FORMAT, GET_PARTITION_BY_NAMES, tblName));
    return super.get_partitions_by_names_req(getPartitionsByNamesRequest);
  }

  @Override
  public void alter_database(String dbname, Database database)
      throws MetaException, NoSuchObjectException, TException {
    /**
     * fallback to default HMS implementation if
     i) catalogdHMS cache is disabled
     or
     ii) syncToLatestEventID_ is false
     **/
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.alter_database(dbname, database);
    }

    String apiName = HmsApiNameEnum.ALTER_DATABASE.apiName();
    Db db = catalog_.getDb(dbname);
    if (db == null) {
      rethrowException(new CatalogException("Database: " +
              dbname + " does not exist in cache"),
          HmsApiNameEnum.ALTER_DATABASE);
    }
    if (!catalog_.tryLockDb(db)) {
      rethrowException(
          new CatalogException("Couldn't acquire write lock on db: " + db.getName()) +
              " when executing HMS API: " + apiName);
    }
    // long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();

    try {
      super.alter_database(dbname, database);
      LOG.debug("Successfully executed HMS API: " + apiName);
      catalogOpExecutor_.syncToLatestEventId(Db db, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      db.getLock().unlock();
    }
  }

  @Override
  public void alter_table(String dbname, String tblName,
      org.apache.hadoop.hive.metastore.api.Table newTable) throws
      InvalidOperationException, MetaException, TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.alter_table(dbname, tblName, newTable);
    }

    String apiName = HmsApiNameEnum.ALTER_TABLE.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname, tblName,
        apiName);

    boolean isRename = !dbname.equalsIgnoreCase(newTable.getDbName()) ||
        !tblName.equalsIgnoreCase(newTable.getTableName());

    if (!isRename) {
      // release lock if it is not rename
      // For rename operation the lock would
      // get released in finally block
      catalog_.getLock().writeLock().unlock();
    }

    // TODO: Handle rename table differently
    try {
      // perform HMS operation
      super.alter_table(dbname, tblName, newTable);
      LOG.debug("Successfully executed HMS api:" + apiName);
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      // release version write lock if rename operation
      if (isRename) {
        catalog_.getLock().writeLock().unlock();
      }
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.add_partition(partition);
    }
    String apiName = HmsApiNameEnum.ADD_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(partition.getDbName(),
        partition.getTableName(), apiName);
    catalog_.getLock().writeLock().unlock();
    Partition addedPartition = super.add_partition(partition);
    LOG.debug("Successfully executed HMS API:" + apiName);

    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return addedPartition;
  }

  @Override
  public Partition append_partition(String dbname, String tblName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.append_partition(dbname, tblName, partVals);
    }
    String apiName = HmsApiNameEnum.APPEND_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    Partition partition = super.append_partition(dbname, tblName, partVals);
    LOG.debug("Successfully executed HMS API: append_partition");
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return partition;
  }

  @Override
  public boolean drop_partition(String dbname, String tblname, List<String> partVals,
                                boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.drop_partition(dbname, tblname, partVals, deleteData);
    }
    String apiName = HmsApiNameEnum.DROP_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblname, apiName);
    catalog_.getLock().writeLock().unlock();
    boolean resp = super.drop_partition(dbname, tblname, partVals, deleteData);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return resp;
  }

  @Override
  public boolean drop_partition_with_environment_context(String dbname, String tblname,
      List<String> partNames, boolean deleteData, EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.drop_partition_with_environment_context(dbname, tblname, partNames,
          deleteData, environmentContext);
    }

    String apiName = HmsApiNameEnum.DROP_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblname, apiName);
    catalog_.getLock().writeLock().unlock();
    boolean resp = super.drop_partition_with_environment_context(dbname, tblname,
        partNames, deleteData, environmentContext);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return resp;
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecMap,
                                      String sourcedb, String sourceTbl,
                                      String destDb, String destTbl)
      throws TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.exchange_partition(partitionSpecMap, sourcedb,
          sourceTbl, destDb, destTbl);
    }

    // acquire lock on multiple tables at once
    String apiName = HmsApiNameEnum.EXCHANGE_PARTITIONS.apiName();
    Table srcTbl, destinationTbl;
    try {
      srcTbl = catalogOpExecutor_.getExistingTable(sourcedb, sourceTbl, apiName);
      destinationTbl = catalogOpExecutor_.getExistingTable(destDb, destTbl, apiName);
    } catch (Exception e) {
      rethrowException(e, apiName);
    }

    if (!catalog_.tryWriteLock(new Table[] {srcTbl, destinationTbl})) {
      rethrowException(new CatalogException("Couldn't acquire lock on tables: "
          + srcTbl.getFullName() + ", " + destinationTbl.getFullName()), apiName);
    }

    Partition exchangedPartition = super.exchange_partition(partitionSpecMap, sourcedb,
        sourceTbl, destDb, destTbl);

    try {
      // TODO: Check if HMS events are generated for
      // both source and dest table. If yes,
      // sync both of them to latest event id
      //catalogOpExecutor_.syncToLatestEventId(srcTbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      srcTbl.releaseWriteLock();
      destinationTbl.releaseWriteLock();
    }
    return exchangedPartition;
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.alter_partition(dbName, tblName, partition);
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.alter_partition(dbName, tblName, partition);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.alter_partitions(dbName, tblName, partitions);
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITIONS.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.super.alter_partitions(dbName, tblName, partitions);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public void alter_partitions_with_environment_context(String dbName, String tblName,
      List<Partition> list, EnvironmentContext environmentContext) throws
      InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.alter_partitions(dbName, tblName, list, environmentContext);
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITIONS_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.alter_partitions(dbName, tblName, list, environmentContext);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public AlterPartitionsResponse alter_partitions_req(
      AlterPartitionsRequest alterPartitionsRequest)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.alter_partitions_req(alterPartitionsRequest);
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITIONS_REQ.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    AlterPartitionsResponse response =
        super.alter_partitions_req(alterPartitionsRequest);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return response;
  }

  @Override
  public void alter_partition_with_environment_context(String dbName, String tblName,
      Partition partition, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.alter_partition_with_environment_context(dbName, tblName,
          partition, environmentContext);
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.alter_partition_with_environment_context(dbName, tblName,
        partition, environmentContext);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public void rename_partition(String dbName, String tblName, List<String> list,
      Partition partition) throws InvalidOperationException,
      MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.rename_partition(dbName, tblName, list, partition);
    }
    String apiName = HmsApiNameEnum.RENAME_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.rename_partition(dbName, tblName, list, partition);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public RenamePartitionResponse rename_partition_req(
      RenamePartitionRequest renamePartitionRequest)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.rename_partition_req(renamePartitionRequest);
    }
    String apiName = HmsApiNameEnum.RENAME_PARTITION_REQ.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    RenamePartitionResponse response =
        super.rename_partition_req(renamePartitionRequest);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return response;
  }

  public void drop_table_with_environment_context(String dbname, String tblname,
      boolean deleteData, EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.drop_table_with_environment_context(dbname, tblname,
          deleteData, environmentContext);
    }
    String apiName = HmsApiNameEnum.DROP_TABLE_WITH_ENVORONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.drop_partition_by_name_with_environment_context(dbname, tblname,
        deleteData, environmentContext);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public void truncate_table(String dbName, String tblName, List<String> partNames)
      throws MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.truncate_table(dbName, tblName, partNames);
    }
    String apiName = HmsApiNameEnum.TRUNCATE_TABLE.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.truncate_table(dbName, tblName, partNames);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public TruncateTableResponse truncate_table_req(
      TruncateTableRequest req) throws MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !syncToLatestEventId_) {
      return super.truncate_table_req(req);
    }
    String apiName = HmsApiNameEnum.TRUNCATE_TABLE_REQ.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    TruncateTableResponse response = super.truncate_table_req(req);
    try {
      catalogOpExecutor_.syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return response;
  }


  private org.apache.impala.catalog.Table getTableAndAcquireWriteLock(String dbName,
      String tblName, String apiName) throws TException {
    org.apache.impala.catalog.Table tbl;
    try {
      tbl = catalogOpExecutor_.getExistingTable(dbName, tblName, apiName);
    } catch (Exception e) {
      rethrowException(e, apiName);
    }
    if (!catalog_.tryWriteLock(tbl)) {
      // should it be an internal exception?
      CatalogException e =
          new CatalogException("Could not acquire lock on table: " +
              tbl.getFullName());
      rethrowException(e, apiName));
    }
    return tbl;
  }
}
