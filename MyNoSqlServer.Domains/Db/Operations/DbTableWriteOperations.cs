using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Domains.Db.Operations
{
    public class DbTableWriteOperations
    {
        private readonly IReplicaSynchronizationService _dataSynchronizer;
        private readonly PersistenceHandler _persistenceHandler;

        public DbTableWriteOperations(IReplicaSynchronizationService dataSynchronizer, PersistenceHandler persistenceHandler)
        {
            _dataSynchronizer = dataSynchronizer;
            _persistenceHandler = persistenceHandler;
        }
        
        public ValueTask<OperationResult> InsertAsync(DbTable dbTable, DynamicEntity entityToInsert,
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            
            if (string.IsNullOrEmpty(entityToInsert.PartitionKey))
                return new ValueTask<OperationResult>(OperationResult.PartitionKeyIsNull);

            if (string.IsNullOrEmpty(entityToInsert.RowKey))
                return new ValueTask<OperationResult>(OperationResult.RowKeyIsNull);

            if (dbTable.HasRecord(entityToInsert))
                return new ValueTask<OperationResult>(OperationResult.RecordExists);


            DbPartition dbPartition = null;
            DbRow dbRow = null;
            var result = OperationResult.RecordNotFound;
            
            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                dbPartition = dbTableWriter.GetOrCreatePartition(entityToInsert.PartitionKey);

                if (dbPartition.HasRecord(entityToInsert.RowKey))
                {
                    result = OperationResult.RecordExists;
                    return false;
                }
                    
                
                dbRow = DbRow.CreateNew(entityToInsert, now);

                if (!dbPartition.Insert(dbRow, now))
                {
                    result = OperationResult.RecordExists;
                    return false;
                }

                result = OperationResult.Ok;
                return true;
            });
            
            if (result != OperationResult.Ok)
                return new ValueTask<OperationResult>(result);
            
            _dataSynchronizer.SynchronizeUpdate(dbTable, new[] {dbRow});

            return _persistenceHandler
                .SynchronizePartitionAsync(dbTable, dbPartition, synchronizationPeriod, snapshotDateTime)
                .ReturnValueTaskResult(OperationResult.Ok);
        }
        
        public ValueTask<OperationResult> InsertOrReplaceAsync(DbTable dbTable, DynamicEntity entity, 
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {

            if (string.IsNullOrEmpty(entity.PartitionKey))
                return new ValueTask<OperationResult>(OperationResult.PartitionKeyIsNull);

            if (string.IsNullOrEmpty(entity.RowKey))
                return new ValueTask<OperationResult>(OperationResult.RowKeyIsNull);
            
            
            DbPartition dbPartition = null;
            DbRow dbRow = null;
            
            
            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                dbPartition = dbTableWriter.GetOrCreatePartition(entity.PartitionKey);
                dbRow = DbRow.CreateNew(entity, now);
                dbPartition.InsertOrReplace(dbRow);
                return true;
            });
            
            _dataSynchronizer.SynchronizeUpdate(dbTable, new[]{dbRow});

            return _persistenceHandler
                .SynchronizePartitionAsync(dbTable, dbPartition, synchronizationPeriod, snapshotDateTime)
                .ReturnValueTaskResult(OperationResult.Ok);
            
        }        
        
        public async ValueTask DeleteEntitiesAsync(DbTable table, DbPartition dbPartition, IEnumerable<DbRow> rows)
        {
            var deletedRows = new List<DbRow>();
            
            var snapshotDateTime = table.GetAccessWithWriteLock(dbTableWriter =>
            {
                deletedRows.AddRange(dbPartition.TryDeleteRows(rows.Select(itm => itm.RowKey)));
                return true;
            });

            if (deletedRows.Count == 0) 
                return;
         
            _dataSynchronizer.SynchronizeDelete(table, deletedRows);
            await _persistenceHandler.SynchronizeDeletePartitionAsync(table, dbPartition, DataSynchronizationPeriod.Sec5, snapshotDateTime);
        }


        private ValueTask<OperationResult> ModifyEntity(DbTable dbTable,
            IMyNoSqlDbEntity entityToModify, DataSynchronizationPeriod dataSynchronizationPeriod, 
            Func<DbRow, DbRow> modifyAction)
        {
            if (dbTable.TryGetRow(entityToModify.PartitionKey, entityToModify.RowKey) == null)
                return new ValueTask<OperationResult>(OperationResult.RecordNotFound);

            var result = OperationResult.RecordNotFound;
            DbPartition dbPartition = null;
            DbRow dbRow = null;
            
            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                dbPartition = dbTableWriter.TryGetPartition(entityToModify.PartitionKey);

                var dbRowFromDb = dbPartition?.TryGetRow(entityToModify.RowKey);
                
                if (dbRowFromDb == null)
                    return false;

                if (dbRowFromDb.TimeStamp != entityToModify.TimeStamp)
                {
                    result = OperationResult.RecordChangedConcurrently;
                    return false;
                }
                
                dbRow = modifyAction(dbRowFromDb);

                if (dbRow != null)
                    dbPartition.InsertOrReplace(dbRow);
                
                result = OperationResult.Ok;
                return true;
            });

            if (result == OperationResult.Ok)
            {
                _dataSynchronizer.SynchronizeUpdate(dbTable, new[] {dbRow});

                return _persistenceHandler
                    .SynchronizePartitionAsync(dbTable, dbPartition, dataSynchronizationPeriod, snapshotDateTime)
                    .ReturnValueTaskResult(result);
            }

            return new ValueTask<OperationResult>(result);
        }
        
        
        
        public ValueTask<OperationResult> ReplaceAsync(DbTable dbTable,
            DynamicEntity entityToUpdate, DataSynchronizationPeriod dataSynchronizationPeriod, DateTime now)
        {
            return ModifyEntity(dbTable, 
                entityToUpdate,
                dataSynchronizationPeriod,
                dbRow => DbRow.CreateNew(entityToUpdate, now));
        }
        
        public ValueTask<OperationResult> MergeAsync(DbTable dbTable,
            DynamicEntity entityToUpdate, DataSynchronizationPeriod dataSynchronizationPeriod, DateTime now)
        {
            return ModifyEntity(dbTable, 
                entityToUpdate, 
                dataSynchronizationPeriod,
                dbRow => DbRow.CreateNew(dbRow.MergeEntities(entityToUpdate), now));
        } 
        
        public ValueTask<OperationResult> DeleteAsync(DbTable table, string partitionKey, string rowKey, 
            DataSynchronizationPeriod synchronizationPeriod)
        {
            var entity = table.TryGetRow(partitionKey, rowKey);
            
            if (entity == null)
                return new ValueTask<OperationResult>(OperationResult.RecordNotFound);

            DbPartition dbPartition = null;
            DbRow dbRow = null;

            var snapshotDateTime = table.GetAccessWithWriteLock(dbTableWriter =>
            {
                dbPartition = dbTableWriter.TryGetPartition(partitionKey);
                dbRow = dbPartition.TryDeleteRow(rowKey);
                return true;
            });


            if (dbRow == null) 
                return new ValueTask<OperationResult>(OperationResult.RowNotFound);
         
            _dataSynchronizer.SynchronizeDelete(table, new[]{dbRow});
            
            return _persistenceHandler
                .SynchronizeDeletePartitionAsync(table, dbPartition, 
                    synchronizationPeriod, snapshotDateTime)
                .ReturnValueTaskResult(OperationResult.Ok);
        }
        
        public ValueTask CleanAndKeepLastRecordsAsync(DbTable dbTable,
            string partitionKey, int amount, 
            DataSynchronizationPeriod synchronizationPeriod)
        {

            var dbPartition = dbTable.GetPartitionIfOneHasToBeCleaned(partitionKey, amount);
            
            if (dbPartition == null)
                return new ValueTask();
            
            IReadOnlyList<DbRow> cleanedRows = null;
            
            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                dbPartition = dbTableWriter.TryGetPartition(partitionKey);

                if (dbPartition == null)
                    return false;

                cleanedRows = dbPartition.CleanAndKeepLastRecords(amount);

                return true;
            });

            if (cleanedRows == null) 
                return new ValueTask();
            
            _dataSynchronizer.SynchronizeDelete(dbTable, cleanedRows);
                
            return _persistenceHandler
                .SynchronizePartitionAsync(dbTable, dbPartition, synchronizationPeriod, snapshotDateTime);
        }
        
        public  async ValueTask BulkInsertOrReplaceAsync(
            DbTable dbTable, IEnumerable<DynamicEntity> entitiesToInsert, DataSynchronizationPeriod syncPeriod)
        {
            
            var partitionsToSync = new Dictionary<string, DbPartition>();
            
            var dateTime = DateTime.UtcNow;

            var dbRows = entitiesToInsert
                .Select(entity => DbRow.CreateNew(entity, dateTime))
                .ToList();
            
            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                foreach (var dbRow in dbRows)
                {
                    var dbPartition = dbTableWriter.GetOrCreatePartition(dbRow.PartitionKey);

                    dbPartition.InsertOrReplace(dbRow);
                    
                    if (!partitionsToSync.ContainsKey(dbPartition.PartitionKey))
                        partitionsToSync.Add(dbPartition.PartitionKey, dbPartition);
                }

                return true;
            });
          
            foreach (var dbPartition in partitionsToSync.Values)
            {
                _dataSynchronizer.PublishInitPartition(dbTable, dbPartition.PartitionKey);
                await _persistenceHandler.SynchronizePartitionAsync(dbTable, dbPartition, syncPeriod, snapshotDateTime);
            }
        }


        public ValueTask ClearTableAndBulkInsertAsync(DbTable dbTable,
            IEnumerable<DynamicEntity> entitiesToInsert, DataSynchronizationPeriod syncPeriod)
        {

            var partitionsToSync = new Dictionary<string, DbPartition>();

            var dateTime = DateTime.UtcNow;

            var dbRows = entitiesToInsert
                .Select(entity => DbRow.CreateNew(entity, dateTime))
                .ToList();

            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                dbTableWriter.Clear();
                
                foreach (var dbRow in dbRows)
                {
                    var dbPartition = dbTableWriter.GetOrCreatePartition(dbRow.PartitionKey);
                    
                    dbPartition.InsertOrReplace(dbRow);

                    if (!partitionsToSync.ContainsKey(dbPartition.PartitionKey))
                        partitionsToSync.Add(dbPartition.PartitionKey, dbPartition);
                }

                return true;
            });

            _dataSynchronizer.PublishInitTable(dbTable);
            return _persistenceHandler.SynchronizeTableAsync(dbTable, syncPeriod, snapshotDateTime);
        }
        
        
        public ValueTask ClearPartitionAndBulkInsertOrUpdateAsync(DbTable dbTable,
            string partitionKeyToClear, IEnumerable<DynamicEntity> entitiesToInsert, DataSynchronizationPeriod syncPeriod)
        {

            var partitionsToSync = new Dictionary<string, DbPartition>();
            
            var dateTime = DateTime.UtcNow;
            
            var dbRows = entitiesToInsert
                .Select(entity => DbRow.CreateNew(entity, dateTime))
                .ToList();

            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                var partitionToClear = dbTableWriter.TryGetPartition(partitionKeyToClear);
                if (partitionToClear != null && partitionToClear.GetRecordsCount() > 0)
                {
                    partitionToClear.Clean();
                    if (!partitionsToSync.ContainsKey(partitionToClear.PartitionKey))
                        partitionsToSync.Add(partitionToClear.PartitionKey, partitionToClear);
                }
                
                foreach (var dbRow in dbRows)
                {
                    var dbPartition = dbTableWriter.GetOrCreatePartition(dbRow.PartitionKey);

                    dbPartition.InsertOrReplace(dbRow);
                    if (!partitionsToSync.ContainsKey(dbPartition.PartitionKey))
                        partitionsToSync.Add(dbPartition.PartitionKey, dbPartition);
                }

                return true;
            });

            _dataSynchronizer.PublishInitTable(dbTable);
            return _persistenceHandler.SynchronizeTableAsync(dbTable, syncPeriod, snapshotDateTime);
        }


        public ValueTask ClearTableAsync(DbTable dbTable, DataSynchronizationPeriod syncPeriod)
        {
            var snapshotDateTime = dbTable.Clean();
            _dataSynchronizer.PublishInitTable(dbTable);
            return _persistenceHandler.SynchronizeTableAsync(dbTable, syncPeriod, snapshotDateTime);
        }
        
        
 

        public async ValueTask KeepMaxPartitionsAmountAsync(DbTable dbTable, int amount)
        {
            var partitionsToGc = dbTable.GetPartitionsToGarbageCollect(amount);

            if (partitionsToGc.Count == 0)
                return;
            
            var result = new List<DbPartition>();
            
            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                foreach (var dbPartition in partitionsToGc)
                {
                    
                    if (dbTableWriter.RemovePartition(dbPartition.PartitionKey))
                        result.Add(dbPartition);
                }

                return true;
            });
            
            if (result.Count == 0)
                return;
            
            foreach (var dbPartition in result)
                _dataSynchronizer.PublishInitPartition(dbTable, dbPartition.PartitionKey);

            foreach (var dbPartition in result)
                await _persistenceHandler.SynchronizeDeletePartitionAsync(dbTable, dbPartition, DataSynchronizationPeriod.Sec1, snapshotDateTime);

        }


        internal void UpdateExpirationTime(DbTable dbTable, IReadOnlyList<DbRow> dbRows, UpdateExpirationTime updateExpirationTime)
        {

            var partitionsToSync = new Dictionary<string, DbPartition>();
            
            var snapshotDateTime = dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                foreach (var group in dbRows.GroupBy(itm => itm.PartitionKey))
                {
                    var dbPartition = dbTableWriter.TryGetPartition(group.Key);

                    if (dbPartition == null)
                        return false;

                    foreach (var dbRow in group)
                        dbPartition.UpdateExpirationTime(dbRow.RowKey, updateExpirationTime);

                    if (!partitionsToSync.ContainsKey(dbPartition.PartitionKey))
                        partitionsToSync.Add(dbPartition.PartitionKey, dbPartition);
                }

                return true;
            });


            foreach (var dbPartition in partitionsToSync.Values)
                _persistenceHandler.SynchronizePartitionAsync(dbTable, dbPartition, DataSynchronizationPeriod.Sec5, snapshotDateTime);
            
            _dataSynchronizer.SynchronizeUpdate(dbTable, dbRows);
        }
        
        public void UpdateExpirationTime(DbTable dbTable, string partitionKeys, IReadOnlyList<string> dbRowKeys, 
            in UpdateExpirationTime updateExpirationTime)
        {
            var dbRows = dbTable.GetRows(partitionKeys, dbRowKeys);
            UpdateExpirationTime(dbTable, dbRows, updateExpirationTime);
        }

    }
}