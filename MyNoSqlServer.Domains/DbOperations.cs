using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.Transactions;

namespace MyNoSqlServer.Domains
{
    public class DbOperations
    {
        private readonly IReplicaSynchronizationService _dataSynchronizer;
        private readonly PersistenceHandler _persistenceHandler;

        public DbOperations(IReplicaSynchronizationService dataSynchronizer, PersistenceHandler persistenceHandler)
        {
            _dataSynchronizer = dataSynchronizer;
            _persistenceHandler = persistenceHandler;
        }


        public async ValueTask<OperationResult> InsertAsync(DbTable table, IMyMemory myMemory,
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            
            var entity = myMemory.ParseDynamicEntity();


            if (string.IsNullOrEmpty(entity.PartitionKey))
                return OperationResult.PartitionKeyIsNull;

            if (string.IsNullOrEmpty(entity.RowKey))
                return OperationResult.RowKeyIsNull;

            if (table.HasRecord(entity))
                return OperationResult.RecordExists;
            
            var (result, dbPartition, dbRow) = table.Insert(entity, now);
            
            if (result != OperationResult.Ok)
                return result;
            
            _dataSynchronizer.SynchronizeUpdate(table, new[] {dbRow});

            await _persistenceHandler.SynchronizePartitionAsync(table, dbPartition.PartitionKey, synchronizationPeriod);
            return OperationResult.Ok;
        }
        


        public async ValueTask<OperationResult> InsertOrReplaceAsync(DbTable table, IMyMemory myMemory, 
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            var entity = myMemory.ParseDynamicEntity();

            if (string.IsNullOrEmpty(entity.PartitionKey))
                return OperationResult.PartitionKeyIsNull;

            if (string.IsNullOrEmpty(entity.RowKey))
                return OperationResult.RowKeyIsNull;
            
            var (dbPartition, dbRow) = table.InsertOrReplace(entity, now);
            
            _dataSynchronizer.SynchronizeUpdate(table, new[]{dbRow});

            await _persistenceHandler.SynchronizePartitionAsync(table, dbPartition.PartitionKey, synchronizationPeriod);

            return OperationResult.Ok;
        }


        public async ValueTask ApplyTransactionsAsync(DbTable table, IEnumerable<IDbTransaction> transactions)
        {
            foreach (var transaction in transactions)
            {
                switch (transaction)
                {
                    case ICleanTableTransaction:
                        table.Clean();
                        await _persistenceHandler.SynchronizeTableAsync(table, DataSynchronizationPeriod.Sec5); 
                        _dataSynchronizer.PublishInitTable(table);
                        break;
                    case ICleanPartitionsTransaction cleanPartitionsTransaction:
                    {
                        var cleaned = table.CleanPartitions(cleanPartitionsTransaction.Partitions);

                        foreach (var dbPartition in cleaned)
                        {
                            await _persistenceHandler.SynchronizePartitionAsync(table, dbPartition.PartitionKey,  DataSynchronizationPeriod.Sec5);
                            _dataSynchronizer.PublishInitPartition(table, dbPartition);
                        }

                        break;
                    }
                    case IDeleteRowsTransaction deleteRows:
                        table.DeleteRows(deleteRows.PartitionKey, deleteRows.RowKeys);
                        await _persistenceHandler.SynchronizePartitionAsync(table, deleteRows.PartitionKey,  DataSynchronizationPeriod.Sec5);
                        break;

                    case IInsertOrReplaceEntitiesTransaction insertOrUpdate:
                    {
                        var updateRows = new Dictionary<string, List<DbRow>>();
                        foreach (var entity in insertOrUpdate.Entities)
                        {
                            var (dbPartition, dbRow) = table.InsertOrReplace(entity, DateTime.UtcNow);
                            
                            if (!updateRows.ContainsKey(dbPartition.PartitionKey))
                                updateRows.Add(dbPartition.PartitionKey, new List<DbRow>());
                            
                            updateRows[dbPartition.PartitionKey].Add(dbRow);
                        }

                        foreach (var (partitionKey, rowsToUpdate) in updateRows)
                        {
                            _dataSynchronizer.SynchronizeUpdate(table, rowsToUpdate);
                            await _persistenceHandler.SynchronizePartitionAsync(table, partitionKey,  DataSynchronizationPeriod.Sec5);
                        }
                        break;
                    }
                }
            }

        }
        
        public async ValueTask<OperationResult> ReplaceAsync(DbTable table, IMyMemory myMemory, 
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            
            var entity = myMemory.ParseDynamicEntity();


            var (result, partition, dbRow) = table.Replace(entity, now);
            
            if (result != OperationResult.Ok)
                return result;
            
            _dataSynchronizer.SynchronizeUpdate(table, new[] {dbRow});

            await _persistenceHandler.SynchronizePartitionAsync(table, partition.PartitionKey, synchronizationPeriod);

            return OperationResult.Ok;
        }

        public async ValueTask<OperationResult> MergeAsync(DbTable table, IMyMemory myMemory,
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            var entity = myMemory.ParseDynamicEntity();

            var (result, partition, dbRow) = table.Merge(entity, now);
            
            if (result != OperationResult.Ok)
                return result;
            
            _dataSynchronizer.SynchronizeUpdate(table, new[] {dbRow});

            await _persistenceHandler.SynchronizePartitionAsync(table, partition.PartitionKey, synchronizationPeriod);
            
            return OperationResult.Ok;
        }

        public async ValueTask<OperationResult> DeleteAsync(DbTable table, string partitionKey, string rowKey, 
            DataSynchronizationPeriod synchronizationPeriod)
        {
            var (dbPartition, dbRow) = table.DeleteRow(partitionKey, rowKey);

            if (dbPartition == null) 
                return OperationResult.RowNotFound;
         
            _dataSynchronizer.SynchronizeDelete(table, new[]{dbRow});
            
            await _persistenceHandler.SynchronizePartitionAsync(table, dbPartition.PartitionKey, synchronizationPeriod);
            return OperationResult.Ok;
        }


        public async ValueTask<OperationResult> CleanAndKeepLastRecordsAsync(DbTable table, string partitionKey, int amount, 
            DataSynchronizationPeriod synchronizationPeriod)
        {
            var (dbPartition, dbRows) = table.CleanAndKeepLastRecords(partitionKey, amount);

            if (dbPartition != null)
            {
                _dataSynchronizer.SynchronizeDelete(table, dbRows);
                
                await _persistenceHandler.SynchronizePartitionAsync(table, dbPartition.PartitionKey, synchronizationPeriod);
            }
            
            return OperationResult.Ok;
        }
        
    }
}