using System;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Persistence;

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