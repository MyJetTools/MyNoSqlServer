using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Rows;
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


        public OperationResult Insert(DbTable table, IMyMemory myMemory,
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

            _persistenceHandler.SynchronizePartition(table, dbPartition.PartitionKey, synchronizationPeriod);
            return OperationResult.Ok;
        }
        


        public OperationResult InsertOrReplace(DbTable table, IMyMemory myMemory, 
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            var entity = myMemory.ParseDynamicEntity();

            if (string.IsNullOrEmpty(entity.PartitionKey))
                return OperationResult.PartitionKeyIsNull;

            if (string.IsNullOrEmpty(entity.RowKey))
                return OperationResult.RowKeyIsNull;
            
            var (dbPartition, dbRow) = table.InsertOrReplace(entity, now);
            
            _dataSynchronizer.SynchronizeUpdate(table, new[]{dbRow});

            _persistenceHandler.SynchronizePartition(table, dbPartition.PartitionKey, synchronizationPeriod);

            return OperationResult.Ok;
        }


        public void ApplyTransactions(IReadOnlyDictionary<string, DbTable> tables, IEnumerable<IDbTransactionAction> transactions)
        {
            foreach (var transaction in transactions)
            {
                DbTable table;
                switch (transaction)
                {
                    case ICleanTableTransactionAction cleanTableTransaction:
                            table = tables[cleanTableTransaction.TableName];
                            table.Clear();
                            _persistenceHandler.SynchronizeTable(table, DataSynchronizationPeriod.Sec5); 
                            _dataSynchronizer.PublishInitTable(table);
                            break;
                    case IDeletePartitionsTransactionAction cleanPartitionsTransaction:
                    {
                        
                        table = tables[cleanPartitionsTransaction.TableName];
                        
                        var cleaned = table.DeletePartitions(cleanPartitionsTransaction.PartitionKeys);

                        foreach (var dbPartition in cleaned)
                        {
                            _persistenceHandler.SynchronizePartition(table, dbPartition.PartitionKey,  DataSynchronizationPeriod.Sec5);
                            _dataSynchronizer.PublishInitPartition(table, dbPartition);
                        }

                        break;
                    }
                    case IDeleteRowsTransactionAction deleteRows:
                        table = tables[deleteRows.TableName];
                        var dbRows = table.DeleteRows(deleteRows.PartitionKey, deleteRows.RowKeys);
                        if (dbRows != null)
                        {
                            _persistenceHandler.SynchronizePartition(table, deleteRows.PartitionKey,  DataSynchronizationPeriod.Sec5);
                            _dataSynchronizer.SynchronizeDelete(table, dbRows);
                        }
                        break;

                    case IInsertOrReplaceEntitiesTransactionAction insertOrUpdate:
                    {
                        table = tables[insertOrUpdate.TableName];
                        var updateRows = new Dictionary<string, List<DbRow>>();
                        foreach (var entity in insertOrUpdate.Entities)
                        {
                            var (dbPartition, dbRow) = table.InsertOrReplace(entity.Payload.ParseDynamicEntity(), DateTime.UtcNow);
                            
                            if (!updateRows.ContainsKey(dbPartition.PartitionKey))
                                updateRows.Add(dbPartition.PartitionKey, new List<DbRow>());
                            
                            updateRows[dbPartition.PartitionKey].Add(dbRow);
                        }

                        foreach (var (partitionKey, rowsToUpdate) in updateRows)
                        {
                            _dataSynchronizer.SynchronizeUpdate(table, rowsToUpdate);
                            _persistenceHandler.SynchronizePartition(table, partitionKey,  DataSynchronizationPeriod.Sec5);
                        }
                        break;
                    }
                }
            }

        }
        
        public OperationResult Replace(DbTable table, IMyMemory myMemory, 
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            
            var entity = myMemory.ParseDynamicEntity();


            var (result, partition, dbRow) = table.Replace(entity, now);
            
            if (result != OperationResult.Ok)
                return result;
            
            _dataSynchronizer.SynchronizeUpdate(table, new[] {dbRow});

            _persistenceHandler.SynchronizePartition(table, partition.PartitionKey, synchronizationPeriod);

            return OperationResult.Ok;
        }

        public OperationResult Merge(DbTable table, IMyMemory myMemory,
            DataSynchronizationPeriod synchronizationPeriod, DateTime now)
        {
            var entity = myMemory.ParseDynamicEntity();

            var (result, partition, dbRow) = table.Merge(entity, now);
            
            if (result != OperationResult.Ok)
                return result;
            
            _dataSynchronizer.SynchronizeUpdate(table, new[] {dbRow});

            _persistenceHandler.SynchronizePartition(table, partition.PartitionKey, synchronizationPeriod);
            
            return OperationResult.Ok;
        }

        public OperationResult DeleteRow(DbTable table, string partitionKey, string rowKey, 
            DataSynchronizationPeriod synchronizationPeriod)
        {
            var (dbPartition, dbRow) = table.DeleteRow(partitionKey, rowKey);

            if (dbPartition == null) 
                return OperationResult.RowNotFound;
         
            _dataSynchronizer.SynchronizeDelete(table, new[]{dbRow});
            
            _persistenceHandler.SynchronizePartition(table, dbPartition.PartitionKey, synchronizationPeriod);
            return OperationResult.Ok;
        }


        public OperationResult CleanAndKeepLastRecords(DbTable table, string partitionKey, int amount, 
            DataSynchronizationPeriod synchronizationPeriod)
        {
            var (dbPartition, dbRows) = table.CleanAndKeepLastRecords(partitionKey, amount);

            if (dbPartition != null)
            {
                _dataSynchronizer.SynchronizeDelete(table, dbRows);
                
                _persistenceHandler.SynchronizePartition(table, dbPartition.PartitionKey, synchronizationPeriod);
            }
            
            return OperationResult.Ok;
        }
        
    }
}