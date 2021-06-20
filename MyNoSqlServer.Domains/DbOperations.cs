using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains
{
    public class DbOperations
    {
        private readonly DbInstance _dbInstance;
        private readonly SyncEventsDispatcher _syncEventsDispatcher;

        public DbOperations(DbInstance dbInstance, SyncEventsDispatcher syncEventsDispatcher)
        {
            _dbInstance = dbInstance;
            _syncEventsDispatcher = syncEventsDispatcher;
        }


        public void SetTableAttributes(DbTable dbTable, bool persist, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            dbTable.SetAttributes(persist, maxPartitionsAmount, attributes);
        }
   
        public OperationResult Insert(DbTable table, IMyMemory myMemory,
            DateTime now, TransactionEventAttributes attributes)
        {
            var entity = myMemory.ParseDynamicEntity();

            if (string.IsNullOrEmpty(entity.PartitionKey))
                return OperationResult.PartitionKeyIsNull;

            if (string.IsNullOrEmpty(entity.RowKey))
                return OperationResult.RowKeyIsNull;

            if (table.HasRecord(entity))
                return OperationResult.RecordExists;
            
            var dbRow = DbRow.CreateNew(entity, now);
            

            return table.GetWriteAccess(writeAccess =>
            {
                var dbPartition = writeAccess.GetOrCreatePartition(entity.PartitionKey);

                var insert = dbPartition.Insert(dbRow);

                if (!insert) 
                    return OperationResult.RecordExists;
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, table, dbRow));
                return OperationResult.Ok;

            });
  
            
        }
        

        public OperationResult InsertOrReplace(DbTable table, IMyMemory myMemory, 
            DateTime now, TransactionEventAttributes attributes)
        {
            var entity = myMemory.ParseDynamicEntity();

            if (string.IsNullOrEmpty(entity.PartitionKey))
                return OperationResult.PartitionKeyIsNull;

            if (string.IsNullOrEmpty(entity.RowKey))
                return OperationResult.RowKeyIsNull;
            
            
            var dbRow = DbRow.CreateNew(entity, now);

            table.GetWriteAccess(writeAccess =>
            {
                var dbPartition = writeAccess.GetOrCreatePartition(entity.PartitionKey);
                dbPartition.InsertOrReplace(dbRow);
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, table, dbRow));
            });

            return OperationResult.Ok;
        }
        
        public OperationResult BulkInsertOrReplace(DbTable table, IEnumerable<IMyMemory> itemsAsArray, TransactionEventAttributes attributes)
        {
            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan.ToDbRow())
                .GroupBy(itm => itm.PartitionKey)
                .ToDictionary(itm => itm.Key, itm => itm.AsReadOnlyList());

            table.GetWriteAccess(writeAccess =>
            {
                
                foreach (var (partitionKey, rows) in dbRows)
                {
                    var dbPartition = writeAccess.GetOrCreatePartition(partitionKey);
                    dbPartition.BulkInsertOrReplace(rows);
                }
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRows(attributes, table, dbRows));
                
            });

            return OperationResult.Ok;

        }
        
        public void CleanPartitionAndBulkInsert(DbTable dbTable, string partitionKeyToClear, IEnumerable<IMyMemory> itemsAsArray, TransactionEventAttributes attributes)
        {

            var dbRowsByPartition = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .GroupBy(itm => itm.PartitionKey)
                .ToDictionary(itm => itm.Key, itm => itm.ToList());


            dbTable.GetWriteAccess(writeAccess =>
            {
                var syncPartitions = new Dictionary<string, IReadOnlyList<DbRow>>();
                
                foreach (var (partitionKey, dbRows) in dbRowsByPartition)
                {
                    var dbPartition = writeAccess.GetOrCreatePartition(partitionKey);

                    if (partitionKey == partitionKeyToClear)
                    {
                        dbPartition.ClearAndBulkInsertOrReplace(dbRows);
                        _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.AsInitPartition(attributes, dbTable, dbPartition));
                    }
                        
                    else
                        dbPartition.BulkInsertOrReplace(dbRows);
                    
                    syncPartitions.Add(partitionKey, dbPartition.GetAllRows());
                }
                
                
                _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.Create(attributes, dbTable, syncPartitions));

            });

        }
        

        public void ApplyTransactions(IReadOnlyDictionary<string, DbTable> tables, IEnumerable<IDbTransactionAction> transactions, 
            TransactionEventAttributes attributes)
        {
            foreach (var transaction in transactions)
            {
                DbTable table;
                switch (transaction)
                {
                    case ICleanTableTransactionAction cleanTableTransaction:
                            table = tables[cleanTableTransaction.TableName];
                            table.Clear(attributes);
                            break;
                    case IDeletePartitionsTransactionAction cleanPartitionsTransaction:
                    {
                        table = tables[cleanPartitionsTransaction.TableName];
                        table.DeletePartitions(cleanPartitionsTransaction.PartitionKeys, attributes);
                        break;
                    }
                    case IDeleteRowsTransactionAction deleteRows:
                        table = tables[deleteRows.TableName];
                        table.DeleteRows(deleteRows.PartitionKey, deleteRows.RowKeys, attributes);
                        break;

                    case IInsertOrReplaceEntitiesTransactionAction insertOrUpdate:
                    {
                        table = tables[insertOrUpdate.TableName];


                        var entities = insertOrUpdate
                            .Entities
                            .Select(entity => DbRow.CreateNew(entity.Payload.ParseDynamicEntity(), DateTime.UtcNow))
                            .GroupBy(itm => itm.PartitionKey)
                            .ToDictionary(itm => itm.Key, itm => itm.AsReadOnlyList());

                        table.GetWriteAccess(writeAccess =>
                        {
                            foreach (var (partitionKey, rows) in entities)
                            {
                                var dbPartition = writeAccess.GetOrCreatePartition(partitionKey);
                                dbPartition.BulkInsertOrReplace(rows);
                            }
                            
                            _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRows(attributes, table, entities));
                        });
                        break;
                    }
                }
            }

        }
        
        public OperationResult Replace(DbTable table, IMyMemory myMemory, 
            DateTime now, TransactionEventAttributes attributes)
        {
            
            var entity = myMemory.ParseDynamicEntity();

            var result = table.Replace(entity, now, attributes);
            
            if (result != OperationResult.Ok)
                return result;
            

            return OperationResult.Ok;
        }

        public OperationResult Merge(DbTable table, IMyMemory myMemory,
            DateTime now, TransactionEventAttributes attributes)
        {
            var entity = myMemory.ParseDynamicEntity();
            return table.Merge(entity, now, attributes);
        }

        public OperationResult DeleteRow(DbTable table, string partitionKey, string rowKey, 
            TransactionEventAttributes attributes)
        {
            return table.DeleteRow(partitionKey, rowKey, attributes);
        }


        public OperationResult CleanAndKeepLastRecords(DbTable table, string partitionKey, int amount, 
            TransactionEventAttributes attributes)
        {
             table.CleanAndKeepLastRecords(partitionKey, amount, attributes);
            return OperationResult.Ok;
        }

        public void DeletePartitions(string tableName, string[] partitionKeys, TransactionEventAttributes attributes)
        {
            var table = _dbInstance.TryGetTable(tableName);
            
            if (table == null)
                return;

            table.DeletePartitions(partitionKeys, attributes);
        }

        public void CleanTableAndBulkInsert(DbTable dbTable, IEnumerable<IMyMemory> itemsAsArray,
            TransactionEventAttributes attributes)
        {
            var dbRowsByPartitions = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .GroupBy(itm => itm.PartitionKey)
                .ToDictionary(itm => itm.Key, itm => itm.AsReadOnlyList());
            
            CleanTableAndBulkInsert(dbTable, dbRowsByPartitions, attributes);

        }
        
        public void CleanTableAndBulkInsert(DbTable dbTable, Dictionary<string, IReadOnlyList<DbRow>> snapshot,
            TransactionEventAttributes attributes)
        {

            dbTable.GetWriteAccess(writeAccess =>
            {
                writeAccess.InitTable(snapshot, attributes);
                
                _syncEventsDispatcher.Dispatch(InitTableTransactionEvent.Create(attributes, dbTable, snapshot));
            });

        }



    }
    
    
}