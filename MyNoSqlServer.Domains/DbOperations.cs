using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains
{
    public class DbOperations
    {
        private readonly DbInstance _dbInstance;

        public DbOperations(DbInstance dbInstance)
        {
            _dbInstance = dbInstance;
        }


        public void SetTableAttributes(string tableName, bool persist, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            var dbTable = _dbInstance.TryGetTable(tableName);
            
            if (dbTable == null)
                return;
            
            dbTable.SetAttributes(persist, maxPartitionsAmount, attributes);

        }


        public void ReplaceTable(string tableName, bool persist, IMyMemory content, TransactionEventAttributes attributes)
        {
              var dbTable = _dbInstance.TryGetTable(tableName) ?? _dbInstance.CreateTableIfNotExists(tableName, persist, attributes);
              dbTable.InitFromSnapshot(content);
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
            
            return table.Insert(entity, now, attributes);
            
        }
        


        public OperationResult InsertOrReplace(DbTable table, IMyMemory myMemory, 
            DateTime now, TransactionEventAttributes attributes)
        {
            var entity = myMemory.ParseDynamicEntity();

            if (string.IsNullOrEmpty(entity.PartitionKey))
                return OperationResult.PartitionKeyIsNull;

            if (string.IsNullOrEmpty(entity.RowKey))
                return OperationResult.RowKeyIsNull;
            
            table.InsertOrReplace(entity, now, attributes);

            return OperationResult.Ok;
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
                        foreach (var entity in insertOrUpdate.Entities)
                            table.InsertOrReplace(entity.Payload.ParseDynamicEntity(), DateTime.UtcNow, attributes);
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
    }
}