using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Partitions;
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

        public bool CreateTable(string tableName, bool persistTable, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            var (result, dbTable) = _dbInstance.GetWriteAccess(writeAccess =>
            {
                var foundTable = writeAccess.TryGetTable(tableName);

                if (foundTable != null)
                    return (false, null);

                var newTable = writeAccess.CreateTable(tableName, persistTable, maxPartitionsAmount);
                return (true, newTable);
            });

            if (result)
                _syncEventsDispatcher.Dispatch(UpdateTableAttributesTransactionEvent.Create(attributes, dbTable));

            return result;
        }
        
        public DbTable CreateTableIfNotExists(string tableName, bool persistTable, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            var (created, dbTable) = _dbInstance.GetWriteAccess(writeAccess =>
            {
                var foundTable = writeAccess.TryGetTable(tableName);

                if (foundTable != null)
                    return (false, null);

                var newTable = writeAccess.CreateTable(tableName, persistTable, maxPartitionsAmount);
                return (true, newTable);
            });

            var set = dbTable.SetAttributes(persistTable, maxPartitionsAmount);

            if (created || set)
                _syncEventsDispatcher.Dispatch(UpdateTableAttributesTransactionEvent.Create(attributes, dbTable));

            return dbTable;
        }

        public DbTable GetOrCreateTable(string tableName, bool persist, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            var result = _dbInstance.TryGetTable(tableName);
            
            if (result != null)
                return result;

            var tableCreated = false;
            
            result = _dbInstance.GetWriteAccess(writeAccess =>
            {
                var dbTable = writeAccess.TryGetTable(tableName);

                tableCreated = true;

                return dbTable ?? writeAccess.CreateTable(tableName, persist, maxPartitionsAmount);
            });

            if (tableCreated)
                _syncEventsDispatcher.Dispatch(UpdateTableAttributesTransactionEvent.Create(attributes, result));

            return result;

        }


        public void SetTableAttributes(DbTable dbTable, bool persist, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            var set = dbTable.SetAttributes(persist, maxPartitionsAmount);

            if (set)
                _syncEventsDispatcher.Dispatch(UpdateTableAttributesTransactionEvent.Create(attributes, dbTable));
        }
        
        public OperationResult DeleteRow(DbTable dbTable, string partitionKey, string rowKey, TransactionEventAttributes attributes)
        {
            return dbTable.GetWriteAccess(writeAccess =>
            {
                var partition = writeAccess.TryGetPartitionWriteAccess(partitionKey);
                if (partition == null)
                    return OperationResult.RowNotFound;

                var row = partition.DeleteRow(rowKey);

                if (row == null)
                    return OperationResult.RowNotFound;
                
    
                _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRow(attributes, dbTable, row));
                
                return OperationResult.Ok;
            });
        }


        public void DeleteRows(DbTable dbTable, string partitionKey,
            IEnumerable<string> rowKeys, TransactionEventAttributes attributes)
        {
            var deletedRows = dbTable.GetWriteAccess(writeAccess => writeAccess.DeleteRows(partitionKey, rowKeys));

            if (deletedRows != null)
                _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRows(attributes, dbTable, deletedRows));   
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
                            Clear(table, attributes);
                            break;
                    case IDeletePartitionsTransactionAction cleanPartitionsTransaction:
                    {
                        table = tables[cleanPartitionsTransaction.TableName];
                        DeletePartitions(table, cleanPartitionsTransaction.PartitionKeys, attributes);
                        break;
                    }
                    case IDeleteRowsTransactionAction deleteRows:
                        table = tables[deleteRows.TableName];
                        DeleteRows(table, deleteRows.PartitionKey, deleteRows.RowKeys, attributes);
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
        
        public OperationResult Replace(DbTable dbTable, IMyMemory myMemory, 
            DateTime now, TransactionEventAttributes attributes)
        {
            
            var entity = myMemory.ParseDynamicEntity();

            return dbTable.GetWriteAccess(writeAccess =>
            {
                var partition = writeAccess.TryGetPartitionWriteAccess(entity.PartitionKey);
                if (partition == null)
                    return OperationResult.RecordNotFound;

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return OperationResult.RecordNotFound;

                if (record.TimeStamp != entity.TimeStamp)
                    return OperationResult.RecordChangedConcurrently;

                record.Replace(entity, now);

                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, dbTable, record));

                return OperationResult.Ok;
            });

        }

        /*
        public OperationResult Merge(DbTable table, IMyMemory myMemory,
            DateTime now, TransactionEventAttributes attributes)
        {
            var entity = myMemory.ParseDynamicEntity();
            return table.Merge(entity, now, attributes);
        }
*/
        
        public void CleanAndKeepLastRecords(DbTable dbTable, string partitionKey,
            int amount, TransactionEventAttributes attributes)
        {

            dbTable.GetWriteAccess(writeAccess =>
            {
                var partition = writeAccess.TryGetPartitionWriteAccess(partitionKey);
                if (partition == null)
                    return;

                var dbRows = partition.CleanAndKeepLastRecords(amount);

                if (dbRows != null)
                    _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRows(attributes, dbTable, dbRows));
            });
            
        }


        public void DeletePartitions(DbTable table, string[] partitionKeys, TransactionEventAttributes attributes)
        {
            table.GetWriteAccess(writeAccess =>
            {
                var partitions = writeAccess.DeletePartitions(partitionKeys);

                if (partitions != null)
                {

                    _syncEventsDispatcher.Dispatch(
                        InitPartitionsTransactionEvent.AsDeletePartitions(attributes, table, partitions));
                }
            });


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
                writeAccess.InitTable(snapshot);
                
                    _syncEventsDispatcher.Dispatch(InitTableTransactionEvent.Create(attributes, dbTable, snapshot));
            });

        }
        
        public void Clear(DbTable dbTable, TransactionEventAttributes attributes)
        {
            dbTable.GetWriteAccess(writeAccess =>
            {
                if (writeAccess.Clear())
                    _syncEventsDispatcher.Dispatch( InitTableTransactionEvent.AsDelete(attributes, dbTable));
            });
        }
        
        public void KeepMaxPartitionsAmount(DbTable dbTable, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {

            var partitionsAmount = dbTable.GetReadAccess(readAccess => readAccess.GetPartitionsAmount());

            if (partitionsAmount <= maxPartitionsAmount)
                return;

            dbTable.GetWriteAccess(writeAccess =>
            {
                var partitionsToGc = writeAccess.GetPartitionsToGc(maxPartitionsAmount);
                
                List<DbPartition> deleted = null;
                foreach (var dbPartition in partitionsToGc)
                {
                    var deletedPartition = writeAccess.DeletePartition(dbPartition.PartitionKey);

                    if (deletedPartition != null)
                    {
                        deleted ??= new List<DbPartition>();
                        deleted.Add(deletedPartition);
                    }
                }
                
                if (deleted != null)
                    _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.AsDeletePartitions(attributes, dbTable, deleted));

            });

        }
        
        public OperationResult Replace(DbTable dbTable,
            DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {

            return dbTable.GetWriteAccess(writeAccess =>
            {
                var partition = writeAccess.TryGetPartitionWriteAccess(entity.PartitionKey);
                if (partition == null)
                    return OperationResult.RecordNotFound;

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return OperationResult.RecordNotFound;

                if (record.TimeStamp != entity.TimeStamp)
                    return OperationResult.RecordChangedConcurrently;

                record.Replace(entity, now);
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, dbTable, record));

                return OperationResult.Ok;
            });

        }
        
        public OperationResult Merge(DbTable dbTable,
            DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {
            var dbRow = dbTable.GetReadAccess(readAccess => readAccess.TryGetRow(entity.PartitionKey, entity.RowKey));

            if (dbRow == null)
                return OperationResult.RecordNotFound;

            if (dbRow.TimeStamp != entity.TimeStamp)
                return OperationResult.RecordChangedConcurrently;

            var newEntities = dbRow.MergeEntities(entity);


            return dbTable.GetWriteAccess(writeAccess =>
            {
                var partition = writeAccess.TryGetPartitionWriteAccess(entity.PartitionKey);
                if (partition == null)
                    return OperationResult.RecordNotFound;

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return OperationResult.RecordNotFound;

                if (record.TimeStamp != entity.TimeStamp)
                    return OperationResult.RecordChangedConcurrently;

                record.Replace(newEntities, now);
                

                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, dbTable, record));

                return OperationResult.Ok;
            });

        }
        
        
        public void BulkDelete(DbTable dbTable, Dictionary<string, List<string>> partitionsAndRows, 
            TransactionEventAttributes attributes)
        {

            dbTable.GetWriteAccess(writeAccess =>
            {
                foreach (var (partitionKey, rowKeys) in partitionsAndRows)
                {
                    if (rowKeys == null || rowKeys.Count == 0)
                    {
                        var deletedPartition = writeAccess.DeletePartition(partitionKey);
                        if (deletedPartition != null)
                            _syncEventsDispatcher.Dispatch(
                                InitPartitionsTransactionEvent.AsDeletePartition(attributes, dbTable, deletedPartition));
                    }
                    else
                    {
                        var partition = writeAccess.TryGetPartitionWriteAccess(partitionKey);

                        if (partition != null)
                        {
                            List<DbRow> deletedRows = null;
                            foreach (var rowKey in rowKeys)
                            {
                                var dbRow = partition.DeleteRow(rowKey);
                                if (dbRow != null)
                                {
                                    deletedRows ??= new List<DbRow>();
                                    deletedRows.Add(dbRow);
                                }
                            }

                            if (deletedRows != null)
                                _syncEventsDispatcher.Dispatch(
                                    DeleteRowsTransactionEvent.AsRows(attributes, dbTable, deletedRows));
                        }
                    }

                }

            });

        }
        
        //ToDo - UnitTest It
        public IReadOnlyList<DbRow> GetRecordsByRowKey(DbTable dbTable, string rowKey, int? limit, int? skip)
        {

            return dbTable.GetReadAccess(readAccess =>
            {
                List<DbRow> resultRecords = null;

                var skippedRemains = skip ?? 0;


                foreach (var partition in readAccess.GetAllPartitions())
                {

                    var dbRow = partition.TryGetRow(rowKey);

                    if (skippedRemains > 0)
                    {
                        skippedRemains--;
                        continue;
                    }

                    if (dbRow != null)
                    {
                        resultRecords ??= new List<DbRow>();
                        resultRecords.Add(dbRow);
                        
                        if (limit != null)
                        {
                            if (resultRecords.Count >= limit)
                                return resultRecords;
                        }
                    }
                }

                return (IReadOnlyList<DbRow>)resultRecords ?? Array.Empty<DbRow>();

            });
            
        }
        
        public void Gc()
        {
            foreach (var table in _dbInstance.GetTables())
            {
                // ToDo - Attributes = 0 - change it
                if (table.MaxPartitionsAmount >0)
                    KeepMaxPartitionsAmount(table, table.MaxPartitionsAmount, null);
            }
        }

    }
    
    
}