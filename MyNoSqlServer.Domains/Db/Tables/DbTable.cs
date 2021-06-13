using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.Query;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Db.Tables
{


    public interface IDbTableReadAccess
    {

        IReadOnlyList<DbRow> GetAllRows();

        IEnumerable<DbPartition> GetAllPartitions();
    }

    public class DbTable : IDbTableReadAccess
    {
        public bool Persist { get; private set; }
        
        public int MaxPartitionsAmount { get; private set; }

        private readonly SyncEventsDispatcher _syncEventsDispatcher;
        
        
        public void SetAttributes(bool persist, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            
            if (persist == Persist && MaxPartitionsAmount == maxPartitionsAmount)
                return;
            
            Persist = persist;
            MaxPartitionsAmount = maxPartitionsAmount;
            _syncEventsDispatcher.Dispatch(SyncTableAttributes.Create(attributes, this));
        }

        
        public void SetMaxPartitionsAmount(int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            MaxPartitionsAmount = maxPartitionsAmount;
            _syncEventsDispatcher.Dispatch(SyncTableAttributes.Create(attributes, this));
        }

        public DbTable(string name, bool persistThisTable, SyncEventsDispatcher syncEventsDispatcher)
        {
            Name = name;
            Persist = persistThisTable;
            _syncEventsDispatcher = syncEventsDispatcher;
        }

        public static DbTable CreateByRequest(string name, bool persistThisTable, SyncEventsDispatcher syncEventsDispatcher)
        {
            return new DbTable(name, persistThisTable, syncEventsDispatcher);
        }

        public static DbTable CreateFromSnapshot(string name, bool persistThisTable, IMyMemory content, SyncEventsDispatcher syncEventsDispatcher)
        {
            var dbTable = new DbTable(name, persistThisTable, syncEventsDispatcher);
            
            
            dbTable.InitFromSnapshot(content);

            return dbTable;

        }
        
        public void UpdatePersist(bool persist, TransactionEventAttributes attributes)
        {
            Persist = persist;
            _syncEventsDispatcher.Dispatch(SyncTableAttributes.Create(attributes, this));
        }

        public string Name { get; }

        private readonly ReaderWriterLockSlim _readerWriterLockSlim = new ();

        private DbPartitionsList _partitions = new ();


        public IReadOnlyList<string> GetAllPartitionKeys()
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.GetAllPartitionKeys();
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        IEnumerable<DbPartition> IDbTableReadAccess.GetAllPartitions()
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.GetAllPartitions().ToList();
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public DbPartition GetPartition(string partitionKey)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.TryGet(partitionKey);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public void InitFromSnapshot(IMyMemory snapshot)
        {

            var dbPartitions = new DbPartitionsList();
            var partitions = snapshot.SplitJsonArrayToObjects();

            foreach (var partitionMemory in partitions)
            {
                var dbRows = partitionMemory.SplitJsonArrayToObjects();

                foreach (var dbRowAsMemory in dbRows)
                {
                    var entity = dbRowAsMemory.ParseDynamicEntity();
                    var dbRow = DbRow.RestoreSnapshot(entity, dbRowAsMemory);

                    var dbPartition = dbPartitions.GetOrCreate(dbRow.PartitionKey);
                    dbPartition.InsertOrReplace(dbRow);
                }

            }

            _partitions = dbPartitions;

        }


        public void InitPartitionFromSnapshot(PartitionSnapshot partitionSnapshot)
        {

            _readerWriterLockSlim.EnterWriteLock();
            try
            {

                var partition = DbPartition.Create(partitionSnapshot.PartitionKey);

                if (_partitions.HasPartition(partitionSnapshot.PartitionKey))
                    return;

                _partitions.InitPartition(partition);


                var partitionAsMyMemory = new MyMemoryAsByteArray(partitionSnapshot.Snapshot);


                foreach (var dbRowMemory in partitionAsMyMemory.SplitJsonArrayToObjects())
                {
                    var entity = dbRowMemory.ParseDynamicEntity();
                    var dbRow = DbRow.RestoreSnapshot(entity, dbRowMemory);
                    partition.InsertOrReplace(dbRow);
                }
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }



        public OperationResult Insert(DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.GetOrCreate(entity.PartitionKey);

                var dbRow = DbRow.CreateNew(entity, now);

                if (partition.Insert(dbRow))
                {
                    _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, this, dbRow));
                    return OperationResult.Ok;
                }

                return OperationResult.RecordExists;
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }


        }

        public int GetPartitionsCount()
        {
            return _partitions.Count;
        }

        public void InsertOrReplace(DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.GetOrCreate(entity.PartitionKey);

                var dbRow = DbRow.CreateNew(entity, now);
                partition.InsertOrReplace(dbRow);
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, this, dbRow));
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }

        public DbRow GetEntity(string partitionKey, string rowKey)
        {
            return TryGetDbRowWithReadLock(partitionKey, rowKey);
        }


        public IReadOnlyList<DbRow> GetAllRecords(int? limit, int? skip)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                var records 
                    = _partitions
                        .GetAllPartitions()
                        .SelectMany(partition => partition.GetAllRows());

                if (skip != null)
                    records = records.Skip(skip.Value);

                if (limit != null)
                    records = records.Take(limit.Value);

                return records.ToList();

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
        public void GetReadAccess(Action<IDbTableReadAccess> readAccess)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                readAccess(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
        public T GetReadAccess<T>(Func<IDbTableReadAccess, T> readAccess)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return readAccess(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public IReadOnlyList<DbRow> GetRecords(string partitionKey, int? limit, int? skip)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                var partition = _partitions.TryGet(partitionKey);
                
                if (partition == null)
                    return Array.Empty<DbRow>();

                if (skip == null && limit == null)
                    return partition.GetAllRows();

                return partition.GetRowsWithLimit(limit, skip);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
        public IReadOnlyList<DbRow> GetRecordsByRowKey(string rowKey, int? limit, int? skip)
        {
            List<DbRow> result = null;
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                var recordsByRowKey = _partitions.GetAllPartitions().Select(dbPartition => dbPartition.TryGetRow(rowKey))
                    .Where(dbRow => dbRow != null);

                if (skip != null)
                    recordsByRowKey = recordsByRowKey.Skip(skip.Value);

                foreach (var dbRow in recordsByRowKey)
                {
                    result ??= new List<DbRow>();
                    result.Add(dbRow);
                    
                    if (limit != null)
                    {
                        if (result.Count >= limit)
                            return result;
                    }
                }
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }

            return (IReadOnlyList<DbRow>)result ?? Array.Empty<DbRow>();
        }

        public void DeleteRows(string partitionKey, IEnumerable<string> rowKeys, TransactionEventAttributes attributes)
        {
            if (partitionKey == null)
                throw new Exception("PartitionKey == null");
                        
            if (rowKeys == null)
                throw new Exception("RowKey == null");
            
            List<DbRow> deletedRows = null;
            
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return;

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
                    _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRows(attributes, this, deletedRows));
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public OperationResult DeleteRow(string partitionKey, string rowKey, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return OperationResult.RowNotFound;

                var row = partition.DeleteRow(rowKey);

                if (row == null)
                    return OperationResult.RowNotFound;
                _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRow(attributes, this, row));
                return OperationResult.Ok;

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public void CleanAndKeepLastRecords(string partitionKey,
            int amount, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return;

                var dbRows = partition.CleanAndKeepLastRecords(amount);
                
                _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRows(attributes, this, dbRows));

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public bool HasRecord(IMyNoSqlDbEntity entityInfo)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                var partition = _partitions.TryGet(entityInfo.PartitionKey);
                if (partition == null)
                    return false;

                return partition.HasRecord(entityInfo.RowKey);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public void BulkInsertOrReplace(
            IEnumerable<IMyMemory> itemsAsArray, TransactionEventAttributes attributes)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan.ToDbRow())
                .ToList();

            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbRow in dbRows)
                {
                    var partition = _partitions.GetOrCreate(dbRow.PartitionKey);

                    partition.InsertOrReplace(dbRow);
                }
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRows(attributes, this, dbRows));
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }


        public void CleanAndBulkInsert(IEnumerable<IMyMemory> itemsAsArray, TransactionEventAttributes attributes)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .ToList();

            _readerWriterLockSlim.EnterWriteLock();

            try
            {
                _partitions.Clear();

                var partitionsToSync = new Dictionary<string, List<DbRow>>();
                foreach (var dbRow in dbRows)
                {
                    var partition = _partitions.GetOrCreate(dbRow.PartitionKey);
                    partition.InsertOrReplace(dbRow);
                    
                    if (!partitionsToSync.ContainsKey(partition.PartitionKey))
                        partitionsToSync.Add(partition.PartitionKey, new List<DbRow>());
                    
                    partitionsToSync[partition.PartitionKey].Add(dbRow);
                }

                _syncEventsDispatcher.Dispatch(InitTableTransactionEvent.Create(attributes, this, partitionsToSync) );


            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public void CleanAndBulkInsert(string partitionKey, IEnumerable<IMyMemory> itemsAsArray, TransactionEventAttributes attributes)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .ToArray();

            _readerWriterLockSlim.EnterWriteLock();


            try
            {
                var syncData = new Dictionary<string, IReadOnlyList<DbRow>>();
                
                var partitionToClean = _partitions.TryGet(partitionKey);
                if (partitionToClean != null)
                {
                    partitionToClean.Clean();    
                    syncData.Add(partitionToClean.PartitionKey, Array.Empty<DbRow>());
                }
                

              
                foreach (var dbRow in dbRows)
                {
                    var partition = _partitions.GetOrCreate(dbRow.PartitionKey);

                    partition.InsertOrReplace(dbRow);
                    
                    if (!syncData.ContainsKey(dbRow.PartitionKey))
                        syncData.Add(dbRow.PartitionKey, null);
                }

                foreach (var pk in syncData.Keys.ToList())
                {
                    if (syncData[pk] == null)
                    {
                        var partition = _partitions.TryGet(pk);

                        if (partition == null)
                            syncData.Remove(pk);
                        else
                            syncData[pk] = partition.GetAllRows();
                    }
                }
                
                _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.Create(attributes, this, syncData));
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }

        public void Clear(TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();

            try
            {
                _partitions.Clear();
                _syncEventsDispatcher.Dispatch( InitTableTransactionEvent.AsDelete(attributes, this));
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public void DeletePartitions(IEnumerable<string> partitions, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();

            try
            {
                foreach (var partitionKey in partitions)
                {
                    var dbPartition = _partitions.DeletePartition(partitionKey);
                    if (dbPartition != null)
                    {
                        _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.AsDeletePartition(attributes, this, dbPartition));
                    }
                }
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }


        public IEnumerable<DbRow> ApplyQuery(IEnumerable<QueryCondition> queryConditions)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.ApplyQuery(queryConditions);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public int GetRecordsCount(string partitionKey)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return _partitions.GetAllPartitions().Sum(itm => itm.GetRecordsCount());

                var partition = _partitions.TryGet(partitionKey);

                return partition?.GetRecordsCount() ?? 0;
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public IReadOnlyList<DbRow> GetMultipleRows(string partitionKey, string[] rowKeys)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return Array.Empty<DbRow>();
                
                var partition = _partitions.TryGet(partitionKey);

                if (partition == null)
                    return Array.Empty<DbRow>();

                return partition.GetRows(rowKeys);

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public IEnumerable<DbRow> GetHighestRowAndBelow(string partitionKey, string rowKey, int maxAmount)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return Array.Empty<DbRow>();

                var partition = _partitions.TryGet(partitionKey);

                if (partition == null)
                    return Array.Empty<DbRow>();

                return partition.GetHighestRowAndBelow(rowKey, maxAmount);

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public void KeepMaxPartitions(in int amount, TransactionEventAttributes attributes)
        {
            var partitionsToGc = GetPartitionsToGc(amount);

            if (partitionsToGc.Count == 0)
                return;


            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                List<DbPartition> deleted = null;
                foreach (var dbPartition in partitionsToGc)
                {
                    var partition = _partitions.DeletePartition(dbPartition.PartitionKey);

                    if (partition != null)
                    {
                        deleted ??= new List<DbPartition>();
                        deleted.Add(partition);
                    }
                }
                
                if (deleted != null)
                    _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.AsDeletePartitions(attributes, this, deleted));

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }


        }


        private DbPartition GetPartitionIfItHasToBeCleaned(string partitionKey, int maxAmount)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return null;

                return partition.GetRecordsCount() <= maxAmount ? null : partition;
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public OperationResult Replace(
            DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(entity.PartitionKey);
                if (partition == null)
                    return OperationResult.RecordNotFound;

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return OperationResult.RecordNotFound;

                if (record.TimeStamp != entity.TimeStamp)
                    return OperationResult.RecordChangedConcurrently;

                record.Replace(entity, now);
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, this, record));

                return OperationResult.Ok;

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }




        private DbRow TryGetDbRowWithReadLock(string partitionKey, string rowKey)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.TryGet(partitionKey)?.TryGetRow(rowKey);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }

        }


        public OperationResult Merge(
            DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {
            var dbRow = TryGetDbRowWithReadLock(entity.PartitionKey, entity.RowKey);

            if (dbRow == null)
                return OperationResult.RecordNotFound;

            if (dbRow.TimeStamp != entity.TimeStamp)
                return OperationResult.RecordChangedConcurrently;

            var newEntities = dbRow.MergeEntities(entity);

            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(entity.PartitionKey);
                if (partition == null)
                    return OperationResult.RecordNotFound;

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return OperationResult.RecordNotFound;

                if (record.TimeStamp != entity.TimeStamp)
                    return OperationResult.RecordChangedConcurrently;

                record.Replace(newEntities, now);
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, this, record));

                return OperationResult.Ok;

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public DbPartition KeepMaxRecordsAmount(string partitionKey, int maxAmount)
        {

            var dbPartition = GetPartitionIfItHasToBeCleaned(partitionKey, maxAmount);

            if (dbPartition == null)
                return null;


            _readerWriterLockSlim.EnterWriteLock();
            try
            {

                if (dbPartition.GetRecordsCount() <= maxAmount)
                    return null;


                dbPartition.CleanAndKeepLastRecords(maxAmount);
                return dbPartition;
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }

        public PartitionSnapshot GetPartitionSnapshot(string partitionKey)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                var dbPartition = _partitions.TryGet(partitionKey);
                if (dbPartition == null)
                    return null;

                var rowsAsBytes = dbPartition.GetAllRows().ToJsonArray().AsArray();
                return PartitionSnapshot.Create(partitionKey, rowsAsBytes);

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public void BulkDelete(Dictionary<string, List<string>> partitionsAndRows, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.ExitWriteLock();
            try
            {
                

                foreach (var (partitionKey, rowKeys) in partitionsAndRows)
                {
                    if (rowKeys == null || rowKeys.Count == 0)
                    {
                        var deletedPartition = _partitions.DeletePartition(partitionKey);
                        if (deletedPartition != null)
                            _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.AsDeletePartition(attributes, this, deletedPartition));
                    }
                    else
                    {
                        var partition = _partitions.TryGet(partitionKey);

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
                                _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRows(attributes, this, deletedRows));
                        }
                    }

                }
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }

        private IReadOnlyList<DbPartition> GetPartitionsToGc(int maxAmount)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.GetPartitionsToGc(maxAmount);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public void Gc()
        {

            var partitionsToGc = GetPartitionsToGc(MaxPartitionsAmount);
            
            if (partitionsToGc.Count == 0)
                return;
            
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbPartition in partitionsToGc)
                    _partitions.DeletePartition(dbPartition.PartitionKey);
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }

        IReadOnlyList<DbRow> IDbTableReadAccess.GetAllRows()
        {
            return _partitions
                    .GetAllPartitions()
                    .SelectMany(partition => partition.GetAllRows()).ToList();
        }
    }
}