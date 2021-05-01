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

namespace MyNoSqlServer.Domains.Db.Tables
{

    public class DbTable
    {
        public bool Persist { get; private set; }
        
        public int MaxPartitionsAmount { get; private set; }
        
        public void SetMaxPartitionsAmount(int maxPartitionsAmount)
        {
            MaxPartitionsAmount = maxPartitionsAmount;
        }

        public DbTable(string name, bool persistThisTable)
        {
            Name = name;
            Persist = persistThisTable;
        }

        public static DbTable CreateByRequest(string name, bool persistThisTable)
        {
            return new DbTable(name, persistThisTable);
        }

        public void UpdatePersist(bool persist)
        {
            Persist = persist;
        }

        public string Name { get; }

        private readonly ReaderWriterLockSlim _readerWriterLockSlim = new ();

        private readonly DbPartitionsList _partitions = new ();


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


        public IReadOnlyList<DbPartition> GetAllPartitions()
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



        public (OperationResult result, DbPartition partition, DbRow dbRow)
            Insert(DynamicEntity entity, DateTime now)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.GetOrCreate(entity.PartitionKey);

                var dbRow = DbRow.CreateNew(entity, now);

                if (partition.Insert(dbRow))
                    return (OperationResult.Ok, partition, dbRow);

                return (OperationResult.RecordExists, null, null);
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

        public (DbPartition partition, DbRow dbRow) InsertOrReplace(DynamicEntity entity, DateTime now)
        {

            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.GetOrCreate(entity.PartitionKey);

                var dbRow = DbRow.CreateNew(entity, now);
                partition.InsertOrReplace(dbRow);

                return (partition, dbRow);
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

        public IReadOnlyList<DbRow> DeleteRows(string partitionKey, IEnumerable<string> rowKeys)
        {
            if (partitionKey == null)
                throw new Exception("PartitionKey == null");
                        
            if (rowKeys == null)
                throw new Exception("RowKey == null");
            
            List<DbRow> result = null;
            
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return null;

                foreach (var rowKey in rowKeys)
                {
                    var dbRow = partition.DeleteRow(rowKey);
                    if (dbRow != null)
                    {
                        result ??= new List<DbRow>();
                        result.Add(dbRow);
                    }
                }

                return result;
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public (DbPartition dbPartition, DbRow dbRow) DeleteRow(string partitionKey, string rowKey)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return (null, null);

                var row = partition.DeleteRow(rowKey);

                if (row != null)
                    return (partition, row);
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

            return (null, null);
        }

        public (DbPartition dbPartition, IReadOnlyList<DbRow> dbRows) CleanAndKeepLastRecords(string partitionKey,
            int amount)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return (null, null);

                var dbRows = partition.CleanAndKeepLastRecords(amount);

                return (partition, dbRows);
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


        public (IEnumerable<DbPartition> partitions, IReadOnlyList<DbRow> rows) BulkInsertOrReplace(
            IEnumerable<IMyMemory> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan.ToDbRow())
                .ToList();


            var partitionsToSync = new Dictionary<string, DbPartition>();

            var rowsToSync = new List<DbRow>();

            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbRow in dbRows)
                {
                    var partition = _partitions.GetOrCreate(dbRow.PartitionKey);

                    partition.InsertOrReplace(dbRow);

                    if (!partitionsToSync.ContainsKey(partition.PartitionKey))
                        partitionsToSync.Add(partition.PartitionKey, partition);

                    rowsToSync.Add(dbRow);
                }

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();

            }


            return (partitionsToSync.Values, rowsToSync);
        }


        public void CleanAndBulkInsert(IEnumerable<IMyMemory> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .ToList();

            _readerWriterLockSlim.EnterWriteLock();

            try
            {
                _partitions.Clear();

                foreach (var dbRow in dbRows)
                {
                    var partition = _partitions.GetOrCreate(dbRow.PartitionKey);
                    partition.InsertOrReplace(dbRow);
                }

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public IEnumerable<DbPartition> CleanAndBulkInsert(string partitionKey, IEnumerable<IMyMemory> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .ToArray();

            _readerWriterLockSlim.EnterWriteLock();



            var result = new Dictionary<string, DbPartition>();

            try
            {
                var partitionToClean = _partitions.TryGet(partitionKey);
                partitionToClean?.Clean();

                foreach (var dbRow in dbRows)
                {
                    var partition = _partitions.GetOrCreate(dbRow.PartitionKey);

                    partition.InsertOrReplace(dbRow);

                    if (!result.ContainsKey(dbRow.PartitionKey))
                        result.Add(dbRow.PartitionKey, partition);
                }

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

            return result.Values;
        }

        public void Clear()
        {
            _readerWriterLockSlim.EnterWriteLock();

            try
            {
                _partitions.Clear();
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public IReadOnlyList<DbPartition> DeletePartitions(IEnumerable<string> partitions)
        {
            List<DbPartition> cleared = null;

            _readerWriterLockSlim.EnterWriteLock();

            try
            {
                foreach (var partitionKey in partitions)
                {
                    var dbPartition = _partitions.DeletePartition(partitionKey);
                    if (dbPartition != null)
                    {
                        cleared ??= new List<DbPartition>();
                        cleared.Add(dbPartition);
                    }

                }
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

            return (IReadOnlyList<DbPartition>)cleared ?? Array.Empty<DbPartition>();
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


        public IReadOnlyList<DbPartition> KeepMaxPartitions(in int amount)
        {
            var partitionsToGc = GetPartitionsToGc(amount);

            if (partitionsToGc.Count == 0)
                return partitionsToGc;

            var result = new List<DbPartition>();

            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbPartition in partitionsToGc)
                {
                    var partition = _partitions.DeletePartition(dbPartition.PartitionKey);
                    
                    if (partition != null)
                    {
                        result.Add(dbPartition);
                    }
                }

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

            return result;

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


        public (OperationResult result, DbPartition partition, DbRow dbRow) Replace(
            DynamicEntity entity, DateTime now)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(entity.PartitionKey);
                if (partition == null)
                    return (OperationResult.RecordNotFound, null, null);

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return (OperationResult.RecordNotFound, null, null);

                if (record.TimeStamp != entity.TimeStamp)
                    return (OperationResult.RecordChangedConcurrently, null, null);

                record.Replace(entity, now);

                return (OperationResult.Ok, partition, record);

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


        public (OperationResult result, DbPartition partition, DbRow dbRow) Merge(
            DynamicEntity entity, DateTime now)
        {
            var dbRow = TryGetDbRowWithReadLock(entity.PartitionKey, entity.RowKey);

            if (dbRow == null)
                return (OperationResult.RecordNotFound, null, null);

            if (dbRow.TimeStamp != entity.TimeStamp)
                return (OperationResult.RecordChangedConcurrently, null, null);

            var newEntities = dbRow.MergeEntities(entity);

            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(entity.PartitionKey);
                if (partition == null)
                    return (OperationResult.RecordNotFound, null, null);

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return (OperationResult.RecordNotFound, null, null);

                if (record.TimeStamp != entity.TimeStamp)
                    return (OperationResult.RecordChangedConcurrently, null, null);

                record.Replace(newEntities, now);

                return (OperationResult.Ok, partition, record);

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

        public bool BulkDelete(Dictionary<string, List<string>> partitionsAndRows)
        {
            var result = false;
            _readerWriterLockSlim.ExitWriteLock();
            try
            {

                foreach (var (partitionKey, rowKeys) in partitionsAndRows)
                {
                    if (rowKeys == null || rowKeys.Count == 0)
                    {
                        var deletedPartition = _partitions.DeletePartition(partitionKey);
                        if (deletedPartition != null)
                            result = true;
                    }
                    else
                    {
                        var partition = _partitions.TryGet(partitionKey);
                        
                        if (partition != null)
                            foreach (var rowKey in rowKeys)
                            {
                                var dbRow = partition.DeleteRow(rowKey);
                                if (dbRow != null)
                                    result = true;
                            }
                    }

                }

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

            return result;
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
    }
}