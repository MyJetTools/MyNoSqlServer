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

        private readonly ReaderWriterLockSlim _readerWriterLockSlim = new ReaderWriterLockSlim();
        
        private readonly SortedList<string, DbPartition> _partitions = new SortedList<string, DbPartition>();


        public IReadOnlyList<DbPartition> GetAllPartitions()
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.Values.ToList();
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
                return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey] : null;
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
                
                if (_partitions.ContainsKey(partitionSnapshot.PartitionKey))
                    return;
                
                _partitions.Add(partition.PartitionKey, partition);


                var partitionAsMyMemory = new MyMemoryAsByteArray(partitionSnapshot.Snapshot);
                
                foreach (var dbRowMemory in partitionAsMyMemory.SplitJsonArrayToObjects())
                {
                    var entity = partitionSnapshot.Snapshot.ParseDynamicEntity();
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
                if (!_partitions.ContainsKey(entity.PartitionKey))
                    _partitions.Add(entity.PartitionKey, DbPartition.Create(entity.PartitionKey));

                var partition = _partitions[entity.PartitionKey];
                
                var dbRow = DbRow.CreateNew(entity, now);
                
                if (partition.Insert(dbRow, now))
                    return (OperationResult.Ok, partition, dbRow);
                
                return (OperationResult.RecordExists, null, null);
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }


        }
        
        public (DbPartition partition, DbRow dbRow) InsertOrReplace(DynamicEntity entity, DateTime now)
        {
           
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                if (!_partitions.ContainsKey(entity.PartitionKey))
                    _partitions.Add(entity.PartitionKey, DbPartition.Create(entity.PartitionKey));

                var partition = _partitions[entity.PartitionKey];

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


        public IReadOnlyList<DbRow> GetAllRecords(int? limit)
        {
            var result = new List<DbRow>();
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (limit == null)
                {
                    foreach (var partition in _partitions.Values)
                        result.AddRange(partition.GetAllRows());
                }
                else
                {
                    foreach (var partition in _partitions.Values)
                    foreach (var dbRow in partition.GetAllRows())
                    {
                        result.Add(dbRow);
                        if (result.Count >= limit.Value)
                            return result;
                    }
                }
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }

            return result;
        }

        public IReadOnlyList<DbRow> GetRecords(string partitionKey, int? limit, int? skip)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (!_partitions.ContainsKey(partitionKey))
                    return Array.Empty<DbRow>();

                var partition = _partitions[partitionKey];

                if (skip == null && limit == null)
                    return partition.GetAllRows();
                

                return partition.GetRowsWithLimit(limit, skip);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public (DbPartition dbPartition, DbRow dbRow) DeleteRow(string partitionKey, string rowKey)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                if (!_partitions.ContainsKey(partitionKey))
                    return (null, null);

                var partition = _partitions[partitionKey];

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
        
        public (DbPartition dbPartition, IReadOnlyList<DbRow> dbRows) CleanAndKeepLastRecords(string partitionKey, int amount)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                if (!_partitions.ContainsKey(partitionKey))
                    return (null,null);

                var partition = _partitions[partitionKey];

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
                if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    return false;

                var partition = _partitions[entityInfo.PartitionKey];

                return partition.HasRecord(entityInfo.RowKey);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public (IEnumerable<DbPartition> partitions, IReadOnlyList<DbRow> rows) BulkInsertOrReplace(IEnumerable<IMyMemory> itemsAsArray)
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
                    if (!_partitions.ContainsKey(dbRow.PartitionKey))
                        _partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = _partitions[dbRow.PartitionKey];

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
                    if (!_partitions.ContainsKey(dbRow.PartitionKey))
                        _partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = _partitions[dbRow.PartitionKey];

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
                if (_partitions.ContainsKey(partitionKey))
                    _partitions[partitionKey].Clean();
                
                foreach (var dbRow in dbRows)
                {
                    if (!_partitions.ContainsKey(dbRow.PartitionKey))
                        _partitions.Add(dbRow.PartitionKey, DbPartition.Create(dbRow.PartitionKey));

                    var partition = _partitions[dbRow.PartitionKey];

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
        
        public void Clean()
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


        public IEnumerable<DbRow> ApplyQuery(IEnumerable<QueryCondition> queryConditions)
        {
            var conditionsDict = queryConditions.GroupBy(itm => itm.FieldName).ToDictionary(itm => itm.Key, itm => itm.ToList());

            var partitions = conditionsDict.ContainsKey(RowJsonUtils.PartitionKeyFieldName)
                ? _partitions.FilterByQueryConditions(conditionsDict[RowJsonUtils.PartitionKeyFieldName]).ToList()
                : _partitions.Values.ToList();

            if (conditionsDict.ContainsKey(RowJsonUtils.PartitionKeyFieldName))
                conditionsDict.Remove(RowJsonUtils.PartitionKeyFieldName);
                
            foreach (var partition in partitions)
                foreach (var dbRow in partition.ApplyQuery(conditionsDict))
                    yield return dbRow;
            
        }


        public int GetRecordsCount(string partitionKey)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return _partitions.Sum(itm => itm.Value.GetRecordsCount());

                return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey].GetRecordsCount() : 0;
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
                
                if (!_partitions.ContainsKey(partitionKey))
                    return Array.Empty<DbRow>();

                return _partitions[partitionKey].GetRows(rowKeys);
                
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
                
                if (!_partitions.ContainsKey(partitionKey))
                    return Array.Empty<DbRow>();

                return _partitions[partitionKey].GetHighestRowAndBelow(rowKey, maxAmount);
                
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        private IReadOnlyList<DbPartition> GetPartitionsToGarbageCollect(int maxAmount)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                if (_partitions.Count <= maxAmount)
                    return Array.Empty<DbPartition>();


                return _partitions
                    .Values
                    .OrderBy(itm => itm.LastAccessTime)
                    .Take(_partitions.Count - maxAmount)
                    .ToList();

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock(); 
            }
        }

        public IReadOnlyList<DbPartition> KeepMaxPartitions(in int amount)
        {
            var partitionsToGc = this.GetPartitionsToGarbageCollect(amount);

            if (partitionsToGc.Count == 0)
                return partitionsToGc;
            
            var result = new List<DbPartition>();
            
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbPartition in partitionsToGc)
                {
                    if (_partitions.ContainsKey(dbPartition.PartitionKey))
                    {
                        _partitions.Remove(dbPartition.PartitionKey);
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
                if (!_partitions.ContainsKey(partitionKey))
                    return null;
                var result = _partitions[partitionKey];


                if (result.GetRecordsCount() <= maxAmount)
                    return null;

                return result;
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
                if (!_partitions.ContainsKey(entity.PartitionKey))
                    return (OperationResult.RecordNotFound, null, null);
                
                var partition = _partitions[entity.PartitionKey];

                var record = partition.GetRow(entity.RowKey);

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
                return !_partitions.ContainsKey(partitionKey) 
                    ? null 
                    : _partitions[partitionKey].GetRow(rowKey);
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
                if (!_partitions.ContainsKey(entity.PartitionKey))
                    return (OperationResult.RecordNotFound, null, null);
                
                var partition = _partitions[entity.PartitionKey];

                var record = partition.GetRow(entity.RowKey);

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
                if (!_partitions.TryGetValue(partitionKey, out var dbPartition)) 
                    return null;
                
                var rowsAsBytes = dbPartition.GetAllRows().ToJsonArray().AsArray();
                return PartitionSnapshot.Create(partitionKey, rowsAsBytes);

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
    }
}