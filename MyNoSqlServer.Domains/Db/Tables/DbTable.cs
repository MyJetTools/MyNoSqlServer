using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Tables
{

    public class DbTable
    {
        private DbTable(string name)
        {
            Name = name;
        }

        public static DbTable CreateByRequest(string name)
        {
            return new DbTable(name);
        }
        
        public static DbTable CreateByInit(string name)
        {
            return new DbTable(name);
        }
        
        public string Name { get; }
        
        internal readonly ReaderWriterLockSlim ReaderWriterLockSlim = new ReaderWriterLockSlim();
        
        private readonly SortedList<string, DbPartition> _partitions = new SortedList<string, DbPartition>();


        public IReadOnlyList<DbPartition> GetAllPartitions()
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.Values.ToList();
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


        public DbPartition GetPartition(string partitionKey)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey] : null;
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


        public DbPartition InitPartitionFromSnapshot(IMyMemory data)
        {
            DbPartition partition = null;
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                foreach (var dbRowMemory in data.SplitJsonArrayToObjects())
                {

                    var jsonFields = dbRowMemory.ParseFirstLevelOfJson();

                    var entityInfo = jsonFields.GetEntityInfo();

                    if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    {
                        partition = DbPartition.Create(entityInfo.PartitionKey);
                        _partitions.Add(entityInfo.PartitionKey, partition);
                    }

                    if (partition == null)
                        partition = _partitions[entityInfo.PartitionKey];
                    
                    var dbRow = DbRow.RestoreSnapshot(entityInfo, dbRowMemory);

                    partition.InsertOrReplace(dbRow);
                }
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }
            
            return partition;

        }

        public (DbPartition partition, DbRow dbRow) Insert(IMyNoSqlDbEntity entityInfo, List<MyJsonFirstLevelFieldData> fields)
        {
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    _partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = _partitions[entityInfo.PartitionKey];
                
                var dbRow = DbRow.CreateNew(entityInfo, fields);
                
                if (partition.Insert(dbRow))
                    return (partition, dbRow);
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return (null, null);
        }
        
        public (DbPartition partition, DbRow dbRow) InsertOrReplace(IMyNoSqlDbEntity entityInfo, List<MyJsonFirstLevelFieldData> fields)
        {
           
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    _partitions.Add(entityInfo.PartitionKey, DbPartition.Create(entityInfo.PartitionKey));

                var partition = _partitions[entityInfo.PartitionKey];

                var dbRow = DbRow.CreateNew(entityInfo, fields);
                partition.InsertOrReplace(dbRow);
                
                return (partition, dbRow);
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

        }    

        public DbRow GetEntity(string partitionKey, string rowKey)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {

                if (!_partitions.ContainsKey(partitionKey))
                    return null;
                var partition = _partitions[partitionKey];

                return partition.GetRow(rowKey);

            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


        public IReadOnlyList<DbRow> GetAllRecords(int? limit)
        {
            var result = new List<DbRow>();
            ReaderWriterLockSlim.EnterReadLock();
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
                ReaderWriterLockSlim.ExitReadLock();
            }

            return result;
        }

        public IEnumerable<DbRow> GetRecords(string partitionKey, int? limit, int? skip)
        {
            ReaderWriterLockSlim.EnterReadLock();
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
                ReaderWriterLockSlim.ExitReadLock();
            }
        }

        public (DbPartition dbPartition, DbRow dbRow) DeleteRow(string partitionKey, string rowKey)
        {
            ReaderWriterLockSlim.EnterWriteLock();
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
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return (null, null);
        }
        
        public (DbPartition dbPartition, IReadOnlyList<DbRow> dbRows) CleanAndKeepLastRecords(string partitionKey, int amount)
        {
            ReaderWriterLockSlim.EnterWriteLock();
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
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }

        public bool HasRecord(IMyNoSqlDbEntity entityInfo)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                if (!_partitions.ContainsKey(entityInfo.PartitionKey))
                    return false;

                var partition = _partitions[entityInfo.PartitionKey];

                return partition.HasRecord(entityInfo.RowKey);
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


        public (IEnumerable<DbPartition> partitions, IReadOnlyList<DbRow> rows) BulkInsertOrReplace(IEnumerable<IMyMemory> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan.ToDbRow())
                .ToList();
            
            
            var partitionsToSync = new Dictionary<string, DbPartition>();
            
            var rowsToSync = new List<DbRow>();
            
            ReaderWriterLockSlim.EnterWriteLock();
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
                ReaderWriterLockSlim.ExitWriteLock();
           
            }


            return (partitionsToSync.Values, rowsToSync);
        }


        public void CleanAndBulkInsert(IEnumerable<IMyMemory> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .ToList();
            
            ReaderWriterLockSlim.EnterWriteLock();
            
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
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }
        
        public IEnumerable<DbPartition> CleanAndBulkInsert(string partitionKey, IEnumerable<IMyMemory> itemsAsArray)
        {

            var dbRows = itemsAsArray
                .Select(arraySpan => arraySpan
                    .ToDbRow())
                .ToArray();
            
            ReaderWriterLockSlim.EnterWriteLock();
            
            

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
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return result.Values;
        }
        
        public void Clean()
        {
            ReaderWriterLockSlim.EnterWriteLock();
            
            try
            {
                _partitions.Clear();
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }
        }


        public IEnumerable<DbRow> ApplyQuery(IEnumerable<QueryCondition> queryConditions)
        {
            var conditionsDict = queryConditions.GroupBy(itm => itm.FieldName).ToDictionary(itm => itm.Key, itm => itm.ToList());

            var partitions = conditionsDict.ContainsKey(DbRowDataUtils.PartitionKeyField)
                ? _partitions.FilterByQueryConditions(conditionsDict[DbRowDataUtils.PartitionKeyField]).ToList()
                : _partitions.Values.ToList();

            if (conditionsDict.ContainsKey(DbRowDataUtils.PartitionKeyField))
                conditionsDict.Remove(DbRowDataUtils.PartitionKeyField);
                
            foreach (var partition in partitions)
                foreach (var dbRow in partition.ApplyQuery(conditionsDict))
                    yield return dbRow;
            
        }


        public int GetRecordsCount(string partitionKey)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return _partitions.Sum(itm => itm.Value.GetRecordsCount());

                return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey].GetRecordsCount() : 0;
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
        }

        public IReadOnlyList<DbRow> GetMultipleRows(string partitionKey, string[] rowKeys)
        {
            ReaderWriterLockSlim.EnterReadLock();
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
                ReaderWriterLockSlim.ExitReadLock();
            }
        }

        public IEnumerable<DbRow> GetHighestRowAndBelow(string partitionKey, string rowKey, int maxAmount)
        {
            ReaderWriterLockSlim.EnterReadLock();
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
                ReaderWriterLockSlim.ExitReadLock();
            }
        }


        private IReadOnlyList<DbPartition> GetPartitionsToGarbageCollect(int maxAmount)
        {
            ReaderWriterLockSlim.EnterReadLock();
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
                ReaderWriterLockSlim.ExitReadLock(); 
            }
        }

        public IReadOnlyList<DbPartition> KeepMaxPartitions(in int amount)
        {
            var partitionsToGc = this.GetPartitionsToGarbageCollect(amount);

            if (partitionsToGc.Count == 0)
                return partitionsToGc;
            
            var result = new List<DbPartition>();
            
            ReaderWriterLockSlim.EnterWriteLock();
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
                ReaderWriterLockSlim.ExitWriteLock();
            }

            return result;

        }


        private DbPartition GetPartitionIfItHasToBeCleaned(string partitionKey, int maxAmount)
        {
            ReaderWriterLockSlim.EnterReadLock();
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
                ReaderWriterLockSlim.ExitReadLock();
            }   
        }


        public DbPartition KeepMaxRecordsAmount(string partitionKey, int maxAmount)
        {

            var dbPartition = GetPartitionIfItHasToBeCleaned(partitionKey, maxAmount);

            if (dbPartition == null)
                return null;

            
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {

                if (dbPartition.GetRecordsCount() <= maxAmount)
                    return null;


                dbPartition.CleanAndKeepLastRecords(maxAmount);
                return dbPartition;
            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

        }
    }
}