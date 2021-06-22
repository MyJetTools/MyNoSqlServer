using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Partitions
{
    public class DbPartitionsList
    {

        private readonly SortedList<string, DbPartition> _partitions = new ();

        private IReadOnlyList<string> _partitionKeys;


        public DbPartition GetOrCreate(string partitionKey)
        {
            if (_partitions.TryGetValue(partitionKey, out var dbPartition))
            {
                dbPartition.LastTimeAccess = DateTimeOffset.UtcNow;
                return dbPartition;
            }

            dbPartition = new DbPartition(partitionKey);
            
            _partitions.Add(partitionKey, dbPartition);
            Count = _partitions.Count;
            _partitionKeys = null;

            return dbPartition;
        }

        public DbPartition TryGet(string partitionKey)
        {
            if (_partitions.TryGetValue(partitionKey, out var dbPartition))
            {
                dbPartition.LastTimeAccess = DateTimeOffset.UtcNow;
                return dbPartition;
            }

            return null;
        }

        public IReadOnlyList<string> GetAllPartitionKeys()
        {
            return _partitionKeys ??= _partitions.Keys.ToList();
        }
        
        public IEnumerable<DbPartition> GetAllPartitions()
        {
            return _partitions.Values;
        }

        public bool HasPartition(string partitionKey)
        {
            return _partitions.ContainsKey(partitionKey);
        }
        public void InitPartition(DbPartition dbPartition)
        {
            _partitions.Add(dbPartition.PartitionKey, dbPartition);
        }

        public DbPartition DeletePartition(string partitionKey)
        {

            if (_partitions.Remove(partitionKey, out var partition))
            {
                _partitionKeys = null;
                return partition;
            }

            return null;
        }


        public IReadOnlyList<DbPartition> GetPartitionsToGc(int maxPartitionsAmount)
        {

            if (maxPartitionsAmount <= 0)
                return Array.Empty<DbPartition>();

            if (_partitions.Count <= maxPartitionsAmount)
                return Array.Empty<DbPartition>();

            return _partitions.Values.OrderBy(partition => partition.LastTimeAccess)
                .Take(_partitions.Count - maxPartitionsAmount).ToList();
        }


        public bool Clear()
        {

            if (_partitions.Count == 0)
                return false;
            
            _partitions.Clear();
            _partitionKeys = null;

            return true;
        }
        
        public IEnumerable<DbRow> ApplyQuery(IEnumerable<QueryCondition> queryConditions)
        {
            var conditionsDict = queryConditions.GroupBy(itm => itm.FieldName)
                .ToDictionary(itm => itm.Key, itm => itm.ToList());

            var partitions = conditionsDict.ContainsKey(RowJsonUtils.PartitionKeyFieldName)
                ? _partitions.FilterByQueryConditions(conditionsDict[RowJsonUtils.PartitionKeyFieldName]).ToList()
                : _partitions.Values.ToList();

            if (conditionsDict.ContainsKey(RowJsonUtils.PartitionKeyFieldName))
                conditionsDict.Remove(RowJsonUtils.PartitionKeyFieldName);

            var now = DateTimeOffset.UtcNow;
            foreach (var partition in partitions)
            {
                partition.LastTimeAccess = now;
                foreach (var dbRow in partition.ApplyQuery(conditionsDict))
                    yield return dbRow;
                
            }

        }
        
        public int Count { get; private set; }



    }
}