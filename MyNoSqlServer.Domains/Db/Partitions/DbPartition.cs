using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Partitions
{
    
    /// <summary>
    /// DbPartition Uses SlimLock of Table
    /// </summary>
    public class DbPartition
    {
        public string PartitionKey { get; private set; }
        
        private readonly SortedList<string, DbRow> _rows = new SortedList<string, DbRow>();

        private IReadOnlyList<DbRow> _rowsAsList = null;
        
        public DateTime LastAccessTime { get; private set; }

        public bool Insert(DbRow row, DateTime now)
        {
            if (_rows.ContainsKey(row.RowKey))
                return false;
            
            _rows.Add(row.RowKey, row);
            _rowsAsList = null;
            LastAccessTime = now;
            
            return true;
        }

        public void InsertOrReplace(DbRow row)
        {
            if (_rows.ContainsKey(row.RowKey))
                _rows[row.RowKey] = row;
            else
                _rows.Add(row.RowKey, row);
            
            _rowsAsList = null;
            
            LastAccessTime = DateTime.UtcNow;
        }


        public DbRow GetRow(string rowKey)
        {
            LastAccessTime = DateTime.UtcNow;
            return _rows.ContainsKey(rowKey) ? _rows[rowKey] : null;
        }

        public bool HasRecord(string rowKey)
        {
            return _rows.ContainsKey(rowKey);
        }
        
        public IReadOnlyList<DbRow> GetAllRows()
        {
            return _rowsAsList ??= _rows.Values.ToList();
        }
        
        public IReadOnlyList<DbRow> GetRowsWithLimit(int? limit, int? skip)
        {
            LastAccessTime = DateTime.UtcNow;
            IEnumerable<DbRow> result = _rows.Values;


            if (skip != null)
                result = result.Skip(skip.Value);
            
            if (limit != null)
                result = result.Take(limit.Value);
            
            return result.ToList();
        }
        
        

        public static DbPartition Create(string partitionKey)
        {
            return new DbPartition
            {
                PartitionKey = partitionKey
            };
        }

        public DbRow DeleteRow(string rowKey)
        {
            LastAccessTime = DateTime.UtcNow;
            if (!_rows.Remove(rowKey, out var result))
                return null;

            _rowsAsList = null;
            return result;
        }

        public void RestoreRecord(IMyNoSqlDbEntity entityInfo, IMyMemory data)
        {
            if (!_rows.ContainsKey(entityInfo.RowKey))
                _rows.Add(entityInfo.RowKey, DbRow.RestoreSnapshot(entityInfo, data));
        }


        public IEnumerable<DbRow> ApplyQuery(IDictionary<string, List<QueryCondition>> conditionsDict)
        {
            var rows = conditionsDict.ContainsKey(RowJsonUtils.RowKeyFieldName)
                ? _rows.FilterByQueryConditions(conditionsDict[RowJsonUtils.RowKeyFieldName])
                : _rows.Values;

            if (conditionsDict.ContainsKey(RowJsonUtils.RowKeyFieldName))
                conditionsDict.Remove(RowJsonUtils.RowKeyFieldName);

            if (conditionsDict.Count == 0)
            {
                foreach (var row in rows)
                    yield return row;
            }
            else
            {
                foreach (var row in rows)
                    if (row.MatchesQuery(conditionsDict))
                        yield return row;
            }
        }
        
        public IEnumerable<DbRow> GetHighestRowAndBelow(string rowKey, int maxAmount)
        {
            LastAccessTime = DateTime.UtcNow;
            return _rows.GetHighestAndBelow(rowKey, maxAmount);
        }


        public override string ToString()
        {
            return PartitionKey+"; Count: "+_rows.Count;
        }


        public void Clean()
        {
            _rows.Clear();
        }
            
        public IReadOnlyList<DbRow> CleanAndKeepLastRecords(int amount)
        {
            
            LastAccessTime = DateTime.UtcNow;
            if (amount<0)
                throw new Exception("Amount must be greater than zero");
            
            Queue<KeyValuePair<string, DbRow>> rowsByLastInsertDateTime = null;
            
            var result = new List<DbRow>();
            
            while (_rows.Count>amount)
            {
                if (rowsByLastInsertDateTime == null)
                    rowsByLastInsertDateTime = _rows.OrderBy(itm => itm.Value.TimeStamp).ToQueue();
                
                var item = rowsByLastInsertDateTime.Dequeue();
                
                result.Add(item.Value);
                _rows.Remove(item.Key);
            }

            return result;
        }

        public IReadOnlyList<DbRow> GetRows(string[] rowKeys)
        {
            LastAccessTime = DateTime.UtcNow;
            return (from rowKey in rowKeys 
                where _rows.ContainsKey(rowKey) 
                select _rows[rowKey]).ToList();
        }

        public int GetRecordsCount()
        {
            return _rows.Count;
        }


    }
}