using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Partitions
{


    public struct UpdateExpirationTime
    {
        public DateTime? ExpiresDate { get; set; }
        
        public bool ClearExpiresDate { get; set; }
    }
    
    
    /// <summary>
    /// DbPartition Uses SlimLock of Table
    /// </summary>
    public class DbPartition 
    {
        public int DataSize { get; private set; }
        
        public string PartitionKey { get; }
        
        private readonly SortedList<string, DbRow> _rows = new SortedList<string, DbRow>();
        
        private readonly PartitionIndex _expirationIndex = new PartitionIndex(dbRow => dbRow.Expires != null);

        public DateTime LastAccessTime { get; private set; }

        public DbPartition(string partitionKey)
        {
            PartitionKey = partitionKey;
        }

        private void UpdatePartitionDataSize(DbRow oldRow, DbRow newRow)
        {
            if (oldRow != null)
                DataSize -= oldRow.Data.Length;

            if (newRow != null)
                DataSize += newRow.Data.Length;
        }

        public bool Insert(DbRow row, DateTime now)
        {
            if (_rows.ContainsKey(row.RowKey))
                return false;
            
            _rows.Add(row.RowKey, row);
            LastAccessTime = now;
            
            _expirationIndex.Insert(row);
            
            UpdatePartitionDataSize(null, row);
            
            return true;
        }

        public void InsertOrReplace(DbRow row)
        {
            if (_rows.TryGetValue(row.RowKey, out var oldRow))
            {
                _rows[row.RowKey] = row;
                _expirationIndex.Update(row);
            }
            else
            {
                _rows.Add(row.RowKey, row);
                _expirationIndex.Insert(row);  
            }
            
            UpdatePartitionDataSize(oldRow, row);
            
            LastAccessTime = DateTime.UtcNow;
        }


        internal DbRow TryGetRow(string rowKey)
        {
            LastAccessTime = DateTime.UtcNow;
            return _rows.TryGetValue(rowKey, out var result) 
                ? result 
                : null;
        }

        internal void UpdateExpirationTime(string rowKey, in UpdateExpirationTime updateExpirationTime)
        {
            if (!_rows.ContainsKey(rowKey)) 
                return;
            
            var dbRow = _rows[rowKey];

            if (updateExpirationTime.ExpiresDate != null)
                dbRow.Expires = updateExpirationTime.ExpiresDate;

            if (updateExpirationTime.ClearExpiresDate)
                dbRow.Expires = null;
            
            _expirationIndex.Update(dbRow);
        }


        public bool HasRecord(string rowKey)
        {
            return _rows.ContainsKey(rowKey);
        }
        
        internal IEnumerable<DbRow> GetAllRows()
        {
            return _rows.Values;
        }
        
        public IEnumerable<DbRow> GetRowsWithExpiration()
        {
            return _expirationIndex.GetIndexedRows();
        }

        public static DbPartition Create(string partitionKey)
        {
            return new DbPartition(partitionKey);
        }
        
        public IEnumerable<DbRow> TryDeleteRows(IEnumerable<string> rowKeys)
        {
            LastAccessTime = DateTime.UtcNow;

            foreach (var rowKey in rowKeys)
            {
                if (!_rows.ContainsKey(rowKey))
                    continue;
                
                var result = _rows[rowKey];
                _rows.Remove(rowKey);
                _expirationIndex.Delete(result);
                yield return result;
                
            }
        }

        public DbRow TryDeleteRow(string rowKey)
        {
            LastAccessTime = DateTime.UtcNow;
            if (!_rows.ContainsKey(rowKey))
                return null;

            var result = _rows[rowKey];
            _rows.Remove(rowKey);
            _expirationIndex.Delete(result);
            UpdatePartitionDataSize(result, null);
            return result;
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
        
        public IReadOnlyList<DbRow> GetHighestRowAndBelow(string rowKey, int maxAmount)
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
                rowsByLastInsertDateTime ??= _rows
                    .OrderBy(itm => itm.Value.TimeStamp)
                    .ToQueue();
                
                var item = rowsByLastInsertDateTime.Dequeue();
                
                result.Add(item.Value);
                _rows.Remove(item.Key);
            }

            foreach (var dbRow in result)
                UpdatePartitionDataSize(dbRow, null);

            return result;
        }
        
        internal IEnumerable<DbRow> GetRows()
        {
            LastAccessTime = DateTime.UtcNow;
            return _rows.Values;
        }
        
        public IReadOnlyList<DbRow> GetRows(IEnumerable<string> rowKeys)
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