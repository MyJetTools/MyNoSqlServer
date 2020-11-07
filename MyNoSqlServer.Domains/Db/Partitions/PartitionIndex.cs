using System;
using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.Db.Partitions
{
    public class PartitionIndex
    {
        private readonly Dictionary<string, DbRow> _indexedRows = new Dictionary<string, DbRow>();

        private readonly Func<DbRow, bool> _indexCondition;

        public PartitionIndex(Func<DbRow, bool> indexCondition)
        {
            _indexCondition = indexCondition;
        }

        public void Insert(DbRow dbRow)
        {
            if (!_indexCondition(dbRow)) 
                return;
            
            if (_indexedRows.ContainsKey(dbRow.RowKey))
                _indexedRows[dbRow.RowKey] = dbRow;
            else
                _indexedRows.Add(dbRow.RowKey, dbRow);
        }
        
        public void Update(DbRow dbRow)
        {
            if (!_indexCondition(dbRow))
            {
                if (_indexedRows.ContainsKey(dbRow.RowKey))
                    _indexedRows.Remove(dbRow.RowKey);
                return;
            }

            if (_indexedRows.ContainsKey(dbRow.RowKey))
                _indexedRows[dbRow.RowKey] = dbRow;
            else
                _indexedRows.Add(dbRow.RowKey, dbRow);
        }

        public void Delete(DbRow dbRow)
        {
            if (_indexedRows.ContainsKey(dbRow.RowKey))
                _indexedRows.Remove(dbRow.RowKey);
        }

        public IEnumerable<DbRow> GetIndexedRows()
        {
            return _indexedRows.Values;
        }
        
        
    }
}