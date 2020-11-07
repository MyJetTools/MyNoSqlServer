using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Db.Operations
{
    public static class DbTableReadOperations
    {
        private static readonly IReadOnlyList<DbRow> _emptyRows = Array.Empty<DbRow>();

        public static DbRow TryGetRow(this DbTable dbTable, string partitionKey, string rowKey)
        {
            DbRow result = null;

            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var dbPartition = dbTableReader.TryGetPartition(partitionKey);

                if (dbPartition == null)
                    return;

                result = dbPartition.TryGetRow(rowKey);
            });

            return result;
        }


        public static IReadOnlyList<DbRow> GetRows(this DbTable dbTable,
            int? limit = null, int? skip = null)
        {
            List<DbRow> result = null;
            
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                if(dbTableReader.Partitions.Count == 0)
                    return;
                    
                var dbRows = dbTableReader.GetRows();
                
                if (skip != null)
                    dbRows = dbRows.Skip(skip.Value);
                
                if (limit != null)
                    dbRows = dbRows.Take(limit.Value);

                result = dbRows.ToList();

            });

            return result ?? _emptyRows;
        }
        
        public static IReadOnlyList<DbRow> GetRows(this DbTable dbTable, string partitionKey, 
            int? limit = null, int? skip = null)
        {
            List<DbRow> result = null;
            
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var dbPartition = dbTableReader.TryGetPartition(partitionKey);
                
                if (dbPartition == null)
                    return;
                
                result = new List<DbRow>();
                
                var dbRows = dbPartition.GetRows();
                
                if (skip != null)
                    dbRows = dbRows.Skip(skip.Value);
                
                if (limit != null)
                    dbRows = dbRows.Take(limit.Value);
                
                result.AddRange(dbRows);
            });

            return result ?? _emptyRows;
        }


        public static IReadOnlyList<DbRow> GetRowsByRowKey(this DbTable dbTable, string rowKey,
            int? limit = null, int? skip = null)
        {
            List<DbRow> result = null;

            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                
                if (dbTableReader.Partitions.Count == 0)
                    return;
                
                var dbRows = dbTableReader.Partitions.Values
                    .Select(dbPartition => dbPartition.TryGetRow(rowKey))
                    .Where(dbRow => dbRow != null);

                if (skip != null)
                    dbRows = dbRows.Skip(skip.Value);

                if (limit != null)
                    dbRows = dbRows.Take(limit.Value);


                result = dbRows.ToList();
            });

            return result ?? _emptyRows;
        }

        public static bool HasRecord(this DbTable dbTable, IMyNoSqlDbEntity entityInfo)
        {
            var result = false;
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var partition = dbTableReader.TryGetPartition(entityInfo.PartitionKey);
                if (partition == null)
                    return;


                result = partition.HasRecord(entityInfo.RowKey);

            });

            return result;

        }
        
        public static IReadOnlyList<DbRow> GetRows(this DbTable dbTable, 
            string partitionKey, IReadOnlyList<string> rowKeys)
        {
            IReadOnlyList<DbRow> result = null;
            
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var dbPartition = dbTableReader.TryGetPartition(partitionKey);
                if (dbPartition == null)
                    return;

                result = dbPartition.GetRows(rowKeys);

            });

            return result ?? _emptyRows;
        }


        public static IReadOnlyList<DbRow> GetHighestRowAndBelow(this DbTable dbTable, string partitionKey,
            string rowKey, int maxAmount)
        {
            IReadOnlyList<DbRow> result = null;

            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var dbPartition = dbTableReader.TryGetPartition(partitionKey);
                if (dbPartition == null)
                    return;

                result = dbPartition.GetHighestRowAndBelow(rowKey, maxAmount);
            });

            return result ?? _emptyRows;
        }


    }
    
}