using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Db.Operations
{
    public class DbTableReadOperationsWithExpiration
    {
        private readonly DbTableWriteOperations _dbTableWriteOperations;

        public DbTableReadOperationsWithExpiration(DbTableWriteOperations dbTableWriteOperations)
        {
            _dbTableWriteOperations = dbTableWriteOperations;
        }
        

        public DbRow TryGetRow(DbTable dbTable, string partitionKey, string rowKey, UpdateExpirationTime updateExpirationTime)
        {
            DbRow result = null;

            dbTable.GetAccessWithWriteLock(dbTableReader =>
            {
                var dbPartition = dbTableReader.TryGetPartition(partitionKey);

                if (dbPartition == null)
                    return;

                result = dbPartition.TryGetRow(rowKey);
                if (result != null)
                    dbPartition.UpdateExpirationTime(result.RowKey, updateExpirationTime);
            });

            if (result != null)
                _dbTableWriteOperations.UpdateExpirationTime(dbTable, new[]{result}, updateExpirationTime);

            return result;
        }

        public IReadOnlyList<DbRow> GetRows(DbTable dbTable,
            int? limit, int? skip, in UpdateExpirationTime updateExpirationTime)
        {
            var result = dbTable.GetRows(limit, skip);

            if (result.Count > 0)
                _dbTableWriteOperations.UpdateExpirationTime(dbTable, result, updateExpirationTime);

            return result;
        }
        
        public IReadOnlyList<DbRow> GetRows(DbTable dbTable, string partitionKey, 
            int? limit, int? skip, in UpdateExpirationTime updateExpirationTime)
        {
            var result = dbTable.GetRows(partitionKey, limit, skip);

            if (result.Count > 0)
                _dbTableWriteOperations.UpdateExpirationTime(dbTable, result, updateExpirationTime);
            
            return result;
        }
        
        public IReadOnlyList<DbRow> GetRowsByRowKey(DbTable dbTable, string rowKey, 
            int? limit, int? skip, in UpdateExpirationTime updateExpirationTime)
        {
            var result = dbTable.GetRowsByRowKey(rowKey, limit, skip);

            if (result.Count > 0)
                _dbTableWriteOperations.UpdateExpirationTime(dbTable, result, updateExpirationTime);

            return result;
        }
        
        public IReadOnlyList<DbRow> GetRows(DbTable dbTable, string partitionKey, IReadOnlyList<string> rowKeys, 
            in UpdateExpirationTime updateExpirationTime)
        {
            var result = dbTable.GetRows(partitionKey, rowKeys);
            
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var dbPartition = dbTableReader.TryGetPartition(partitionKey);
                if (dbPartition == null)
                    return;

                result = dbPartition.GetRows(rowKeys);

            });
            
            if (result.Count > 0)
                _dbTableWriteOperations.UpdateExpirationTime(dbTable, result, updateExpirationTime);

            return result;
        }        
        
        public IReadOnlyList<DbRow> GetHighestRowAndBelow(DbTable dbTable, string partitionKey,
            string rowKey, int maxAmount, 
            in UpdateExpirationTime updateExpirationTime)
        {
            var result = dbTable.GetHighestRowAndBelow(partitionKey, rowKey, maxAmount);

            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var dbPartition = dbTableReader.TryGetPartition(partitionKey);
                if (dbPartition == null)
                    return;

                result = dbPartition.GetHighestRowAndBelow(rowKey, maxAmount);
            });
            
            if (result.Count > 0)
                _dbTableWriteOperations.UpdateExpirationTime(dbTable, result, updateExpirationTime);

            return result;
        }        
        
    }
}