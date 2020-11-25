using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Db.Operations
{
    public static class DbTableGcOperations
    {
        
        private static IEnumerable<DbRow> GetExpiredEntities(this DbPartition dbPartition, DateTime now)
        {
            return dbPartition
                .GetRowsWithExpiration()
                .Where(dbRow => dbRow.Expires != null && dbRow.Expires.Value >= now);
        }


        private static readonly IEnumerable<(DbPartition partition, List<DbRow> rows)> EmptyExpiredRowsResult =
            Array.Empty<(DbPartition partition, List<DbRow> rows)>();

        public static IEnumerable<(DbPartition partition, List<DbRow> rows)> GetExpiredEntities(this DbTable dbTable, DateTime nowTime)
        {
            
            /*

            Dictionary<string, (DbPartition partition, List<DbRow> rows)> result = null;
                
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                foreach (var dbPartition in dbTableReader.Partitions.Values)
                {
                    
                    foreach (var expiredRow in dbPartition.GetExpiredEntities(nowTime))
                    {
                        result ??= new Dictionary<string, (DbPartition partition, List<DbRow> rows)>();
                        
                        if (!result.ContainsKey(dbPartition.PartitionKey))
                            result.Add(dbPartition.PartitionKey, (dbPartition, new List<DbRow>()));
                        
                        result[dbPartition.PartitionKey].rows.Add(expiredRow);
                    }
                }
            });
            */

           // return result?.Values ?? EmptyExpiredRowsResult;

           return EmptyExpiredRowsResult;
        }

        internal static DbPartition GetPartitionIfOneHasToBeCleaned(this DbTable dbTable, string partitionKey,
            int maxAmount)
        {
            DbPartition result = null;

            dbTable.GetAccessWithReadLock(dbTableReader =>
            {

                var dbPartition = dbTableReader.TryGetPartition(partitionKey);

                if (dbPartition == null)
                    return;

                if (dbPartition.GetRecordsCount() > maxAmount)
                    result = dbPartition;

            });

            return result;
        }


        private static readonly IReadOnlyList<DbPartition> EmptyPartitionsList = Array.Empty<DbPartition>();
        
        internal static IReadOnlyList<DbPartition> GetPartitionsToGarbageCollect(this DbTable dbTable, int maxAmount)
        {

            IReadOnlyList<DbPartition> result = null;
            
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var partitions = dbTable.AllPartitions;
                
                if (partitions.Count == 0)
                    return;
                
                result = partitions
                    .OrderBy(itm => itm.LastAccessTime)
                    .Take(partitions.Count - maxAmount)
                    .ToList();
            });

            return result ?? EmptyPartitionsList;
        }
        
        
  
        
    }
}