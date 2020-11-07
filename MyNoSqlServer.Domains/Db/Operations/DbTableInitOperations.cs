using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Domains.Db.Operations
{
    public static class DbTableInitOperations
    {
        
        public static DbPartition InitPartitionFromSnapshot(this DbTable dbTable, IMyMemory data)
        {
            DbPartition partition = null;
            
            dbTable.GetAccessWithWriteLock(dbTableWriter =>
            {
                foreach (var dbRowMemory in data.SplitJsonArrayToObjects())
                {
                    var entity = dbRowMemory.ParseDynamicEntity();

                    partition ??= dbTableWriter.GetOrCreatePartition(entity.PartitionKey);
                    
                    var dbRow = DbRow.Restore(entity);

                    partition.InsertOrReplace(dbRow);
                }
            });
            
            return partition;
        }
        
    }
}