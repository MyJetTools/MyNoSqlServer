using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Partitions;

namespace MyNoSqlServer.Domains.Db.Tables
{
    public static class DbTableExtensions
    {

        public static IReadOnlyList<DbPartition> GetAllPartitionsWithReadLock(this DbTable dbTable)
        {
            
            return dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                return dbTableReader.GetAllPartitions();
            });
            
        }
        
    }
}