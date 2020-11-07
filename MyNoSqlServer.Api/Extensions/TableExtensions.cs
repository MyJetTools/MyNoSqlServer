using System.Linq;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Api.Extensions
{
    public static class TableExtensions
    {
        public static int RecordsCount(this DbTable dbTable)
        {
            if (dbTable == null)
                return 0;

            var result = 0;
            dbTable.GetAccessWithReadLock(tableReader =>
            {
                result = tableReader.Partitions.Values.Sum(dbPartition => dbPartition.GetRecordsCount());
            });

            return result;
        }
        
        public static int PartitionsCount(this DbTable dbTable)
        {
            if (dbTable == null)
                return 0;

            var result = 0;
            dbTable.GetAccessWithReadLock(tableReader =>
            {
                result = tableReader.Partitions.Count;
            });

            return result;
        }
        
    }
}