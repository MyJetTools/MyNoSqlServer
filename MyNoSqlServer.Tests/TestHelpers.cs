using System;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Tests
{
    public static class TestHelpers
    {

        public static void Insert(this DbTable dbTable, DynamicEntity dynamicEntity, DateTime? now = null)
        {
            var dbRow = DbRow.CreateNew(dynamicEntity, now ?? DateTime.UtcNow);

            dbTable.GetWriteAccess(writeAccess =>
            {
                var partition = writeAccess.GetOrCreatePartition(dbRow.PartitionKey);

                partition.Insert(dbRow);
            });
        }
        
    }
}