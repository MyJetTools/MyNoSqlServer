using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    
    public class PartitionSnapshot
    {
        public string TableName { get; set; }
        public string PartitionKey { get; set; }
        public byte[] Snapshot { get; set; }

        public static PartitionSnapshot Create(string tableName, string partitionKey, byte[] snapshot)
        {
            return new PartitionSnapshot
            {
                TableName = tableName,
                PartitionKey = partitionKey,
                Snapshot = snapshot
            };
        }

        public static PartitionSnapshot Create(DbTable table, DbPartition partition)
        {
            var dbRowsAsByteArray = partition.GetAllRows().ToJsonArray().AsArray();
            return Create(table.Name, partition.PartitionKey, dbRowsAsByteArray);
        }


        public override string ToString()
        {
            return TableName + "/" + PartitionKey;
        }
    }
    
    public interface ISnapshotStorage
    {
        ValueTask SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot);
        ValueTask SaveTableSnapshotAsync(DbTable dbTable);
        ValueTask DeleteTablePartitionAsync(DbTable dbTable, string partitionKey);
        IAsyncEnumerable<PartitionSnapshot> LoadSnapshotsAsync();
    }
}