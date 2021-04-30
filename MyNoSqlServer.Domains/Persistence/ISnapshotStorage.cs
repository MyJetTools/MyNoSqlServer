using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Persistence
{
    
    public class PartitionSnapshot
    {
        public string PartitionKey { get; set; }
        public byte[] Snapshot { get; set; }

        public static PartitionSnapshot Create(string partitionKey, byte[] snapshot)
        {
            return new PartitionSnapshot
            {
                PartitionKey = partitionKey,
                Snapshot = snapshot
            };
        }

        public override string ToString()
        {
            return PartitionKey;
        }
    }


    public interface ITableLoader
    {
        string TableName { get; }
        
        bool Persist { get; }

        IAsyncEnumerable<PartitionSnapshot> GetPartitionsAsync();

    }
    
    public interface ISnapshotStorage
    {
        ValueTask SavePartitionSnapshotAsync(DbTable dbTable, PartitionSnapshot partitionSnapshot);
        ValueTask SaveTableSnapshotAsync(DbTable dbTable);
        ValueTask DeleteTablePartitionAsync(DbTable dbTable, string partitionKey);
        IAsyncEnumerable<ITableLoader> LoadTablesAsync();
        ValueTask SetTableAttributesAsync(DbTable dbTable);
    }
}