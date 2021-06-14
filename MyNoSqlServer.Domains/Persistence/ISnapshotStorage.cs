using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;

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


        public IEnumerable<DbRow> GetRecords()
        {
            var partitionAsMyMemory = new MyMemoryAsByteArray(Snapshot);
            
            foreach (var dbRowMemory in partitionAsMyMemory.SplitJsonArrayToObjects())
            {
                var entity = dbRowMemory.ParseDynamicEntity();
                var dbRow = DbRow.RestoreSnapshot(entity, dbRowMemory);
                yield return dbRow;
            }
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
        ValueTask SavePartitionSnapshotAsync(DbTable dbTable, PartitionSnapshot partitionSnapshot, Dictionary<string, string> headers);
        ValueTask SaveTableSnapshotAsync(DbTable dbTable, Dictionary<string, string> headers);
        ValueTask DeleteTablePartitionAsync(DbTable dbTable, string partitionKey, Dictionary<string, string> headers);
        IAsyncEnumerable<ITableLoader> LoadTablesAsync();
        ValueTask SetTableAttributesAsync(DbTable dbTable, Dictionary<string, string> headers);
    }
}