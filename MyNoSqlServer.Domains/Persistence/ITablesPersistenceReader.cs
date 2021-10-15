using System.Collections.Generic;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
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
        
        int MaxPartitionsAmount { get; }

        IAsyncEnumerable<PartitionSnapshot> GetPartitionsAsync();

    }
    
    public interface ITablesPersistenceReader
    {
        
        IAsyncEnumerable<ITableLoader> LoadTablesAsync();
    }
}