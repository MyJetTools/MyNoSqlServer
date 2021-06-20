using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.TransactionEvents;

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
    
    public interface ITablePersistenceStorage
    {
        ValueTask SaveTableAttributesAsync(DbTable dbTable, UpdateTableAttributesTransactionEvent data);
        ValueTask SaveTableSnapshotAsync(DbTable dbTable, InitTableTransactionEvent data);
        ValueTask SavePartitionSnapshotAsync(DbTable dbTable, InitPartitionsTransactionEvent data);
        ValueTask SaveRowUpdatesAsync(DbTable dbTable, UpdateRowsTransactionEvent eventData);
        ValueTask SaveRowDeletesAsync(DbTable dbTable, DeleteRowsTransactionEvent eventData);


        ValueTask FlushIfNeededAsync();
  
        IAsyncEnumerable<ITableLoader> LoadTablesAsync();
        
        
        bool HasDataAtSaveProcess { get; }

    }
}