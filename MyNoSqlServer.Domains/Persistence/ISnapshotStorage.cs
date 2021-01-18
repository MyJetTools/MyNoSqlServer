using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Persistence
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

        public static PartitionSnapshot Create(DbTable table, string partitionKey)
        {
            var dbRowsAsByteArray = table.GetRows(partitionKey).ToJsonArray().AsArray();
            return Create(table.Name, partitionKey, dbRowsAsByteArray);
        }


        public override string ToString()
        {
            return TableName + "/" + PartitionKey;
        }
    }


    public interface ITableMetaData
    {
        public DateTime Created { get; }
        public bool Persisted { get; }
    }


    public interface ITableLoader
    {
        string TableName { get; }
        IAsyncEnumerable<PartitionSnapshot> LoadSnapshotsAsync();
        
        ITableMetaData MetaData { get; }
    }
    
    public interface ISnapshotStorage
    {
        ValueTask SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot);
        ValueTask SaveTableSnapshotAsync(DbTable dbTable);
        ValueTask DeleteTablePartitionAsync(string tableName, string partitionKey);
        
        ValueTask DeleteTableAsync(string tableName);
        
        IAsyncEnumerable<ITableLoader> LoadSnapshotsAsync();

        ValueTask CreateTableAsync(DbTable tableName);
    }
}