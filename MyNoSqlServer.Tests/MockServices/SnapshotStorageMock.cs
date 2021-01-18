using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Tests.MockServices
{

    public class DeleteSnapshotMockCommand
    {
        public string TableName { get; set; }
        public string PartitionKey { get; set; }
    }
    
    public class SnapshotStorageMock : ISnapshotStorage
    {

        
        public readonly List<object> SnapshotWriteCommands = new List<object>();
        
        public ValueTask SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot)
        {
            SnapshotWriteCommands.Add(partitionSnapshot);
            return new ValueTask();
        }

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable)
        {
            SnapshotWriteCommands.Add(dbTable);
            return new ValueTask();
        }

        public ValueTask DeleteTablePartitionAsync(string tableName, string partitionKey)
        {
            var command = new DeleteSnapshotMockCommand
            {
                PartitionKey = partitionKey,
                TableName = tableName
            };
            SnapshotWriteCommands.Add(command);
            return new ValueTask();
        }

        public ValueTask DeleteTableAsync(string tableName)
        {
            return new ValueTask();
        }

        public async IAsyncEnumerable<ITableLoader> LoadSnapshotsAsync()
        {
            foreach (var tableLoader in Array.Empty<ITableLoader>())
            {
                yield return tableLoader;
            }
        }

        public ValueTask CreateTableAsync(DbTable tableName, bool persistTable)
        {
            return new ValueTask();
        }

        public ValueTask CreateTableAsync(DbTable dbTable)
        {
            return new ValueTask();
        }
    }

}