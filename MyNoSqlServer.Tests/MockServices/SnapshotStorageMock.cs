using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
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

        public async IAsyncEnumerable<PartitionSnapshot> LoadSnapshotsAsync()
        {
            foreach (var partitionSnapshot in Array.Empty<PartitionSnapshot>())
            {
                yield return partitionSnapshot;
            }
        }
    }
}