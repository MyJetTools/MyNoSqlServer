using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.AzureStorage
{
    
    public class AzureBlobSnapshotStorage : ISnapshotStorage
    {
        private readonly CloudStorageAccount _storageAccount;

        public AzureBlobSnapshotStorage(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        public async ValueTask SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(partitionSnapshot.TableName);

            if (container == null)
            {
                
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped synchronizing snapshot: {partitionSnapshot}");
                return;
            }

            await container.SavePartitionAsync(partitionSnapshot.PartitionKey, partitionSnapshot.Snapshot);
            
            Console.WriteLine($"{DateTime.UtcNow:s} Saved snapshot: {partitionSnapshot}");
        }

        public async ValueTask SaveTableSnapshotAsync(DbTable dbTable)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped synchronizing table: {dbTable.Name}");
                return;
            }

            await container.CleanContainerAsync();
            Console.WriteLine($"{DateTime.UtcNow:s} Container cleaned: {dbTable.Name}");

            var partitions = dbTable.GetAllPartitions();

            foreach (var dbPartition in partitions)
            {
                var data = dbPartition.GetAllRows().ToJsonArray().AsArray();
                await container.SavePartitionAsync(dbPartition.PartitionKey, data);
            
                Console.WriteLine($"{DateTime.UtcNow:s} Saved snapshot: {dbTable.Name}/{dbPartition.PartitionKey}");
            }
        }

        public async ValueTask DeleteTablePartitionAsync(string tableName, string partitionKey)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(tableName);
            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped deleting snapshot: {tableName}/{partitionKey}");
                return;
            }

            await container.DeletePartitionAsync(partitionKey);
            
            Console.WriteLine($"{DateTime.UtcNow:s} Snapshot is deleted: {tableName}/{partitionKey}");
            
        }

        public async IAsyncEnumerable<PartitionSnapshot> LoadSnapshotsAsync()
        {

            const string ignoreContainerName = "nosqlsnapshots";

            await foreach (var container in _storageAccount.GetListOfContainersAsync())
            {

                if (container.Name == ignoreContainerName)
                    continue;

                await foreach (var blockBlob in container.GetListOfBlobsAsync())
                {
                    var memoryStream = new MemoryStream();

                    await blockBlob.DownloadToStreamAsync(memoryStream);

                    var snapshot = new PartitionSnapshot
                    {
                        TableName = container.Name,
                        PartitionKey = blockBlob.Name.Base64ToString(),
                        Snapshot = memoryStream.ToArray()
                    };

                    yield return snapshot;
                    Console.WriteLine("Loaded snapshot: " + snapshot);
                }
            }

        }

        public async IAsyncEnumerable<string> GetPersistedTablesAsync()
        {
            await foreach (var container in _storageAccount.GetListOfContainersAsync())
                yield return container.Name;
        }
    }
    
    
}