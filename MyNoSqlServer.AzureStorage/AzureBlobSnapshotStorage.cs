using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.AzureStorage
{
    
    public class AzureBlobSnapshotStorage : ISnapshotStorage
    {
        private readonly CloudStorageAccount _storageAccount;

        public AzureBlobSnapshotStorage(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        public async ValueTask SavePartitionSnapshotAsync(DbTable dbTable, PartitionSnapshot partitionSnapshot)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);

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

        public async ValueTask DeleteTablePartitionAsync(DbTable dbTable, string partitionKey)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped deleting snapshot: {dbTable.Name}/{partitionKey}");
                return;
            }

            await container.DeletePartitionAsync(partitionKey);
            
            Console.WriteLine($"{DateTime.UtcNow:s} Snapshot is deleted: {dbTable.Name}/{partitionKey}");
            
        }

        public async IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {

            await foreach (var container in _storageAccount.GetListOfContainersAsync())
            {
                if (container.Name == SystemFileNames.SystemContainerName)
                    continue;

                var tableMetadata = await TableMetadataSaver.GetTableMetadataAsync(container);

                var loader = new AzurePartitionsLoader(container, tableMetadata.Persist);

                yield return loader;
            }
        }

        public async ValueTask SetTableSavableAsync(DbTable dbTable, bool savable)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            await TableMetadataSaver.SaveTableMetadataAsync(container, savable);
        }

        public async IAsyncEnumerable<string> GetPersistedTablesAsync()
        {
            await foreach (var container in _storageAccount.GetListOfContainersAsync())
                yield return container.Name;
        }
    }
    
    
}