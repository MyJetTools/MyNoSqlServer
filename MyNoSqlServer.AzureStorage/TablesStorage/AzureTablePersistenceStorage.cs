using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.Persistence.Blobs;

namespace MyNoSqlServer.AzureStorage.TablesStorage
{
    
    public class AzureTablePersistenceStorage : IBlobPersistenceStorage, ITablesPersistenceReader
    {
        private readonly CloudStorageAccount _storageAccount;

        public AzureTablePersistenceStorage(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        private AppLogs _appLogs;
        public void Inject(AppLogs appLogs)
        {
            _appLogs = appLogs;
        }


        public async ValueTask SavePartitionAsync(DbTable dbTable, string partitionKey)
        {
            IReadOnlyList<DbRow> rows = null;
            dbTable.GetReadAccess(readAccess =>
            {
                var partition = readAccess.TryGetPartition(partitionKey);

                if (partition == null)
                    return;
            
                rows = partition.GetAllRows();
            });

            if (rows == null)
            {
                await DeleteTablePartitionAsync(dbTable.Name, partitionKey);
                return;
            }
            
            
            if (rows.Count == 0)
            {
                await DeleteTablePartitionAsync(dbTable.Name, partitionKey);
                return;
            }


            await SavePartitionSnapshotAsync(dbTable.Name, partitionKey, rows.ToJsonArray().AsArray());

        }

        private async ValueTask SavePartitionSnapshotAsync(string tableName, string partitionKey, byte[] snapshot)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(tableName);

            if (container == null)
            {
                _appLogs?.WriteInfo(tableName, "SavePartitionSnapshotAsync", $"{tableName}/{partitionKey}", "Skipped synchronizing partition");
                return;
            }

            await container.SavePartitionAsync(partitionKey, snapshot);
            
            _appLogs?.WriteInfo(tableName, "SavePartitionSnapshotAsync", $"{tableName}/{partitionKey}", "Partition Saved");
        }

        public async ValueTask SaveTableAsync(DbTable dbTable)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            if (container == null)
            {
                _appLogs?.WriteInfo(dbTable.Name, "SaveTableSnapshotAsync", dbTable.Name, "Skipped synchronizing table");
                return;
            }

            await container.CleanContainerAsync();
            _appLogs?.WriteInfo(dbTable.Name, "SaveTableSnapshotAsync", dbTable.Name, "Container cleaned");

            var partitions = dbTable.GetReadAccess(readAccess =>
            {
                var partitionSnapshots = new Dictionary<string, byte[]>();

                foreach (var dbPartition in readAccess.GetAllPartitions())
                {
                    partitionSnapshots.Add(dbPartition.PartitionKey, dbPartition.GetAllRows().ToJsonArray().AsArray());
                }

                return partitionSnapshots;
            });

            foreach (var (partitionKey, snapshot) in partitions)
            {
                await container.SavePartitionAsync(partitionKey, snapshot);
            
                _appLogs?.WriteInfo(dbTable.Name, "SaveTableSnapshotAsync", $"{dbTable.Name}/{partitionKey}", "Saved snapshot");
            }
        }

        private async ValueTask DeleteTablePartitionAsync(string tableName, string partitionKey)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(tableName);
            if (container == null)
            {
                _appLogs?.WriteInfo(tableName, "DeleteTablePartitionAsync", $"{tableName}/{partitionKey}", "Skipped deleting snapshot");
                return;
            }

            await container.DeletePartitionAsync(partitionKey);

            _appLogs?.WriteInfo(tableName, "DeleteTablePartitionAsync", $"{tableName}/{partitionKey}", "Snapshot is deleted");

        }



        public async IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {

            await foreach (var container in _storageAccount.GetListOfContainersAsync())
            {
                if (container.Name == SystemFileNames.SystemContainerName)
                    continue;

                var tableMetadata = await TableMetadataSaver.GetTableMetadataAsync(container);

                var loader = new AzurePartitionsLoader(container, tableMetadata.Persist, tableMetadata.MaxPartitionsAmount);

                yield return loader;
            }
        }


        public async ValueTask SaveTableAttributesAsync(DbTable dbTable)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            await TableMetadataSaver.SaveTableMetadataAsync(container, dbTable.Persist, dbTable.MaxPartitionsAmount);
        }


    }
    
    
}