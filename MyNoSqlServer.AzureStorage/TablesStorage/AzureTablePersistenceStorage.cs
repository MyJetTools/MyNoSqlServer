﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.AzureStorage.TablesStorage
{
    
    
    public class AzureTablePersistenceStorage : ITablePersistenceStorage
    {
        private readonly CloudStorageAccount _storageAccount;


        private Dictionary<string, SnapshotIteration> _snapshotIteration;
        private readonly object _lockObject = new();

        public AzureTablePersistenceStorage(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }


        private async ValueTask SavePartitionAsync(DbTable dbTable, string partitionKey)
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
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped synchronizing delete partition: {tableName}/{partitionKey}. It does not exist");
                
                return;
            }

            await container.SavePartitionAsync(partitionKey, snapshot);
            
            Console.WriteLine($"{DateTime.UtcNow:s} Deleted partition: {tableName}/{partitionKey}");
        }

        private async ValueTask SaveTableSnapshotAsync(DbTable dbTable)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            if (container == null)
            {
                Console.WriteLine($"{DateTime.UtcNow:s} Skipped synchronizing table: {dbTable.Name}");
                return;
            }

            await container.CleanContainerAsync();
            Console.WriteLine($"{DateTime.UtcNow:s} Container cleaned: {dbTable.Name}");

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
            
                Console.WriteLine($"{DateTime.UtcNow:s} Saved snapshot: {dbTable.Name}/{partitionKey}");
            }
        }

        private async ValueTask DeleteTablePartitionAsync(string tableName, string partitionKey)
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


        private void GetSnapshotIteration(DbTable dbTable, Action<SnapshotIteration> callback)
        {
            lock (_lockObject)
            {
                _snapshotIteration ??= new Dictionary<string, SnapshotIteration>();
                
                if (!_snapshotIteration.ContainsKey(dbTable.Name))
                    _snapshotIteration.Add(dbTable.Name, new SnapshotIteration(dbTable));
                
                callback(_snapshotIteration[dbTable.Name]);
            }
        }


        private IReadOnlyDictionary<string, SnapshotIteration> GetSnapshotIterationToCommit()
        {

            lock (_lockObject)
            {
                var result = _snapshotIteration;
                _snapshotIteration = null;

                _hasDataOnProcess = result != null;
                return result;
            }
            
        }
        
        
        public ValueTask SaveTableAttributesAsync(DbTable dbTable, UpdateTableAttributesTransactionEvent data)
        {
            GetSnapshotIteration(dbTable, snapshot =>
            {
                snapshot.SyncTableAttributes();    
            });
            
            return new ValueTask();
        }

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable, InitTableTransactionEvent data)
        {
            GetSnapshotIteration(dbTable, snapshot =>
            {
                snapshot.SyncTableAttributes();    
            });
            
            return new ValueTask();
        }

        public ValueTask SavePartitionSnapshotAsync(DbTable dbTable, InitPartitionsTransactionEvent data)
        {
            GetSnapshotIteration(dbTable, snapshot =>
            {
                foreach (var (partitionKey, _) in data.Partitions)
                {
                    snapshot.SyncPartition(partitionKey);    
                } 
            });
            
            return new ValueTask();
        }

        public ValueTask SaveRowUpdatesAsync(DbTable dbTable, UpdateRowsTransactionEvent eventData)
        {
            GetSnapshotIteration(dbTable, snapshot =>
            {
                foreach (var (partitionKey, _) in eventData.RowsByPartition)
                {
                    snapshot.SyncPartition(partitionKey);    
                } 
            });
            
            return new ValueTask();
        }

        public ValueTask SaveRowDeletesAsync(DbTable dbTable, DeleteRowsTransactionEvent eventData)
        {
            GetSnapshotIteration(dbTable, snapshot =>
            {
                foreach (var (partitionKey, _) in eventData.Rows)
                {
                    snapshot.SyncPartition(partitionKey);    
                } 
            });
            
            return new ValueTask();
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

        private bool _hasDataOnProcess;

        public bool HasDataAtSaveProcess
        {
            get
            {
                lock (_lockObject)
                    return _snapshotIteration != null || _hasDataOnProcess;
            }
        }

        private async ValueTask SaveTableAttributesAsync(DbTable dbTable)
        {
            var container = await _storageAccount.GetBlockBlobReferenceAsync(dbTable.Name);
            await TableMetadataSaver.SaveTableMetadataAsync(container, dbTable.Persist, dbTable.MaxPartitionsAmount);
        }
        
        public async ValueTask FlushIfNeededAsync()
        {
            var snapshots = GetSnapshotIterationToCommit();
            
            if (snapshots == null)
                return;


            foreach (var snapshotIteration  in snapshots.Values)
            {
                if (snapshotIteration.HasSyncTableAttributes)
                    await SaveTableAttributesAsync(snapshotIteration.DbTable);

                
                if (snapshotIteration.SyncWholeTable)
                {
                    await SaveTableSnapshotAsync(snapshotIteration.DbTable);
                    continue;
                }

                
                if (snapshotIteration.PartitionsToSync == null)
                    continue;

                foreach (var (partitionKey, _) in snapshotIteration.PartitionsToSync)
                    await SavePartitionAsync(snapshotIteration.DbTable, partitionKey);
                    
            }

            lock (_lockObject)
                _hasDataOnProcess = false;

        }



    }
    
    
}