using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence.Blobs
{
    public class BlobsSaver : IPersistenceShutdown
    {
        private readonly PersistenceQueue _persistenceQueue;
        private readonly DbInstance _dbInstance;
        private readonly AppLogs _appLogs;
        private readonly IBlobPersistenceStorage _blobPersistenceStorage;

        public BlobsSaver(PersistenceQueue persistenceQueue, DbInstance dbInstance, 
            AppLogs appLogs, IBlobPersistenceStorage blobPersistenceStorage)
        {
            _persistenceQueue = persistenceQueue;
            _dbInstance = dbInstance;
            _appLogs = appLogs;
            _blobPersistenceStorage = blobPersistenceStorage;
        }
        
        

        private async Task FlushToBlobAsync(string tableName, List<ITransactionEvent> eventsByTable)
        {

            try
            {
                var dbTable = _dbInstance.GetTable(tableName);

                var snapshotIteration = SnapshotIteration.Create(eventsByTable);

                if (snapshotIteration.HasSyncTableAttributes)
                    await _blobPersistenceStorage.SaveTableAttributesAsync(dbTable);

                
                if (snapshotIteration.SyncWholeTable)
                {
                    await _blobPersistenceStorage.SaveTableAsync(dbTable);
                    return;
                }

                
                if (snapshotIteration.PartitionsToSync == null)
                    return;

                foreach (var (partitionKey, _) in snapshotIteration.PartitionsToSync)
                    await _blobPersistenceStorage.SavePartitionAsync(dbTable, partitionKey);



            }
            catch (Exception e)
            {
                _appLogs.WriteError(tableName, "BlobsSaver.FlushToBlobAsync", "N/A", e);
            }

        }


        private async Task FlushToBlobAsync(Dictionary<string, List<ITransactionEvent>> events)
        {
            try
            {
                foreach (var (tableName, tableEvents) in events)
                    await FlushToBlobAsync(tableName, tableEvents);

            }
            finally
            {
                HasDataInProcess = false;
            }
        }

        public ValueTask FlushToBlobAsync()
        {
            HasDataInProcess = true;
            var snapshot = _persistenceQueue.GetSnapshot();

            if (snapshot == null)
            {
                HasDataInProcess = false;
                return new ValueTask();
            }
            
            return new ValueTask(FlushToBlobAsync(snapshot));
        }

        public bool HasDataInProcess { get; private set; }
    }
}