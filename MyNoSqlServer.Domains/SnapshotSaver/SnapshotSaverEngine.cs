using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.SnapshotSaver.Implementation;

namespace MyNoSqlServer.Domains.SnapshotSaver
{




    public class SnapshotSaverEngine
    {
        private readonly DbInstance _dbInstance;
        private readonly ISnapshotStorage _snapshotStorage;
        private readonly IReplicaSynchronizationService _replicaSynchronizationService;
        private readonly ISnapshotSaverScheduler _snapshotSaverScheduler;

        private readonly AsyncLock _asyncLock = new AsyncLock();

        public SnapshotSaverEngine(DbInstance dbInstance, ISnapshotStorage snapshotStorage,
            IReplicaSynchronizationService replicaSynchronizationService,
            ISnapshotSaverScheduler snapshotSaverScheduler)
        {
            _dbInstance = dbInstance;
            _snapshotStorage = snapshotStorage;
            _replicaSynchronizationService = replicaSynchronizationService;
            _snapshotSaverScheduler = snapshotSaverScheduler;
        }
        
        

        public async Task LoadSnapshotsAsync()
        {

            await foreach (var tableLoader in _snapshotStorage.LoadSnapshotsAsync())
            {
                var table = _dbInstance.CreateTableIfNotExists(tableLoader.TableName, 
                    tableLoader.MetaData.Persisted, tableLoader.MetaData.Created);


                await foreach (var snapshot in tableLoader.LoadSnapshotsAsync())
                {
                    try
                    {

                        var partition = table.InitPartitionFromSnapshot(snapshot.Snapshot.AsMyMemory());

                        if (partition != null)
                            _replicaSynchronizationService.PublishInitPartition(table, partition.PartitionKey);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(
                            $"Snapshots {snapshot.TableName}/{snapshot.PartitionKey} could not be loaded: " +
                            e.Message);
                    }
                }
            }

        }

        private readonly Dictionary<string, DateTime> _lastSyncDateTime
            = new Dictionary<string, DateTime>();

        private DateTime GetLastDateTime(string tableName)
        {
            lock (_lastSyncDateTime)
            {
                if (!_lastSyncDateTime.ContainsKey(tableName))
                    return DateTime.Now.AddYears(-20);

                return _lastSyncDateTime[tableName];
            }
        }


        private void UpdateLastDateTime(string tableName, DateTime updateDateTime)
        {
            lock (_lastSyncDateTime)
            {
                if (_lastSyncDateTime.ContainsKey(tableName))
                    _lastSyncDateTime[tableName] = updateDateTime;
                
                _lastSyncDateTime.Add(tableName, updateDateTime);
            }
        }
            

        private async Task SyncTableAsync(ITableToSaveEventsQueue eventsQueue)
        {

            var eventToSync = eventsQueue.Dequeue();

            while (eventToSync != null)
            {
                switch (eventToSync)
                {
                    case CreateTablePersistEvent syncEvent:
                        await _snapshotStorage.CreateTableAsync(syncEvent.Table);
                        break;

                    case SyncTablePersistEvent syncEvent:
                        var lastUpdateDateTime = GetLastDateTime(syncEvent.Table.Name);
                        if (syncEvent.SnapshotDateTime > lastUpdateDateTime)
                            await _snapshotStorage.SaveTableSnapshotAsync(syncEvent.Table);
                        break;

                    case SyncPartitionPersistEvent syncEvent:
                        var lastUpdateDateTime2 = GetLastDateTime(syncEvent.Table.Name);
                        if (syncEvent.SnapshotDateTime > lastUpdateDateTime2)
                        {
                            var partitionSnapshot = new PartitionSnapshot
                            {
                                PartitionKey = syncEvent.Partition.PartitionKey,
                                Snapshot = syncEvent.Partition.GetRows().ToJsonArray().AsArray(),
                                TableName = syncEvent.Table.Name
                            };

                            await _snapshotStorage.SavePartitionSnapshotAsync(partitionSnapshot);
                        }  
                        break;

                    case SyncDeletePartitionPersistEvent syncEvent:
                        await _snapshotStorage.DeleteTablePartitionAsync(syncEvent.Table.Name,
                            syncEvent.Partition.PartitionKey);
                        break;

                    case SyncDeleteTablePersistEvent syncEvent:
                        await _snapshotStorage.DeleteTableAsync(syncEvent.Table.Name);
                        break;
                }
                
                UpdateLastDateTime(eventToSync.Table.Name, eventToSync.SnapshotDateTime);

                eventToSync = eventsQueue.Dequeue();
            }

        }

        public async ValueTask SynchronizeAsync(string tableName)
        {
            try
            {

                await _asyncLock.LockAsync();
                try
                {

                    if (tableName == null)
                    {
                        foreach (var eventsQueue in _snapshotSaverScheduler.GetEventsQueue())
                        {
                            await SyncTableAsync(eventsQueue);
                        }
                    }
                    else
                    {
                        var eventsQueue = _snapshotSaverScheduler.TryGetEventsQueue(tableName);
                        
                        if (eventsQueue != null)
                            await SyncTableAsync(eventsQueue);
                    }
                    
                }
                finally
                {
                    _asyncLock.Unlock();
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("There is something wrong during saving the snapshot. " + e.Message);
            }
            finally
            {
                await Task.Delay(1000);
            }
        }

    }

}