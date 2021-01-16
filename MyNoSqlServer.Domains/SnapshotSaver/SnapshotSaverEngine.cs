using System;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    
    
    
    
    public class SnapshotSaverEngine 
    {
        private readonly DbInstance _dbInstance;
        private readonly ISnapshotStorage _snapshotStorage;
        private readonly IReplicaSynchronizationService _replicaSynchronizationService;
        private readonly ISnapshotSaverScheduler _snapshotSaverScheduler;


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
                var table = _dbInstance.CreateTableIfNotExists(tableLoader.TableName);


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

        public async Task TheLoop()
        {

            
            while (!_appIsShuttingDown || _snapshotSaverScheduler.TasksToSyncCount()>0)
                try
                {
                    var elementToSave = _snapshotSaverScheduler.GetTaskToSync(_appIsShuttingDown);

                    while (elementToSave != null)
                    {
                        switch (elementToSave)
                        {
                            
                            case SyncTable syncTable:
                                await _snapshotStorage.SaveTableSnapshotAsync(syncTable.DbTable);
                                break;
                            
                            case SyncPartition syncPartition:
                                var partitionSnapshot = PartitionSnapshot.Create(syncPartition.DbTable, syncPartition.PartitionKey);
                                await _snapshotStorage.SavePartitionSnapshotAsync(partitionSnapshot);
                                break;
                            
                            case SyncDeletePartition syncDeletePartition:
                                await _snapshotStorage.DeleteTablePartitionAsync(syncDeletePartition.TableName,
                                    syncDeletePartition.PartitionKey);
                                break;
                            
                        }

                        elementToSave = _snapshotSaverScheduler.GetTaskToSync(_appIsShuttingDown);
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


        private Task _theLoop;

        private bool _appIsShuttingDown;

        public void Start()
        {
            _appIsShuttingDown = false;
            _theLoop = TheLoop();
        }


        public void Stop()
        {
            Console.WriteLine("Shutting down sync tasks");
            _appIsShuttingDown = true;
            _theLoop.Wait();
        }
        
    }
    
}