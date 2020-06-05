using System;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    
    public class SnapshotSaverEngine 
    {
        private static async Task LoadSnapshotsAsync()
        {

            await foreach (var snapshot in ServiceLocator.SnapshotStorage.LoadSnapshotsAsync())
            {
                try
                {
                    var table = DbInstance.CreateTableIfNotExists(snapshot.TableName);
                    var partition = table.InitPartitionFromSnapshot(snapshot.Snapshot.AsMyMemory());

                    if (partition != null)
                        ServiceLocator.DataSynchronizer.PublishInitPartition(table, partition);
                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        $"Snapshots {snapshot.TableName}/{snapshot.PartitionKey} could not be loaded: " +
                        e.Message);
                } 
            }
            
        }

        public async Task TheLoop()
        {
            await LoadSnapshotsAsync();
            
            while (!_appIsShuttingDown || ServiceLocator.SnapshotSaverScheduler.TasksToSyncCount()>0)
                try
                {
                    var elementToSave = ServiceLocator.SnapshotSaverScheduler.GetTaskToSync(_appIsShuttingDown);

                    while (elementToSave != null)
                    {
                        switch (elementToSave)
                        {
                            
                            case SyncTable syncTable:
                                await ServiceLocator.SnapshotStorage.SaveTableSnapshotAsync(syncTable.DbTable);
                                break;
                            
                            case SyncPartition syncPartition:
                                
                                var partitionSnapshot = PartitionSnapshot.Create(syncPartition.DbTable, syncPartition.DbPartition);
                                await ServiceLocator.SnapshotStorage.SavePartitionSnapshotAsync(partitionSnapshot);
                                break;
                            
                            case SyncDeletePartition syncDeletePartition:
                                await ServiceLocator.SnapshotStorage.DeleteTablePartitionAsync(syncDeletePartition.TableName,
                                    syncDeletePartition.PartitionKey);
                                break;
                            
                        }

                        elementToSave = ServiceLocator.SnapshotSaverScheduler.GetTaskToSync(_appIsShuttingDown);
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