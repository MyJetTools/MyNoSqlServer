using System;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    
    public class SnapshotSaverEngine 
    {
        private readonly DbInstance _dbInstance;
        private readonly ISnapshotStorage _snapshotStorage;
        private readonly ISnapshotSaverScheduler _snapshotSaverScheduler;

        public SnapshotSaverEngine(DbInstance dbInstance, ISnapshotStorage snapshotStorage, 
            ISnapshotSaverScheduler snapshotSaverScheduler)
        {
            _dbInstance = dbInstance;
            _snapshotStorage = snapshotStorage;
            _snapshotSaverScheduler = snapshotSaverScheduler;
        }
        
        
        public async Task LoadSnapshotsAsync()
        {

            await foreach (var tableLoader in _snapshotStorage.LoadTablesAsync())
            {
                try
                {
                    Console.WriteLine("Restoring table: '"+tableLoader.TableName+"'");
                    var started = DateTime.UtcNow;
                    var table = _dbInstance.RestoreTable(tableLoader.TableName, tableLoader.Persist);

                    await foreach (var partitionSnapshot in tableLoader.GetPartitionsAsync())
                        table.InitPartitionFromSnapshot(partitionSnapshot); 
                    
                    Console.WriteLine("Restored table: '"+tableLoader.TableName+"' in "+(DateTime.UtcNow - started));

                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        $"Snapshots  for table {tableLoader.TableName} could not be loaded: " + e.Message);
                } 
            }
            
        }


        private async Task HandleTaskToSyncAsync(ISyncTask syncTask)
        {
            switch (syncTask)
            {
                            
                case SyncSetTableSavable syncSetTableSavable:
                    await _snapshotStorage.SetTableSavableAsync(syncSetTableSavable.DbTable, syncSetTableSavable.Savable);
                    break;
                            
                case SyncTable syncTable:
                    await _snapshotStorage.SaveTableSnapshotAsync(syncTable.DbTable);
                    break;
                            
                case SyncPartition syncPartition:

                    var snapshot = syncPartition.DbTable.GetPartitionSnapshot(syncPartition.PartitionKey);
                                
                    if (snapshot == null)
                        await _snapshotStorage.DeleteTablePartitionAsync(syncPartition.DbTable, syncPartition.PartitionKey);
                    else
                        await _snapshotStorage.SavePartitionSnapshotAsync(syncPartition.DbTable, snapshot);
                    break;
                            
            }
        }

        public async ValueTask IterateAsync(bool appIsShuttingDown)
        {

            var elementToSave = _snapshotSaverScheduler.GetTaskToSync(appIsShuttingDown);

            while (elementToSave != null)
            {
                await HandleTaskToSyncAsync(elementToSave);
                elementToSave = _snapshotSaverScheduler.GetTaskToSync(appIsShuttingDown);
            }

        }

        public async ValueTask SynchronizeImmediatelyAsync(DbTable dbTable)
        {
            var tasksToSync = _snapshotSaverScheduler.GetTasksToSync(dbTable.Name);

            foreach (var taskToSync in tasksToSync)
            {
                await HandleTaskToSyncAsync(taskToSync);
            }
            
        }
        
        
        public async Task StopAsync()
        {
            var count = _snapshotSaverScheduler.TasksToSyncCount();
            while (count>0)
            {
                Console.WriteLine($"There are {count} tasks to save. Pushing to save");
                await IterateAsync(true);
                count = _snapshotSaverScheduler.TasksToSyncCount();
            }
            
            Console.WriteLine("Snapshot saver is closed properly");
        }
    }
    
}