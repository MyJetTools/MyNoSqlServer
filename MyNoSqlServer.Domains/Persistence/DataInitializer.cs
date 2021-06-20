using System;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Domains.Persistence
{
    
    public class DataInitializer 
    {
        private readonly DbInstance _dbInstance;
        private readonly ITablePersistenceStorage _tablePersistenceStorage;
        private readonly PersistenceQueue _persistenceQueue;


        public DataInitializer(DbInstance dbInstance, ITablePersistenceStorage tablePersistenceStorage, 
            PersistenceQueue persistenceQueue)
        {
            _dbInstance = dbInstance;
            _tablePersistenceStorage = tablePersistenceStorage;
            _persistenceQueue = persistenceQueue;
        }
        
        public async Task LoadSnapshotsAsync()
        {

            await foreach (var tableLoader in _tablePersistenceStorage.LoadTablesAsync())
            {
                try
                {
                    Console.WriteLine("Restoring table: '"+tableLoader.TableName+"'");
                    var started = DateTime.UtcNow;
                    var table = _dbInstance.RestoreTable(tableLoader.TableName, tableLoader.Persist);

                    if (tableLoader.Persist)
                    {
                        await foreach (var partitionSnapshot in tableLoader.GetPartitionsAsync())
                            table.GetWriteAccess(writeAccess =>
                            {
                                writeAccess.InitPartition(partitionSnapshot.PartitionKey,
                                    partitionSnapshot.GetRecords().ToList(), null);
                            });
                    }
                    
                    Console.WriteLine("Restored table: '"+tableLoader.TableName+"' in "+(DateTime.UtcNow - started));

                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        $"Snapshots  for table {tableLoader.TableName} could not be loaded: " + e.Message);
                } 
            }
            
        }


        public async Task StopAsync()
        {

            while (true)
            {
                var unsavedAmount = _persistenceQueue.GetUnsavedAmount();

                if (unsavedAmount > 0)
                {
                    Console.WriteLine($"We have data in Queue to save amounted as {unsavedAmount}... Waiting");
                    await Task.Delay(500);
                    continue;
                }

                if (_tablePersistenceStorage.HasDataAtSaveProcess)
                {
                    Console.WriteLine("We have data in Save... Waiting");
                    await Task.Delay(500);
                    continue;
                }
                
                
                break;
            }
            
        }

    }
    
}