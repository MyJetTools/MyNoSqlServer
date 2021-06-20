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


        public DataInitializer(DbInstance dbInstance, ITablePersistenceStorage tablePersistenceStorage)
        {
            _dbInstance = dbInstance;
            _tablePersistenceStorage = tablePersistenceStorage;
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

                if (_tablePersistenceStorage.HasDataAtSaveProcess)
                {
                    Console.WriteLine("We have data to Save... Waiting");
                    await Task.Delay(500);
                    continue;
                }
                
                
                break;
            }
            
        }

    }
    
}