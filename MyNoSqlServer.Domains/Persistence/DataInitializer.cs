using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Domains.Db;

namespace MyNoSqlServer.Domains.Persistence
{
    
    public class DataInitializer 
    {
        private readonly DbInstance _dbInstance;
        private readonly IPersistenceShutdown _persistenceShutdown;
        private readonly PersistenceQueue _persistenceQueue;


        public DataInitializer(DbInstance dbInstance,
            IPersistenceShutdown persistenceShutdown, PersistenceQueue persistenceQueue)
        {
            _dbInstance = dbInstance;
            _persistenceShutdown = persistenceShutdown;
            _persistenceQueue = persistenceQueue;
        }

        public async Task LoadSnapshotsAsync(ITablesPersistenceReader tablesPersistenceReader)
        {

            await foreach (var tableLoader in tablesPersistenceReader.LoadTablesAsync())
            {
                try
                {
                    Console.WriteLine("Restoring table: '"+tableLoader.TableName+"'");
                    var started = DateTime.UtcNow;

                    var dbTable = _dbInstance.GetWriteAccess(writeAccess => writeAccess.CreateTable(
                        tableLoader.TableName, tableLoader.Persist,
                        tableLoader.MaxPartitionsAmount));

                    if (tableLoader.Persist)
                    {
                        await foreach (var partitionSnapshot in tableLoader.GetPartitionsAsync())
                            dbTable.GetWriteAccess(writeAccess =>
                            {
                                writeAccess.InitPartition(partitionSnapshot.PartitionKey,
                                    partitionSnapshot.GetRecords().ToList());
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

            //ToDo - Since we are not loading tables in Node Mode - we have to establish flag - initialized
        }


        public async Task StopAsync()
        {
            return; //ToDo - Disabled while debugging

            while (true)
            {

                if (_persistenceShutdown.HasDataInProcess)
                {
                    Console.WriteLine("We have data to Save... Waiting");
                    await Task.Delay(500);
                    continue;
                }

                var messagesInQueue = _persistenceQueue.Count;

                if (messagesInQueue > 0)
                {
                    Console.WriteLine($"We have {messagesInQueue} messages to persist in the queue... Waiting");
                    await Task.Delay(500);
                    continue;  
                }
                

                break;
            }
            
        }

    }
    
}