using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence
{
    
    public class PersistenceHandler
    {
        private readonly PersistenceQueue _persistenceQueue;
        private readonly DbInstance _dbInstance;
        private readonly ITablePersistenceStorage _tablePersistenceStorage;


        public PersistenceHandler(
            PersistenceQueue persistenceQueue, DbInstance dbInstance, ITablePersistenceStorage tablePersistenceStorage)
        {
            _persistenceQueue = persistenceQueue;
            _dbInstance = dbInstance;
            _tablePersistenceStorage = tablePersistenceStorage;
        }

        private async Task PersistEvents(DbTable table, IReadOnlyList<ITransactionEvent> events)
        {


            foreach (var @event in events)
                switch (@event)
                {
                    case InitTableTransactionEvent initTableTransactionEvent:
                        await _tablePersistenceStorage.SaveTableSnapshotAsync(table, initTableTransactionEvent);
                        break;
                    
                    case InitPartitionsTransactionEvent initPartitionsTransaction:
                        await _tablePersistenceStorage.SavePartitionSnapshotAsync(table, initPartitionsTransaction);

                        break;
                    
                    case UpdateTableAttributesTransactionEvent syncTableAttributesEvent:
                        await _tablePersistenceStorage.SaveTableAttributesAsync(table, syncTableAttributesEvent);
                        break;
                    
                    case UpdateRowsTransactionEvent updateRowsTransactionEvent:
                        await _tablePersistenceStorage.SaveRowUpdatesAsync(table, updateRowsTransactionEvent);
                        break;
                        
                    case DeleteRowsTransactionEvent deleteRowsTransactionEvent:
                        await _tablePersistenceStorage.SaveRowDeletesAsync(table, deleteRowsTransactionEvent);
                        break;
                }
        }


        private async Task PersistEvents(
            IReadOnlyDictionary<string, IReadOnlyList<ITransactionEvent>> eventsToPersist)
        {

            foreach (var (tableName, events) in eventsToPersist)
            {
                var table = _dbInstance.TryGetTable(tableName);

                if (table == null)
                {
                    Console.WriteLine($"Table {tableName} is not found. Skipping persistence loop");
                }
                else
                {
                    await PersistEvents(table, events);    
                }
                
            }
                

        }

        
        //ToDo - Plug it to timer
        public async ValueTask PersistAsync()
        {
            while (true)
            {
                var eventsToPersist = _persistenceQueue.GetEventsToPersist();

                if (eventsToPersist == null)
                    return;

                await PersistEvents(eventsToPersist);
            }
        }


    }
}