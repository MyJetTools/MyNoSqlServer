using System.Threading.Tasks;
using AsyncAwaitUtils;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence
{
    
    
    public class PersistenceHandler
    {
        private readonly AsyncQueue<ITransactionEvent> _persistenceQueue = new ();
        private readonly DbInstance _dbInstance;
        private readonly ITablePersistenceStorage _tablePersistenceStorage;
        private readonly AppLogs _appLogs;


        public PersistenceHandler(DbInstance dbInstance, ITablePersistenceStorage tablePersistenceStorage, AppLogs appLogs)
        {
            _dbInstance = dbInstance;
            _tablePersistenceStorage = tablePersistenceStorage;
            _appLogs = appLogs;
        }
        
        public void PersistEvent(ITransactionEvent @event)
        {
            _persistenceQueue.Put(@event);
        }

        private bool _working;

        private Task _task;

        private async Task TaskLoop()
        {

            while (_working)
            {
                var @event = await _persistenceQueue.GetAsync();

                var table = _dbInstance.TryGetTable(@event.TableName);

                if (table == null)
                {
                    _appLogs.WriteInfo(@event.TableName, "PersistenceHandler.Loop", "N/A", "Table not found");
                    continue;
                }

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


        }

        public void Start()
        {
            _working = true;
            _task = TaskLoop();
        }


    }
}