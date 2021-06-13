using System.Collections.Generic;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence
{
    public class PersistenceScheduler
    {
        private readonly Dictionary<string, List<ITransactionEvent>> _queuesByTable =
            new ();

        private readonly object _lockObject = new ();

        private List<ITransactionEvent> GetQueueByTable(string tableName)
        {
            if (_queuesByTable.TryGetValue(tableName, out var queue))
                return queue;

            queue = new List<ITransactionEvent>();

            _queuesByTable.Add(tableName, queue);
            return queue;
        }

        public void PublishPersistenceEvent(ITransactionEvent transactionEvent)
        {
            lock (_lockObject)
            {
                var queueByTable = GetQueueByTable(transactionEvent.TableName);
                
                queueByTable.Add(transactionEvent);
            }
        }
        
    }
}