using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence
{
    public class PersistenceQueue
    {
        private readonly Dictionary<string, List<ITransactionEvent>> _queuesByTable =
            new ();

        private IReadOnlyList<string> _keys = Array.Empty<string>();

        private readonly object _lockObject = new ();

        public int GetUnsavedAmount()
        {
            lock (_lockObject)
            {
                return _queuesByTable.Sum(itm => itm.Value.Count);
            }
            
        }

        private List<ITransactionEvent> GetQueueByTable(string tableName)
        {
            if (_queuesByTable.TryGetValue(tableName, out var queue))
                return queue;

            queue = new List<ITransactionEvent>();

            _queuesByTable.Add(tableName, queue);
            _keys = _queuesByTable.Keys.ToList();
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
        
        
        


        public IReadOnlyDictionary<string, IReadOnlyList<ITransactionEvent>> GetEventsToPersist()
        {
            lock (_lockObject)
            {

                var eventsAmount = _queuesByTable.Sum(itm => itm.Value.Count);

                if (eventsAmount == 0)
                    return null;

                Dictionary<string, IReadOnlyList<ITransactionEvent>> result = null;

                foreach (var key in _keys)
                {
                    if (_queuesByTable[key].Count > 0)
                    {
                        result ??= new Dictionary<string, IReadOnlyList<ITransactionEvent>>();

                        var items = _queuesByTable[key];
                        _queuesByTable.Remove(key);
                        _queuesByTable.Add(key, new List<ITransactionEvent>());
                        
                        result.Add(key, items);
                    }
                }
                
                return result;
                
            }
        }

    }
}