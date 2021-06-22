using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence
{
    public class PersistenceQueue
    {
        private Dictionary<string, List<ITransactionEvent>> _eventsToPersist = new();

        private readonly object _lockObject = new();


        public int Count
        {
            get
            {
                lock (_lockObject)
                    return _eventsToPersist?.Sum(itm => itm.Value.Count) ?? 0;
            }
        }


        public void NewEvent(ITransactionEvent transactionEvent)
        {
            lock (_lockObject)
            {
                _eventsToPersist ??= new Dictionary<string, List<ITransactionEvent>>();

                if (_eventsToPersist.TryGetValue(transactionEvent.TableName, out var tableQueue))
                    tableQueue.Add(transactionEvent);
                else
                    _eventsToPersist.Add(transactionEvent.TableName, new List<ITransactionEvent> { transactionEvent });

                Console.ForegroundColor = ConsoleColor.Blue;
                Console.WriteLine($"Enqueued persistence message. Table {transactionEvent.TableName}. {transactionEvent.GetType()}");
                Console.ResetColor();

            }
        }

        public Dictionary<string, List<ITransactionEvent>> GetSnapshot()
        {
            lock (_lockObject)
            {
                var result = _eventsToPersist;
                _eventsToPersist = null;
                return result;
            }
        }
        
    }
}