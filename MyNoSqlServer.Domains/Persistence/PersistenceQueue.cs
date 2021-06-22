using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence
{
    public class PersistenceQueue
    {
        private Dictionary<string, List<ITransactionEvent>> _eventsToPersist;

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
                Console.WriteLine($"Enqueued persistence message. Table {transactionEvent.TableName}. {transactionEvent.GetType()} {transactionEvent.GetLocationsAsString()}");
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


    public static class TransactionEventUtils
    {
        public static string GetLocationsAsString(this ITransactionEvent transactionEvent)
        {
            if (transactionEvent.Attributes == null)
                return "Attribute is null";

            if (transactionEvent.Attributes.Locations == null)
                return "Null";

            var result = new StringBuilder();

            foreach (var location in transactionEvent.Attributes.Locations)
            {
                result.Append(location + ";");
            }

            return result.ToString();

        }
    }
}