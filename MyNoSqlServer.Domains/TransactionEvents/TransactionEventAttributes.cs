using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace  MyNoSqlServer.Domains.TransactionEvents
{

    public enum EventSource
    {
        ClientRequest, Synchronization
    }
    
    
    public class TransactionEventAttributes
    {

        public TransactionEventAttributes(string location, DataSynchronizationPeriod synchronizationPeriod,
            EventSource eventSource,
            IReadOnlyDictionary<string, string> headers)
        {
            Location = location;
            SynchronizationPeriod = synchronizationPeriod;
            Headers = headers;
            EventSource = eventSource;
        }
        
        public string Location { get;  }
        
        public DataSynchronizationPeriod SynchronizationPeriod { get; }
        
        public EventSource EventSource { get; }
        
        public IReadOnlyDictionary<string, string> Headers { get;  }
        
    }
}