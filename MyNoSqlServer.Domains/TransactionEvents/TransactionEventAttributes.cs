using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;

namespace  MyNoSqlServer.Domains.TransactionEvents
{

    public enum EventSource
    {
        ClientRequest, Synchronization
    }
    
    
    public class TransactionEventAttributes
    {

        public TransactionEventAttributes(List<string> locations, DataSynchronizationPeriod synchronizationPeriod,
            EventSource eventSource,
            IReadOnlyDictionary<string, string> headers)
        {
            Locations = locations;
            SynchronizationPeriod = synchronizationPeriod;
            Headers = headers;
            EventSource = eventSource;
        }
        
        public List<string> Locations { get;  }
        
        public DataSynchronizationPeriod SynchronizationPeriod { get; }
        
        public EventSource EventSource { get; }
        
        public IReadOnlyDictionary<string, string> Headers { get;  }


        public bool HasLocation(string location)
        {
            if (Locations == null)
                return false;


            return Locations.Any(itm => itm == location);
        }
        
    }
}