using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace  MyNoSqlServer.Domains.TransactionEvents
{
    public class TransactionEventAttributes
    {

        public TransactionEventAttributes(string location, DataSynchronizationPeriod synchronizationPeriod,
            IReadOnlyDictionary<string, string> headers)
        {
            Location = location;
            SynchronizationPeriod = synchronizationPeriod;
            Headers = headers;
        }
        
        public string Location { get;  }
        
        public DataSynchronizationPeriod SynchronizationPeriod { get;  }
        
        public IReadOnlyDictionary<string, string> Headers { get;  }
        
    }
}