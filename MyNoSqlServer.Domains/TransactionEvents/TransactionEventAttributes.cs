using MyNoSqlServer.Abstractions;

namespace  MyNoSqlServer.Domains.TransactionEvents
{
    public class TransactionEventAttributes
    {
        public string Location { get; set; }
        
        public DataSynchronizationPeriod SynchronizationPeriod { get; set; }
        
    }
}