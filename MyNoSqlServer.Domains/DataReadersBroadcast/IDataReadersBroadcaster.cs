using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.DataReadersBroadcast
{
    public interface IDataReadersBroadcaster
    {
        public void BroadcastEvent(ITransactionEvent @event);
    }
}