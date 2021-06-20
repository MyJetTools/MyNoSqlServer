using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Domains.TransactionEvents
{


    public class SyncEventsDispatcher
    {
        public void Dispatch(ITransactionEvent syncEntitiesEvent)
        {
            foreach (var subscriber in _subscribers)
                subscriber(syncEntitiesEvent);
        }

        private readonly List<Action<ITransactionEvent>> _subscribers = new ();


        public void SubscribeOnSyncEvent(Action<ITransactionEvent> callback)
        {
            _subscribers.Add(callback);
        }
    }
   
}