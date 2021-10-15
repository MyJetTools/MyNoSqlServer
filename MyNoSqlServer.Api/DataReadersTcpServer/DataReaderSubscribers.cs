using System.Collections.Generic;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Api.DataReadersTcpServer
{
    public class DataReaderSubscribers
    {
        private readonly Dictionary<string, IReadOnlyList<DataReaderTcpService>> _subscribers 
            = new ();

        public void Unsubscribe(string tableName, DataReaderTcpService tcpService)
        {
            lock (_subscribers)
            {
                if (!_subscribers.ContainsKey(tableName))
                    return;
                _subscribers[tableName] = _subscribers[tableName].RemoveFromReadOnlyList(itm => itm.Id == tcpService.Id);
            }

        }


        public void Subscribe(string tableName, DataReaderTcpService changes)
        {
            lock (_subscribers)
            {
                if (!_subscribers.ContainsKey(tableName))
                {
                    _subscribers.Add(tableName, new[]{changes});
                    return;
                }

                _subscribers[tableName] = _subscribers[tableName].AddToReadOnlyList(changes);
            }
        }

        public IReadOnlyList<DataReaderTcpService> GetSubscribers(string tableName)
        {
            lock (_subscribers)
            {
                if (_subscribers.ContainsKey(tableName))
                    return _subscribers[tableName];

                return null;
            }
            
        }
    }
}