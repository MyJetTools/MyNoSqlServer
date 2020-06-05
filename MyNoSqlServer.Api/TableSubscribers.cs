using System.Collections.Generic;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Api
{
    public static class TableSubscribers
    {
        private static  readonly Dictionary<string, IReadOnlyList<ChangesTcpService>> Subscribers 
            = new Dictionary<string, IReadOnlyList<ChangesTcpService>>();



        public static void Unsubscribe(string tableName, ChangesTcpService tcpService)
        {
            lock (Subscribers)
            {
                if (!Subscribers.ContainsKey(tableName))
                    return;
                Subscribers[tableName] = Subscribers[tableName].RemoveFromReadOnlyList(itm => itm.Id == tcpService.Id);
            }

        }


        public static void Subscribe(string tableName, ChangesTcpService changes)
        {
            lock (Subscribers)
            {
                if (!Subscribers.ContainsKey(tableName))
                {
                    Subscribers.Add(tableName, new[]{changes});
                    return;
                }

                Subscribers[tableName] = Subscribers[tableName].AddToReadOnlyList(changes);
            }
        }

        public static IReadOnlyList<ChangesTcpService> GetConnections(string tableName)
        {
            lock (Subscribers)
            {
                if (Subscribers.ContainsKey(tableName))
                    return Subscribers[tableName];

                return null;
            }
            
        }
    }
}