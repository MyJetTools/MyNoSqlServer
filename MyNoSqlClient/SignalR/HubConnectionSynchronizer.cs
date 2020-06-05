using System;
using Microsoft.AspNetCore.SignalR.Client;

namespace MyNoSqlClient.SignalR
{
    public class HubConnectionSynchronizer
    {
        private HubConnection _connection;
        
        public void Set(HubConnection connection)
        {
            _connection = connection;
        }

        public HubConnection Get()
        {

            var result = _connection;
            if (result == null)
                throw new Exception("No active connections");

            return result;
        }
    }
}