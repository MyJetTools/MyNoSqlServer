using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.Api.DataReadersTcpServer
{
    public class DataReaderTcpService : TcpContext<IMyNoSqlTcpContract>, IReaderConnection
    {
        
        private IReadOnlyList<string> _tablesSubscribed = new List<string>();

        public string Ip => TcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";
        
        public IEnumerable<string> Tables => _tablesSubscribed;

        string IReaderConnection.Name => ContextName;

        public DateTime ConnectedTime  => SocketStatistic.ConnectionTime;

        public DateTime LastIncomingTime => SocketStatistic.LastReceiveTime;

        string IReaderConnection.Id => Id.ToString();
        

        protected override ValueTask OnConnectAsync()
        {
            return new ValueTask();
        }

        protected override ValueTask OnDisconnectAsync()
        {
            lock (_tablesSubscribed)
            {
                foreach (var tableName in _tablesSubscribed)
                    ServiceLocator.DataReadersTcpBroadcaster.Unsubscribe( this, tableName);

            }

            return new ValueTask();
        }
        
        private void HandleGreeting(GreetingContract greetingContract)
        {
            Console.WriteLine("Greeting: "+greetingContract.Name);
            SetContextName(greetingContract.Name);
        }




        protected override ValueTask HandleIncomingDataAsync(IMyNoSqlTcpContract data)
        {
            switch (data)
            {
                case PingContract _:
                    SendDataToSocket(PongContract.Instance);
                    if (ContextName.Contains("test"))
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine(DateTime.UtcNow.ToString("s")+" Send pong to the "+ContextName);
                        Console.ResetColor();
                    }
                    break;
                
                case GreetingContract greetingContract:
                    HandleGreeting(greetingContract);
                    break;
                
                case SubscribeContract subscribeContract:
                    HandleSubscribe(subscribeContract);
                    break;
                
            }
            
            return new ValueTask();
        }

        private void HandleSubscribe(SubscribeContract subscribeContract)
        {
            if (string.IsNullOrEmpty(subscribeContract.TableName))
                return;

            var table = ServiceLocator.DbInstance.TryGetTable(subscribeContract.TableName);

            if (table == null)
                return;

            var tables = new List<string>();
            if (_tablesSubscribed.Count >0)
                tables.AddRange(_tablesSubscribed);
            
            tables.Add(subscribeContract.TableName);


            _tablesSubscribed = tables;
            
            ServiceLocator.DataReadersTcpBroadcaster.Subscribe(this, table);

        }
        
    }
}