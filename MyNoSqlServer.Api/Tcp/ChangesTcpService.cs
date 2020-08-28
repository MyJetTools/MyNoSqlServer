using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.ChangesBroadcasting;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.Api.Tcp
{
    public class ChangesTcpService : TcpContext<IMyNoSqlTcpContract>, IChangesBroadcaster
    {

        private List<string> _subscribed;
        
        protected override ValueTask OnConnectAsync()
        {
            _broadCastId = Id + "-" + Guid.NewGuid().ToString("N");
            Ip = TcpClient.Client.RemoteEndPoint.ToString();
            return new ValueTask();
        }

        protected override ValueTask OnDisconnectAsync()
        {
            if (_subscribed != null)
                ServiceLocator.ChangesSubscribers.Unsubscribe(_subscribed, this);
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
                    SendPacket(PongContract.Instance);
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


            _subscribed ??= new List<string>();
            
            _subscribed.Add(subscribeContract.TableName);
            
            ServiceLocator.ChangesSubscribers.Subscribe(new[]{subscribeContract.TableName}, this);

            
            var rows = table.GetAllRecords(null);
            
            Console.WriteLine($"Socket {Id} is subscribed to the table {subscribeContract.TableName}. Initialized records: {rows.Count}");


        }

        IReadOnlyList<string> IChangesBroadcaster.Tables => _subscribed;

        string IChangesBroadcaster.Name => ContextName;


        public string Ip { get; private set; }

        DateTime IChangesBroadcaster.Created => SocketStatistic.ConnectionTime;

        DateTime IChangesBroadcaster.LastUpdate => SocketStatistic.LastReceiveTime;

        public void PublishInitTable(DbTable dbTable)
        {
            var initContract = new InitTableContract
            {
                TableName = dbTable.Name,
                Data = dbTable.GetJsonArray() 
            };

            SendPacket(initContract);
        }

        public void PublishInitPartition(DbTable dbTable, DbPartition partition)
        {

            var packetToBroadcast = new InitPartitionContract
            {
                TableName = dbTable.Name,
                PartitionKey = partition.PartitionKey,
                Data = partition.GetAllRows().ToJsonArray().AsArray()
            };
            
            SendPacket(packetToBroadcast);
        }

        public void SynchronizeUpdate(DbTable dbTable, DbRow dbRow)
        {
            SynchronizeUpdate(dbTable, new[]{dbRow});
        }

        public void SynchronizeUpdate(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
            var packetToBroadcast = new UpdateRowsContract
            {
                TableName = dbTable.Name,
                Data = dbRows.ToJsonArray().AsArray()
            };

            SendPacket(packetToBroadcast);
        }

        public void SynchronizeDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {

            var packetToBroadcast = new DeleteRowsContract
            {
                TableName = dbTable.Name,
                RowsToDelete = dbRows.Select(row => (row.PartitionKey, row.RowKey)).ToList()
            };
            
            SendPacket(packetToBroadcast);
        }

        private string _broadCastId;
        string IChangesBroadcaster.Id => _broadCastId;
    }
}