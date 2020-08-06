using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.Api
{
    public class ChangesTcpService : TcpContext<IMyNoSqlTcpContract>
    {
        
        
        private  IReadOnlyList<string> _tablesSubscribed = new List<string>();

        public IReadOnlyList<string> Tables => _tablesSubscribed;
        
        protected override ValueTask OnConnectAsync()
        {
            return new ValueTask();
        }

        protected override ValueTask OnDisconnectAsync()
        {
            lock (_tablesSubscribed)
            {
                foreach (var tableName in _tablesSubscribed)
                    TableSubscribers.Unsubscribe(tableName, this);
            }

            return new ValueTask();
        }
        
        private void HandleGreeting(GreetingContract greetingContract)
        {
            Console.WriteLine("Greeting: "+greetingContract.Name);
            SetContextName(greetingContract.Name);
        }


        
        public static void BroadcastInitTable(DbTable dbTable)
        {

            var connections = TableSubscribers.GetConnections(dbTable.Name);
            
            if (connections == null)
                return;
            
            var packetToBroadcast = new InitTableContract
            {
                TableName = dbTable.Name,
                Data = dbTable.GetAllRecords(null).ToHubUpdateContract()
            };

            foreach (var connection in connections)
                    connection.SendPacket(packetToBroadcast);
        }

        public static void BroadcastInitPartition(DbTable dbTable, DbPartition partition)
        {

            var connections = TableSubscribers.GetConnections(dbTable.Name);

            if (connections == null)
                return;

            var packetToBroadcast = new InitPartitionContract
            {
                TableName = dbTable.Name,
                PartitionKey = partition.PartitionKey,
                Data = partition.GetAllRows().ToHubUpdateContract()
            };

            foreach (var connection in connections)
                connection.SendPacket(packetToBroadcast);
        }

        public static void BroadcastRowsUpdate(DbTable dbTable, IReadOnlyList<DbRow> entities)
        {

            var connections = TableSubscribers.GetConnections(dbTable.Name);
            
            if (connections == null)
                return;
            
            var packetToBroadcast = new UpdateRowsContract
            {
                TableName = dbTable.Name,
                Data = entities.ToHubUpdateContract()
            };

            foreach (var connection in connections)
                    connection.SendPacket(packetToBroadcast);
        }

        public static void BroadcastRowsDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
            var connections = TableSubscribers.GetConnections(dbTable.Name);

            if (connections == null)
                return;

            var packetToBroadcast = new DeleteRowsContract
            {
                TableName = dbTable.Name,
                RowsToDelete = dbRows.Select(row => (row.PartitionKey, row.RowKey)).ToList()
            };

            foreach (var connection in connections)
                connection.SendPacket(packetToBroadcast);
        }

        protected override ValueTask HandleIncomingDataAsync(IMyNoSqlTcpContract data)
        {
            switch (data)
            {
                case PingContract _:
                    SendPacket(PongContract.Instance);
                    if (ContextName.Contains("test"))
                        Console.WriteLine("Send pong to the "+ContextName);
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

            
            TableSubscribers.Subscribe(subscribeContract.TableName, this);
            
            var rows = table.GetAllRecords(null);
            
            Console.WriteLine($"Socket {Id} is subscribed to the table {subscribeContract.TableName}. Initialized records: {rows.Count}");

            var initContract = new InitTableContract
            {
                TableName = subscribeContract.TableName,
                Data = rows.ToHubUpdateContract() 
            };

            SendPacket(initContract);
        }
        
    }
}