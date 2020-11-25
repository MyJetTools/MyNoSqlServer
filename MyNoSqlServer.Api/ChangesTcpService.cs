using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Domains.Db.Operations;
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

            
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                foreach (var dbPartition in dbTableReader.Partitions)
                {
                                
                    var packetToBroadcast = new InitPartitionContract
                    {
                        TableName = dbTable.Name,
                        Data = dbTable.GetRows().ToHubUpdateContract(),
                        PartitionKey = dbPartition.Key
                    };

                    foreach (var connection in connections)
                        connection.SendPacketAsync(packetToBroadcast);
                }
            });
            

        }

        public static void BroadcastInitPartition(DbTable dbTable, string partitionKey)
        {

            var connections = TableSubscribers.GetConnections(dbTable.Name);

            if (connections == null)
                return;

            var initTablePacket = new InitTableContract
            {
                TableName = dbTable.Name,
                Data = Array.Empty<DbRow>().ToHubUpdateContract() 
            };
            
            foreach (var connection in connections)
                connection.SendPacketAsync(initTablePacket);
            

            var packetToBroadcast = new InitPartitionContract
            {
                TableName = dbTable.Name,
                PartitionKey = partitionKey,
                Data = dbTable.GetRows(partitionKey).ToHubUpdateContract()
            };

            foreach (var connection in connections)
                connection.SendPacketAsync(packetToBroadcast);
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
                    connection.SendPacketAsync(packetToBroadcast);
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
                connection.SendPacketAsync(packetToBroadcast);
        }

        protected override ValueTask HandleIncomingDataAsync(IMyNoSqlTcpContract data)
        {
            switch (data)
            {
                case PingContract _:
                    SendPacketAsync(PongContract.Instance);
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
                
                
                case UpdateExpiresTimeTcpContract expiresTimeTcpContract:
                    var dbTable = ServiceLocator.DbInstance.TryGetTable(expiresTimeTcpContract.TableName);
                    if (dbTable != null)
                        ServiceLocator.DbTableWriteOperations.UpdateExpirationTime(dbTable, 
                            expiresTimeTcpContract.PartitionKey, 
                            expiresTimeTcpContract.RowKeys, 
                            expiresTimeTcpContract.Expires);
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
            
            var rows = table.GetRows();
            
            Console.WriteLine($"Socket {Id} is subscribed to the table {subscribeContract.TableName}. Initialized records: {rows.Count}");

            var initContract = new InitTableContract
            {
                TableName = subscribeContract.TableName,
                Data = rows.ToHubUpdateContract() 
            };

            SendPacketAsync(initContract);
        }
        
    }
}