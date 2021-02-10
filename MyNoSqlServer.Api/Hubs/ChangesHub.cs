using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using MyNoSqlServer.Api.Controllers;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Api.Hubs
{

    public class ChangesConnection : IConnection, IReaderConnection
    {
        public IClientProxy Client { get; }
        public string Id { get; }

        string IReaderConnection.Id => Id;
        public string Name { get; internal set; } = "SignalR - unknown";

        public string Ip { get; }

        IEnumerable<string> IReaderConnection.Tables => _subscribersAsList;

        public DateTime ConnectedTime { get; } = DateTime.UtcNow;

        public DateTime LastIncomingTime { get; internal set; } = DateTime.UtcNow;


        private readonly Dictionary<string, string> _subscribes = new Dictionary<string, string>();
        private IReadOnlyList<string> _subscribersAsList = Array.Empty<string>();

        public void Subscribe(string tableName)
        {
            lock (_subscribes)
            {
                if (!_subscribes.ContainsKey(tableName))
                    _subscribes.Add(tableName, tableName);

                _subscribersAsList = _subscribes.Keys.ToList();
            }
        }

        public bool SubscribedToTable(string tableChangeSubscribed)
        {
            lock (_subscribes)
                return _subscribes.ContainsKey(tableChangeSubscribed);
        }
        public ChangesConnection(string id, IClientProxy clientProxy, string ip)
        {
            Client = clientProxy;
            Id = id;
            Ip = ip;
        }
        

  
    }

    public class ChangesHub : Hub
    {
        public static readonly ConnectionsManager<ChangesConnection> Connections = new ConnectionsManager<ChangesConnection>();

        public override Task OnConnectedAsync()
        {
            var httpContext = Context.GetHttpContext();
            Connections.Add(Context.ConnectionId, new ChangesConnection(Context.ConnectionId, Clients.Caller, httpContext.GetIp()));
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception exception)
        {
            Connections.Delete(Context.ConnectionId);
            return base.OnDisconnectedAsync(exception);
        }



        public static void BroadcastRowsUpdate(DbTable dbTable, IReadOnlyList<DbRow> entities)
        {
            var clientsToSend = Connections.Get(itm => itm.SubscribedToTable(dbTable.Name)).Select(itm => itm.Client);

            byte[] packetToBroadcast = null;

            foreach (var clientProxy in clientsToSend)
            {
                if (packetToBroadcast == null)
                    packetToBroadcast = entities.ToHubUpdateContract();

                clientProxy.SendAsync(dbTable.Name, "u", packetToBroadcast);
            }

        }

        public static void BroadcastDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
            var clientsToSend = Connections.Get(itm => itm.SubscribedToTable(dbTable.Name)).Select(itm => itm.Client);

            byte[] packetToBroadcast = null;

            foreach (var clientProxy in clientsToSend)
            {
                if (packetToBroadcast == null)
                    packetToBroadcast = dbRows.ToHubDeleteContract();

                clientProxy.SendAsync(dbTable.Name, "d", packetToBroadcast);
            }

        }

        public static void BroadCastInit(DbTable dbTable)
        {
            var clientsToSend = Connections.Get(itm => itm.SubscribedToTable(dbTable.Name)).Select(itm => itm.Client);

            byte[] packetToBroadcast = null;

            foreach (var clientProxy in clientsToSend)
            {
                if (packetToBroadcast == null)
                    packetToBroadcast = dbTable.GetAllRecords(null).ToHubUpdateContract();

                clientProxy.SendAsync(dbTable.Name, "i", packetToBroadcast);
            }
        }
        
        public static void BroadCastInit(DbTable dbTable, DbPartition partition)
        {
            var clientsToSend = Connections.Get(itm => itm.SubscribedToTable(dbTable.Name)).Select(itm => itm.Client);

            byte[] packetToBroadcast = null;

            foreach (var clientProxy in clientsToSend)
            {
                if (packetToBroadcast == null)
                    packetToBroadcast = partition.GetAllRows().ToHubUpdateContract();

                clientProxy.SendAsync(dbTable.Name, "i:"+partition.PartitionKey, packetToBroadcast);
            }
        }

        public async Task Subscribe(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return;

            var table = ServiceLocator.DbInstance.TryGetTable(tableName);

            if (table == null)
                return;

            Connections.Update(Context.ConnectionId, itm => { itm.Subscribe(tableName); });

            var rows = table.GetAllRecords(null);

            var dataToSend = rows.ToHubUpdateContract();

            await Clients.Caller.SendAsync(tableName, "i", dataToSend);
        }


        public Task GetRow(string tableName, string corrId, string partitionKey, string rowKey)
        {
            var dbTable = ServiceLocator.DbInstance.TryGetTable(tableName);
            if (dbTable == null)
                return Clients.Caller.SendTableNotFoundAsync(corrId);

            var partition = dbTable.GetPartition(partitionKey);

            if (partition == null)
                return Clients.Caller.SendRowNotFoundAsync(corrId);

            var row = partition.GetRow(rowKey);
            
            if (row == null)
                return Clients.Caller.SendRowNotFoundAsync(corrId);


            return Clients.Caller.SendRowAsync(corrId, row);

        }
        
        public Task GetRowsByPartition(string tableName, string corrId, string partitionKey, int limit, int skip)
        {
            var dbTable = ServiceLocator.DbInstance.TryGetTable(tableName);
            if (dbTable == null)
                return Clients.Caller.SendTableNotFoundAsync(corrId);

            var partition = dbTable.GetPartition(partitionKey);

            if (partition == null)
                return Clients.Caller.SendEmptyRowsAsync(corrId);

            var rows = partition.GetRowsWithLimit(limit.ContractToLimit(), skip.ContractToSkip());

            return Clients.Caller.SendRowsAsync(corrId, rows.ToJsonArray().AsArray());
        }
        
        public Task GetRows(string tableName, string corrId, int limit, int skip)
        {
            var dbTable = ServiceLocator.DbInstance.TryGetTable(tableName);
            if (dbTable == null)
                return Clients.Caller.SendTableNotFoundAsync(corrId);


            var rows = dbTable.GetAllRecords(limit.ContractToLimit());

            return Clients.Caller.SendRowsAsync(corrId, rows.ToJsonArray().AsArray());
        }

        public ValueTask Greeting(string appName)
        {
            if (Connections.TryGet(Context.ConnectionId, out var connection))
            {
                connection.Name = "SignalR - "+appName;
            }

            return new ValueTask();
        }

        public async Task Ping()
        {
            if (Connections.TryGet(Context.ConnectionId, out var connection))
            {
                connection.LastIncomingTime = DateTime.UtcNow;
            }
            
            await Clients.Caller.SendAsync("system", "heartbeat");
        }

    }


}