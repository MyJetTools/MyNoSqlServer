using System;
using System.Linq;
using System.Threading.Tasks;
using AsyncAwaitUtils;
using MyNoSqlServer.Domains.DataReadersBroadcast;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.TcpContracts;

namespace MyNoSqlServer.Api.DataReadersTcpServer
{
    public class DataReadersTcpBroadcaster : IDataReadersBroadcaster
    {

        private readonly DataReaderSubscribers _subscribers;

        private readonly AsyncQueue<ITransactionEvent> _asyncQueue = new();

        public DataReadersTcpBroadcaster(DataReaderSubscribers subscribers)
        {
            _subscribers = subscribers;
        }
        
        public void BroadcastEvent(ITransactionEvent @event)
        {
            _asyncQueue.Put(@event);
        }

        private async Task ThreadLoopAsync()
        {

            while (_working)
            {
                try
                {
                    var @event = await _asyncQueue.GetAsync();

                    switch (@event)
                    {
                        case InitTableTransactionEvent initTableTransactionEvent:
                            BroadcastInitTable(initTableTransactionEvent);
                            break;
                        case InitPartitionsTransactionEvent initPartitionsTransactionEvent:
                            BroadcastInitPartitions(initPartitionsTransactionEvent);
                            break;
                        case UpdateRowsTransactionEvent updateRowsTransactionEvent:
                            BroadcastRowsUpdate(updateRowsTransactionEvent);
                            break;
                        case DeleteRowsTransactionEvent deleteRowsTransactionEvent:
                            BroadcastRowsDelete(deleteRowsTransactionEvent);
                            break;
                    }
                }
      
                catch (Exception e)
                {
                    if (e is ObjectDisposedException disposedException)
                    {
                        Console.WriteLine("DataReaderBroadcaster is disposed. MSG:"+disposedException.Message);
                        break; 
                    }
                    
                    Console.WriteLine("DataReaderBroadcaster: "+e.Message);
                }
            }
        }
        
        private void BroadcastInitTable(InitTableTransactionEvent initTableTransactionEvent)
        {
            var subscribers = _subscribers.GetSubscribers(initTableTransactionEvent.TableName);
            
            if (subscribers == null)
                return;
            
            var packetToBroadcast = new InitTableContract
            {
                TableName = initTableTransactionEvent.TableName,
                Data = initTableTransactionEvent.Snapshot.AsByteArray()
            };

            foreach (var subscriber in subscribers)
                subscriber.SendDataToSocket(packetToBroadcast);
        }

        
        private void BroadcastInitPartitions(InitPartitionsTransactionEvent initPartitionsTransactionEvent)
        {
            var connections = _subscribers.GetSubscribers(initPartitionsTransactionEvent.TableName);

            if (connections == null)
                return;

            foreach (var (partitionKey, rows) in initPartitionsTransactionEvent.Partitions)
            {
                var packetToBroadcast = new InitPartitionContract
                {
                    TableName = initPartitionsTransactionEvent.TableName,
                    PartitionKey = partitionKey,
                    Data = rows.ToJsonArray().AsArray()
                };

                foreach (var connection in connections)
                    connection.SendDataToSocket(packetToBroadcast); 
            }
        }

        private void BroadcastRowsUpdate(UpdateRowsTransactionEvent updateRowsTransactionEvent)
        {

            var connections = _subscribers.GetSubscribers(updateRowsTransactionEvent.TableName);

            if (connections == null)
                return;

            var packetToBroadcast = new UpdateRowsContract
            {
                TableName = updateRowsTransactionEvent.TableName,
                Data = updateRowsTransactionEvent.Rows.ToJsonArray().AsArray()
            };

            foreach (var connection in connections)
                connection.SendDataToSocket(packetToBroadcast);
        }
        
        private void BroadcastRowsDelete(DeleteRowsTransactionEvent deleteRowsTransactionEvent)
        {
            var connections = _subscribers.GetSubscribers(deleteRowsTransactionEvent.TableName);

            if (connections == null)
                return;

            var packetToBroadcast = new DeleteRowsContract
            {
                TableName = deleteRowsTransactionEvent.TableName,
                RowsToDelete = deleteRowsTransactionEvent.Rows.Select(row => (row.PartitionKey, row.RowKey)).ToList()
            };

            foreach (var connection in connections)
                connection.SendDataToSocket(packetToBroadcast);
        }


        public void Subscribe(DataReaderTcpService tcpService, DbTable table)
        {

            _subscribers.Subscribe(table.Name, tcpService);

            var rowCounts = 0;
            
            table.GetReadAccess(readAccess =>
            {
                var rows = readAccess.GetAllRows();

                rowCounts = rows.Count;
                
                var initContract = new InitTableContract
                {
                    TableName = table.Name,
                    Data = rows.ToJsonArray().AsArray()
                };
                tcpService.SendDataToSocket(initContract);
            });
            
            Console.WriteLine($"Socket {tcpService.Id} is subscribed to the table {table.Name}. Initialized records: {rowCounts}");
        }
        
        public void Unsubscribe(DataReaderTcpService tcpService, string tableName)
        {
            _subscribers.Unsubscribe(tableName, tcpService);
        }

        private bool _working;

        private Task _threadLoop;

 
        public void Start()
        {
            _working = true;
            _threadLoop = ThreadLoopAsync();
        }


        public void Stop()
        {
            _working = false;
            _asyncQueue.Dispose();
            _threadLoop.Wait();
        }


    }
    
}