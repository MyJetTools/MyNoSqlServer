using System;
using System.Threading.Tasks;
using MyNoSqlClient.ReadRepository;
using MyNoSqlClient.Tcp.Contracts;
using MyTcpSockets;
using IMyNoSqlTcpContract = MyNoSqlClient.Tcp.Contracts.IMyNoSqlTcpContract;
using InitTableContract = MyNoSqlClient.Tcp.Contracts.InitTableContract;
using PingContract = MyNoSqlClient.Tcp.Contracts.PingContract;
using SubscribeContract = MyNoSqlClient.Tcp.Contracts.SubscribeContract;

namespace MyNoSqlClient.Tcp
{
    public class MyNoSqlServerClientTcpContext : ClientTcpContext<IMyNoSqlTcpContract>
    {
        private readonly MyNoSqlSubscriber _subscriber;

        public MyNoSqlServerClientTcpContext(MyNoSqlSubscriber subscriber)
        {
            _subscriber = subscriber;
        }
        
        protected override async ValueTask OnConnectAsync()
        {
            foreach (var tableToSubscribe in _subscriber.GetTablesToSubscribe())
            {
                var subscribePacket = new SubscribeContract
                {
                    TableName = tableToSubscribe
                };

                await SendPacketAsync(subscribePacket);

                Console.WriteLine("Subscribed to MyNoSql table: " + tableToSubscribe);
            }
        }

        protected override ValueTask OnDisconnectAsync()
        {
            return new ValueTask();
        }

        protected override ValueTask HandleIncomingDataAsync(IMyNoSqlTcpContract data)
        {
            switch (data)
            {
                case InitTableContract initTableContract:
                    _subscriber.HandleInitTableEvent(initTableContract.TableName, initTableContract.Data);
                    break;
                
                case InitPartitionContract initPartitionContract:
                    _subscriber.HandleInitPartitionEvent(initPartitionContract.TableName, initPartitionContract.PartitionKey,
                        initPartitionContract.Data);
                    break;
                
                case UpdateRowsContract updateRowsContract:
                    _subscriber.HandleUpdateRowEvent(updateRowsContract.TableName, updateRowsContract.Data);
                    break;
                
                case DeleteRowsContract deleteRowsContract:
                    _subscriber.HandleDeleteRowEvent(deleteRowsContract.TableName, deleteRowsContract.RowsToDelete);
                    break;
                
            }

            return new ValueTask();
        }

        protected override IMyNoSqlTcpContract GetPingPacket()
        {
            return PingContract.Instance;
        }
    }
}