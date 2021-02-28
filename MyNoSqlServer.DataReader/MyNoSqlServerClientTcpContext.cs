using System;
using System.Threading.Tasks;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.DataReader
{
    public class MyNoSqlServerClientTcpContext : ClientTcpContext<IMyNoSqlTcpContract>
    {
        private readonly MyNoSqlSubscriber _subscriber;
        private readonly string _appName;

        public MyNoSqlServerClientTcpContext(MyNoSqlSubscriber subscriber, string appName)
        {
            _subscriber = subscriber;
            _appName = appName;
        }

        
        private static readonly Lazy<string> GetReaderVersion = new Lazy<string>(() =>
        {
            try
            {
                return typeof(MyNoSqlServerClientTcpContext).Assembly.GetName().Version.ToString();
            }
            catch (Exception)
            {
                return null;
            }
        });


        protected override ValueTask OnConnectAsync()
        {

            var readerVersion = GetReaderVersion.Value;

            readerVersion = readerVersion == null ? "" : ";ReaderVersion:" + readerVersion;

            var greetingsContract = new GreetingContract
            {
                Name = _appName + readerVersion
            };

            SendDataToSocket(greetingsContract);

            foreach (var tableToSubscribe in _subscriber.GetTablesToSubscribe())
            {
                var subscribePacket = new SubscribeContract
                {
                    TableName = tableToSubscribe
                };

                SendDataToSocket(subscribePacket);

                Console.WriteLine("Subscribed to MyNoSql table: " + tableToSubscribe);
            }

            return new ValueTask();

        }

        protected override ValueTask OnDisconnectAsync()
        {
            return new ValueTask();
        }

        protected override ValueTask HandleIncomingDataAsync(IMyNoSqlTcpContract data)
        {

            try
            {
                switch (data)
                {

                    case InitTableContract initTableContract:
                        _subscriber.HandleInitTableEvent(initTableContract.TableName, initTableContract.Data);
                        break;

                    case InitPartitionContract initPartitionContract:
                        _subscriber.HandleInitPartitionEvent(initPartitionContract.TableName,
                            initPartitionContract.PartitionKey,
                            initPartitionContract.Data);
                        break;

                    case UpdateRowsContract updateRowsContract:
                        _subscriber.HandleUpdateRowEvent(updateRowsContract.TableName, updateRowsContract.Data);
                        break;

                    case DeleteRowsContract deleteRowsContract:
                        _subscriber.HandleDeleteRowEvent(deleteRowsContract.TableName, deleteRowsContract.RowsToDelete);
                        break;

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("There is a problem with Packet: " + data.GetType());
                Console.WriteLine(e);
                throw;
            }

            return new ValueTask();
        }

        protected override IMyNoSqlTcpContract GetPingPacket()
        {
            return PingContract.Instance;
        }
    }
}