using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MyNoSqlServer.Grpc;
using MyNoSqlServer.Grpc.Contracts;

namespace MyNoSqlServer.DataReader.GrpcClient
{
    public class MyNoSqlServerGrpcClient : MyNoSqlSubscriber
    {
        private readonly Func<IMyNoSqlServerReaderGrpcConnection> _getConnection;
        private readonly string _connectionName;

        public MyNoSqlServerGrpcClient(Func<IMyNoSqlServerReaderGrpcConnection> getConnection, string connectionName)
        {
            _getConnection = getConnection;
            _connectionName = connectionName;
        }

        private async Task TaskLoopAsync()
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                try
                {


                    var connection = _getConnection();

                    await foreach (var response in connection.SubscribeAsync(new SubscribeOnChangesGrpcRequest
                    {
                        ReaderName = _connectionName,
                        Tables = GetTablesToSubscribe().ToArray() 
                    }))
                    {
                        if (response.InitTableData != null)
                            HandleInitTableEvent(response.TableName, response.InitTableData);
                        
                        if (response.InitPartitionData != null)
                            HandleInitPartitionEvent(response.TableName, response.InitPartitionData.PartitionKey, response.InitPartitionData.InitPartitionData);
                        
                        if (response.UpdateRowsData != null)
                            HandleUpdateRowEvent(response.TableName, response.UpdateRowsData);
                        
                        if (response.DeletedRows != null)
                            HandleDeleteRowsEvent(response.TableName,  response.DeletedRows.Select(itm => (itm.PartitionKey, itm.RowKeys)));

                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }


        private Task _theTaskLoop;
        private CancellationTokenSource _cancellationToken;

        public void Start()
        {
            _cancellationToken = new CancellationTokenSource();
            _theTaskLoop = TaskLoopAsync();
        }


        public void Stop()
        {
            _cancellationToken.Cancel();
            _theTaskLoop.Wait();
        }
    }
}