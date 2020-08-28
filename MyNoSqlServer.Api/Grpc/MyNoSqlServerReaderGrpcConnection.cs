using System.Collections.Generic;
using Grpc.Core;
using MyNoSqlServer.Grpc;
using MyNoSqlServer.Grpc.Contracts;
using ProtoBuf.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    public class MyNoSqlServerReaderGrpcConnection : IMyNoSqlServerReaderGrpcConnection
    {
        private readonly ServerCallContext _serverCallContext;

        public MyNoSqlServerReaderGrpcConnection(ServerCallContext serverCallContext)
        {
            _serverCallContext = serverCallContext;
        }
        
        public async IAsyncEnumerable<ChangeGrpcResponseContract> SubscribeAsync(SubscribeOnChangesGrpcRequest request)
        {

            var ctx = _serverCallContext.GetHttpContext();

            var changesBroadCaster = new GrpcChangesBroadcaster(request.ReaderName, ctx.Connection.RemoteIpAddress.ToString(), request.Tables);

            try
            {
                ServiceLocator.ChangesSubscribers.Subscribe(request.Tables, changesBroadCaster);

                while (changesBroadCaster.Connected)
                    yield return await changesBroadCaster.GetResponseAsync();
            }
            finally
            {
                ServiceLocator.ChangesSubscribers.Unsubscribe(request.Tables, changesBroadCaster);
            }

        }
    }
}