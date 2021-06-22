using System;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Nodes;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    public class NodeSyncGrpcService : IMyNoSqlServerNodeSynchronizationGrpcService
    {
        public ValueTask<SyncTransactionGrpcModel> SyncAsync(SyncGrpcRequest request)
        {

            try
            {
                var session = ServiceLocator.NodeSessionsList.GetOrCreate(request.Location);
                return session.ProcessAsync(request.SessionId, request.RequestId);
            }
            catch (Exception e)
            {
                ServiceLocator.AppLogs.WriteError(null, "NodeSyncGrpcService.SyncAsync", Newtonsoft.Json.JsonConvert.SerializeObject(request), e);
                throw;
            }

        }

        public async ValueTask<PayloadWrapperGrpcModel> SyncCompressedAsync(SyncGrpcRequest request)
        {

            try
            {
                var session = ServiceLocator.NodeSessionsList.GetOrCreate(request.Location);
                var result = await session.ProcessAsync(request.SessionId, request.RequestId);
                return result.ToProtobufWrapper(true);
            }
            catch (Exception e)
            {
                ServiceLocator.AppLogs.WriteError(null, "NodeSyncGrpcService.SyncAsync", Newtonsoft.Json.JsonConvert.SerializeObject(request), e);
                throw;
            }

        }
    }
}