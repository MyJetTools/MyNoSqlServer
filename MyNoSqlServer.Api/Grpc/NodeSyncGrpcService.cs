using System.Threading.Tasks;
using MyNoSqlServer.Domains.Nodes;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    public class NodeSyncGrpcService : IMyNoSqlServerNodeSynchronizationGrpcService
    {
        public ValueTask<SyncTransactionGrpcModel> SyncAsync(SyncGrpcRequest request)
        {
            var session = ServiceLocator.NodeSessionsList.GetOrCreate(request.Location);
            return session.ProcessAsync(request.SessionId, request.RequestId);
        }

        public async ValueTask<PayloadWrapperGrpcModel> SyncCompressedAsync(SyncGrpcRequest request)
        {
            var session = ServiceLocator.NodeSessionsList.GetOrCreate(request.Location);
            var result = await session.ProcessAsync(request.SessionId, request.RequestId);
            return result.ToProtobufWrapper(true);
        }
    }
}