using System.Threading.Tasks;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    public class NodeSyncGrpcService : IMyNoSqlServerNodeSynchronizationGrpcService
    {
        public ValueTask<SyncGrpcResponse> SyncAsync(SyncGrpcRequest request)
        {
            var session = ServiceLocator.NodeSessionsList.GetOrCreate(request.Location);

            return session.ProcessAsync(request.SessionId, request.RequestId, request.Compress);
        }
    }
}