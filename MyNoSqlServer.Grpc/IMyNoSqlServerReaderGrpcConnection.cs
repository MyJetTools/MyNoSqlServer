using System.Collections.Generic;
using System.ServiceModel;
using MyNoSqlServer.Grpc.Contracts;

namespace MyNoSqlServer.Grpc
{
    [ServiceContract(Name = "MyNoSqlServerReader")]
    public interface IMyNoSqlServerReaderGrpcConnection
    {
        [OperationContract(Action = "Subscribe")]
        IAsyncEnumerable<ChangeGrpcResponseContract> SubscribeAsync(SubscribeOnChangesGrpcRequest request);
    }
}