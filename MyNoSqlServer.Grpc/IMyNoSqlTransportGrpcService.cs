using System.Collections.Generic;
using System.ServiceModel;
using System.Threading.Tasks;

namespace MyNoSqlServer.Grpc
{
    [ServiceContract(Name = "MyNoSqlServerGrpc")]
    public interface IMyNoSqlTransportGrpcService
    {
        [OperationContract(Action = "GetRows")]
        IAsyncEnumerable<TableEntityTransportGrpcContract> GetRowsAsync(GetEntitiesGrpcRequest request);
        
        [OperationContract(Action = "GetRow")]
        ValueTask<GetDbRowGrpcResponse> GetRowAsync(GetEntityGrpcRequest request);
        
        [OperationContract(Action = "PostTransactionOperations")]
        ValueTask<TransactionGrpcResponse> PostTransactionActionsAsync(TransactionPayloadGrpcRequest request);
        
        [OperationContract(Action = "CancelTransactions")]
        ValueTask CancelTransactionAsync(CancelTransactionGrpcRequest request);
    }
}