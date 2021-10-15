using System;
using System.Collections.Generic;
using System.ServiceModel;
using System.Threading.Tasks;

namespace MyNoSqlServer.Grpc
{
   [ServiceContract(Name = "mynosqlserver.Writer")]
    public interface IMyNoSqlWriterGrpcService
    {

        [OperationContract(Action = "CreateTableIfNotExists")] 
        ValueTask CreateTableIfNotExistsAsync(CreateTableIfNotExistsGrpcRequest request);

        [OperationContract(Action = "SetTableAttributes")]
        ValueTask SetTableAttributesAsync(SetTableAttributesGrpcRequest request);
        
        [OperationContract(Action = "GetRows")]
        IAsyncEnumerable<TableEntityTransportGrpcContract> GetRowsAsync(GetEntitiesGrpcRequest request);
        
        [OperationContract(Action = "GetRow")]
        ValueTask<GetDbRowGrpcResponse> GetRowAsync(GetEntityGrpcRequest request);
        
        [OperationContract(Action = "PostTransactionOperations")]
        ValueTask<TransactionGrpcResponse> PostTransactionActionsAsync(TransactionPayloadGrpcRequest request);
        
        [OperationContract(Action = "CancelTransactions")]
        ValueTask CancelTransactionAsync(CancelTransactionGrpcRequest request);
    }
    
    
    [ServiceContract(Name = "MyNoSqlServerGrpc")]
    [Obsolete("Use please IMyNoSqlWriterGrpcService")]
    public interface IMyNoSqlWriterGrpcServiceLegacy
    {

        [OperationContract(Action = "CreateTableIfNotExists")]
        ValueTask CreateTableIfNotExistsAsync(CreateTableIfNotExistsGrpcRequest request);

        [OperationContract(Action = "SetTableAttributes")]
        ValueTask SetTableAttributesAsync(SetTableAttributesGrpcRequest request);
        
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