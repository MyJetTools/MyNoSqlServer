using System.Runtime.Serialization;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Grpc
{



    [DataContract]
    public class TransactionActionGrpcModel
    {
        [DataMember(Order = 1)]
        public TransactionType TransactionType { get; set; }
        
        [DataMember(Order = 2)]
        public byte[] Payload { get; set; }


        public static TransactionActionGrpcModel Create(IDbTransactionAction dbTransactionAction)
        {
            var (type, payload) = dbTransactionAction.SerializeTransactionsToGrpc();
            return new TransactionActionGrpcModel
            {
                TransactionType = type,
                Payload = payload
            };
        }
        
    }


    [DataContract]
    public class TransactionPayloadGrpcRequest
    {
        [DataMember(Order = 1)]
        public string TransactionId { get; set; }
        [DataMember(Order = 2)]
        public TransactionActionGrpcModel[] Actions { get; set; }
        [DataMember(Order = 3)]
        public bool Commit { get; set; }
    }
    
    
    
    
    [DataContract]
    public class TransactionGrpcResponse
    {
        [DataMember(Order = 1)]
        public TransactionOperationResult Result { get; set; } 
        
        [DataMember(Order = 2)]
        public string Id { get; set; }
    }

    
    [DataContract]
    public class CancelTransactionGrpcRequest
    {
        [DataMember(Order = 1)]
        public string Id { get; set; }  
    }
    
    
}