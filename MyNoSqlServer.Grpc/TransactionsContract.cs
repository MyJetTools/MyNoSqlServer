using System.Runtime.Serialization;

namespace MyNoSqlServer.Grpc
{



    [DataContract]
    public class TransactionStepGrpcModel
    {
        [DataMember(Order = 1)]
        public TransactionType TransactionType { get; set; }
        
        [DataMember(Order = 2)]
        public byte[] Payload { get; set; }
        
    }


    [DataContract]
    public class TransactionPayloadGrpcRequest
    {
        [DataMember(Order = 1)]
        public string TransactionId { get; set; }
        [DataMember(Order = 2)]
        public TransactionStepGrpcModel[] Steps { get; set; }
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