using System.Runtime.Serialization;

namespace MyNoSqlServer.Grpc
{

    public enum MyNoSqlResponse
    {
        Ok, TableNotFound, DbRowNotFound
    }

    public enum ContentType
    {
        Json, Protobuf
    }

    [DataContract]
    public class GetEntitiesGrpcRequest
    {
        [DataMember(Order = 1)]
        public string TableName { get; set; }
        
        [DataMember(Order = 2)]
        public string PartitionKey { get; set; }
        
        [DataMember(Order = 3)]
        public string RowKey { get; set; }
        
        [DataMember(Order = 4)]
        public int? Skip { get; set; }

        [DataMember(Order = 5)]
        public int? Limit { get; set; }
    }
    
    
    [DataContract]
    public class GetEntityGrpcRequest
    {
        [DataMember(Order = 1)]
        public string TableName { get; set; }
        
        [DataMember(Order = 2)]
        public string PartitionKey { get; set; }
        
        [DataMember(Order = 3)]
        public string RowKey { get; set; }
    }
    
    [DataContract]
    public class TableEntityTransportGrpcContract
    {
        [DataMember(Order = 1)]
        public ContentType ContentType { get; set; }
        
        [DataMember(Order = 2)]
        public byte[] Content { get; set; }
    }

    [DataContract]
    public class GetDbRowGrpcResponse
    {
        [DataMember(Order = 1)]
        public MyNoSqlResponse Response { get; set; }
        
        [DataMember(Order = 2)]
        public TableEntityTransportGrpcContract Entity { get; set; }
        
    }
}