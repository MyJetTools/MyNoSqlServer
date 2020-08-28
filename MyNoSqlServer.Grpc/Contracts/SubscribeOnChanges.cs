using System.Runtime.Serialization;

namespace MyNoSqlServer.Grpc.Contracts
{
    [DataContract]
    public class SubscribeOnChangesGrpcRequest
    {
    
        [DataMember(Order = 1)]
        public string ReaderName { get; set; }
        
        [DataMember(Order = 2)]
        public string[] Tables { get; set; }
        
    }

    [DataContract]
    public class DeleteDbRowContract
    {
        [DataMember(Order = 1)]
        public string PartitionKey { get; set; }
        [DataMember(Order = 2)]
        public string[] RowKeys { get; set; }
    }

    [DataContract]
    public class InitPartitionGrpcContract
    {
        [DataMember(Order = 1)] 
        public string PartitionKey { get; set; }
        
        [DataMember(Order = 2)] 
        public byte[] InitPartitionData { get; set; }
    }
    

    [DataContract]
    public class ChangeGrpcResponseContract
    {
        
        [DataMember(Order = 1)] 
        public string TableName { get; set; }
        
        [DataMember(Order = 2)] 
        public byte[] InitTableData { get; set; }
        
        [DataMember(Order = 3)] 
        public InitPartitionGrpcContract InitPartitionData { get; set; }
        
        [DataMember(Order = 4)] 
        public byte[] UpdateRowsData { get; set; }
        
        [DataMember(Order = 5)] 
        public DeleteDbRowContract[] DeletedRows { get; set; }
    }
    
}