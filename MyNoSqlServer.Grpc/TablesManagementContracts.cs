using System.Runtime.Serialization;

namespace MyNoSqlServer.Grpc
{
    
    
    [DataContract]
    public class CreateTableIfNotExistsGrpcRequest
    {
        
        [DataMember(Order = 1)]
        public string TableName { get; set; }

        [DataMember(Order = 2)]
        public bool PersistTable { get; set; }
        
        [DataMember(Order = 3)]
        public MyNoSqlServerGrpcHeader[] Headers { get; set; }

    }

    [DataContract]
    public class SetTableAttributesGrpcRequest
    {
        [DataMember(Order = 1)]
        public string TableName { get; set; }
        
        [DataMember(Order = 2)]
        public int? MaxPartitionsAmount { get; set; }
        
        [DataMember(Order = 3)]
        public MyNoSqlServerGrpcHeader[] Headers { get; set; }
    }
}