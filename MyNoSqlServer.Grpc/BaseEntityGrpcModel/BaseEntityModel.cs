using System.Runtime.Serialization;

namespace MyNoSqlServer.Grpc.Models
{
    [DataContract]
    public class BaseEntityGrpcModel
    {
        [DataMember(Order = 1)]
        public string PartitionKey { get; set; }
        
        [DataMember(Order = 2)]
        public string RowKey { get; set; }
    }
    
}