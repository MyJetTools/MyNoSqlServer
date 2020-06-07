using System;

namespace MyNoSqlServer.Abstractions
{
    public interface IMyNoSqlDbEntity
    {
        string PartitionKey { get; set; }
        string RowKey { get; set; }
        string TimeStamp { get; set; }
        DateTime? Expires { get; set; }
    }


    public class MyNoSqlDbEntity : IMyNoSqlDbEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string TimeStamp { get; set; }
        public DateTime? Expires { get; set; }
    }
}