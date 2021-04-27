using Newtonsoft.Json;

namespace MyNoSqlServer.DataWriter
{
    internal class StartTransactionResponseContract
    {
        [JsonProperty("transactionId")]
        public string TransactionId { get; set; }
    }
    
    
    internal class StartReadingMultiPartContract
    {
        [JsonProperty("snapshotId")]
        public string SnapshotId { get; set; }
    }
}