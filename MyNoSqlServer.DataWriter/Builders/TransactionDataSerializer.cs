using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter.Builders
{
    
    public class TransactionDataSerializer<T>  where T : IMyNoSqlDbEntity, new()
    {

        private readonly List<object> _transactionData = new List<object>();

        public void CleanTable()
        {
            var transactionModel = new CleanTableRequestModel
            {
                Type = "CleanTable",
            };

            _transactionData.Add(transactionModel);
        }
        
        public void DeletePartitions(string[] partitions)
        {
            var transactionModel = new CleanPartitionsRequestModel
            {
                Type = "CleanPartitions",
                PartitionKeys = partitions,
            };

            _transactionData.Add(transactionModel);

        }


        public void DeleteRows(string partitionKey, string[] rowKeys)
        {
            var transactionModel = new DeleteRowsRequestModel
            {
                Type = "DeletePartitions",
                PartitionKey = partitionKey,
                RowKeys = rowKeys
            };

            _transactionData.Add(transactionModel);

        }
        
        public void InsertOrReplace(IEnumerable<T> entities)
        {
            var transactionModel = new InsertOrUpdateRequestModel<T>
            {
                Type = "InsertOrUpdate",
                Entities = entities
            };

            
            _transactionData.Add(transactionModel);


        }


        public string Serialize()
        {
            try
            {
                return Newtonsoft.Json.JsonConvert.SerializeObject(_transactionData);
            }
            finally
            {
                _transactionData.Clear();
            }

        }


        public int Count => _transactionData.Count;

    }
    
    internal class CleanTableRequestModel
    {
        public string Type { get; set; }
    }
    
    internal class CleanPartitionsRequestModel
    {
        public string Type { get; set; }
        public string[] PartitionKeys { get; set; }
    }
    
    internal class DeleteRowsRequestModel
    {
        public string Type { get; set; }
        public string PartitionKey { get; set; }
        public string[] RowKeys { get; set; }

    }
    
    internal class InsertOrUpdateRequestModel<T> where T:IMyNoSqlDbEntity, new()
    {
        public string Type { get; set; }
        public IEnumerable<T> Entities { get; set; }
    }
}