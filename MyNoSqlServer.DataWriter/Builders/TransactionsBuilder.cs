using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter.Builders
{
    public class TransactionsBuilder<T> where T : IMyNoSqlDbEntity, new()
    {
        private readonly Func<string> _getUrl;
        private readonly string _tableName;
        private string Id { get; }

        private readonly List<object> _transactionObjects = new List<object>();

        public TransactionsBuilder(Func<string> getUrl, string tableName, string id)
        {
            _getUrl = getUrl;
            _tableName = tableName;
            Id = id;
        }
        
        public TransactionsBuilder<T> CleanTable()
        {
            var transactionModel = new CleanTableRequestModel
            {
                Type = "CleanTable",
            };

            _transactionObjects.Add(transactionModel);

            return this;
        }
        
        public TransactionsBuilder<T> CleanPartitions(string[] partitions)
        {
            var transactionModel = new CleanPartitionsRequestModel
            {
                Type = "CleanPartitions",
                Partitions = partitions,
            };

            _transactionObjects.Add(transactionModel);

            return this;
        }


        public TransactionsBuilder<T> DeleteRows(string partitionKey, string[] rowKeys)
        {
            var transactionModel = new DeleteRowsRequestModel
            {
                Type = "DeletePartitions",
                PartitionKey = partitionKey,
                RowKeys = rowKeys
            };

            _transactionObjects.Add(transactionModel);

            return this;
        }
        
        public TransactionsBuilder<T> InsertOrReplace(IEnumerable<T> entities)
        {
            var transactionModel = new InsertOrUpdateRequestModel<T>
            {
                Type = "InsertOrUpdate",
                Entities = entities
            };

            
            _transactionObjects.Add(transactionModel);

            return this;
        }

        public async ValueTask<TransactionsBuilder<T>> PostTransactionSteps()
        {
            await _getUrl()
                .AppendPathSegments("Transaction", "NewSteps")
                .WithTableNameAsQueryParam(_tableName)
                .WithTransactionIdAsQueryParam(Id)
                .PostJsonAsync(_transactionObjects);
            
            _transactionObjects.Clear();

            return this;
        }


        public async ValueTask CommitAsync()
        {
            if (_transactionObjects.Count > 0)
                await PostTransactionSteps();
            
            await _getUrl()
                .AppendPathSegments("Transaction", "Commit")
                .WithTransactionIdAsQueryParam(Id)
                .PostAsync();
        }
    }
    
    
    internal class CleanTableRequestModel
    {
        public string Type { get; set; }
    }
    
    internal class CleanPartitionsRequestModel
    {
        public string Type { get; set; }
        public string[] Partitions { get; set; }
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