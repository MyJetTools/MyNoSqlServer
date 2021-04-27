using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter.Builders
{
#if NET5_0 || NETSTANDARD2_1 || NETCOREAPP3_1
    public class TransactionsBuilder<T> : IAsyncDisposable where T : IMyNoSqlDbEntity, new()
    #else
        public class TransactionsBuilder<T> : IDisposable where T : IMyNoSqlDbEntity, new()
#endif
    {
        private readonly Func<string> _getUrl;
        private readonly string _tableName;

        private bool _commited = false;
        private string Id { get; }

        private readonly TransactionDataSerializer<T> _transactionSerializer = new TransactionDataSerializer<T>();

        public TransactionsBuilder(Func<string> getUrl, string tableName, string id)
        {
            _getUrl = getUrl;
            _tableName = tableName;
            Id = id;
        }

        public TransactionsBuilder<T> CleanTable()
        {
            _transactionSerializer.CleanTable();
            return this;
        }

        public TransactionsBuilder<T> DeletePartitions(string[] partitions)
        {
            _transactionSerializer.DeletePartitions(partitions);
            return this;
        }

        public TransactionsBuilder<T> DeletePartition(string partition)
        {
            _transactionSerializer.DeletePartitions(new[] { partition });
            return this;
        }


        public TransactionsBuilder<T> DeleteRows(string partitionKey, string[] rowKeys)
        {
            _transactionSerializer.DeleteRows(partitionKey, rowKeys);
            return this;
        }


        public TransactionsBuilder<T> InsertOrReplace(T entity)
        {
            _transactionSerializer.InsertOrReplace(new[] { entity });
            return this;
        }

        public TransactionsBuilder<T> InsertOrReplace(IEnumerable<T> entities)
        {
            _transactionSerializer.InsertOrReplace(entities);
            return this;
        }

        public async ValueTask<TransactionsBuilder<T>> PostAsync()
        {

            if (_transactionSerializer.Count == 0)
                return this;

            var json = _transactionSerializer.Serialize();

            await _getUrl()
                .AppendPathSegments("Transaction", "Append")
                .WithTableNameAsQueryParam(_tableName)
                .WithTransactionIdAsQueryParam(Id)
                .PostStringAsync(json);

            return this;
        }


        public async ValueTask CommitAsync()
        {
            await PostAsync();

            await _getUrl()
                .AppendPathSegments("Transaction", "Commit")
                .WithTransactionIdAsQueryParam(Id)
                .PostAsync();

            _commited = true;
        }

#if NET5_0 || NETSTANDARD2_1 || NETCOREAPP3_1
        public ValueTask DisposeAsync()
        {
            if (_commited)
                return new ValueTask();
            
            var task = _getUrl()
                .AppendPathSegments("Transaction", "Cancel")
                .WithTransactionIdAsQueryParam(Id)
                .PostAsync();

            return new ValueTask(task);
        }

#else
        public void Dispose()
        {
            if (_commited)
                return;
            
            _getUrl()
                .AppendPathSegments("Transaction", "Cancel")
                .WithTransactionIdAsQueryParam(Id)
                .PostAsync();

        }

#endif


    }




}