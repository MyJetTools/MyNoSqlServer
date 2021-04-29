using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter.Builders
{
    public class TransactionsBuilder<T> : ITransactionsBuilder<T> where T : IMyNoSqlDbEntity, new()
    {
#if NET5_0
        private static readonly System.Diagnostics.ActivitySource _source = new("MyNoSql.TransactionsBuilder");
#endif

        private readonly Func<string> _getUrl;
        private readonly string _tableName;

        private bool _committed = false;
        private string Id { get; }

        private readonly TransactionDataSerializer<T> _transactionSerializer = new TransactionDataSerializer<T>();

        public TransactionsBuilder(Func<string> getUrl, string tableName, string id)
        {
            _getUrl = getUrl;
            _tableName = tableName;
            Id = id;
        }

        public ITransactionsBuilder<T> CleanTable()
        {
            _transactionSerializer.CleanTable();
            return this;
        }

        public ITransactionsBuilder<T> DeletePartitions(string[] partitions)
        {
            _transactionSerializer.DeletePartitions(partitions);
            return this;
        }

        public ITransactionsBuilder<T> DeletePartition(string partition)
        {
            _transactionSerializer.DeletePartitions(new[] { partition });
            return this;
        }


        public ITransactionsBuilder<T> DeleteRows(string partitionKey, string[] rowKeys)
        {
            _transactionSerializer.DeleteRows(partitionKey, rowKeys);
            return this;
        }


        public ITransactionsBuilder<T> InsertOrReplace(T entity)
        {
            _transactionSerializer.InsertOrReplace(new[] { entity });
            return this;
        }

        public ITransactionsBuilder<T> InsertOrReplace(IEnumerable<T> entities)
        {
            _transactionSerializer.InsertOrReplace(entities);
            return this;
        }

        public async ValueTask<ITransactionsBuilder<T>> PostAsync()
        {

            if (_transactionSerializer.Count == 0)
                return this;

            var json = SerializeTransaction();

#if NET5_0
            using var actPost = _source.StartActivity("PostAsync.ExecuteHttpCall");
#endif
            
            await _getUrl()
                .AppendPathSegments("Transaction", "Append")
                .WithTableNameAsQueryParam(_tableName)
                .WithTransactionIdAsQueryParam(Id)
                .PostStringAsync(json);

            return this;
        }

        private string SerializeTransaction()
        {
#if NET5_0
            using var act = _source.StartActivity("PostAsync.Serialize");
#endif

            var json = _transactionSerializer.Serialize();
            return json;
        }


        public async ValueTask CommitAsync()
        {
#if NET5_0
            using var act = _source.StartActivity("CommitAsync");
#endif

            await PostAsync();

#if NET5_0
            using var actCommit = _source.StartActivity("CommitAsync.ExecuteHttpCall");
#endif

            await _getUrl()
                .AppendPathSegments("Transaction", "Commit")
                .WithTransactionIdAsQueryParam(Id)
                .PostAsync();

            _committed = true;
        }

#if NET5_0 || NETSTANDARD2_1 || NETCOREAPP3_1
        public ValueTask DisposeAsync()
        {
            if (_committed)
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
            if (_committed)
                return;
            
            _getUrl()
                .AppendPathSegments("Transaction", "Cancel")
                .WithTransactionIdAsQueryParam(Id)
                .PostAsync();

        }

#endif


    }




}