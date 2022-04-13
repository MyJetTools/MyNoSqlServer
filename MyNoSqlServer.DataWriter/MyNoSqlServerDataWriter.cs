using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataWriter.Builders;
using MyNoSqlServer.DataWriter.Exceptions;

namespace MyNoSqlServer.DataWriter
{

    public class MyNoSqlServerDataWriter<T> : IMyNoSqlServerDataWriter<T> where T : IMyNoSqlDbEntity, new()
    {

        private const string RowController = "Row";

        internal readonly Func<string> GetUrl;
        private readonly bool _persist;
        private readonly DataSynchronizationPeriod _dataSynchronizationPeriod;
        internal readonly string TableName;

        public MyNoSqlServerDataWriter(Func<string> getUrl, string tableName, bool persist,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            GetUrl = getUrl;
            _persist = persist;
            _dataSynchronizationPeriod = dataSynchronizationPeriod;
            TableName = tableName.ToLower();
            Task.Run(CreateTableIfNotExistsAsync);
        }

        public async Task CreateTableIfNotExistsAsync()
        {
            await MakeCall(async () =>
            {
                await GetUrl()
                    .AppendPathSegments("Tables", "CreateIfNotExists")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPersistTableAsQueryParam(_persist)
                    .PostStringAsync(string.Empty);
            }, "CreateIfNotExists");
        }

        public async ValueTask InsertAsync(T entity)
        {
            await MakeCall(async () =>
            {
                await GetUrl()
                    .AppendPathSegments(RowController, "Insert")
                    .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                    .WithTableNameAsQueryParam(TableName)
                    .PostJsonAsync(entity);
            }, "Insert");
        }

        public async ValueTask InsertOrReplaceAsync(T entity)
        {
            await MakeCall(async () =>
            {
                await GetUrl()
                    .AppendPathSegments(RowController, "InsertOrReplace")
                    .WithTableNameAsQueryParam(TableName)
                    .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                    .PostJsonAsync(entity);
            }, "InsertOrReplace");
        }

        public async ValueTask CleanAndKeepLastRecordsAsync(string partitionKey, int amount)
        {
            await MakeCall(async () =>
            {
                await GetUrl()
                    .AppendPathSegments("CleanAndKeepLastRecords")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .SetQueryParam("amount", amount)
                    .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                    .AllowNonOkCodes()
                    .DeleteAsync();
            }, "CleanAndKeepLastRecords");
        }

        public async ValueTask BulkInsertOrReplaceAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            await MakeCall(async () =>
            {
                await GetUrl()
                    .AppendPathSegments("Bulk", "InsertOrReplace")
                    .WithTableNameAsQueryParam(TableName)
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .PostJsonAsync(entities);
            }, "InsertOrReplace");
        }


        public async ValueTask CleanAndBulkInsertAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            await MakeCall(async () =>
            {
                await GetUrl()
                    .AppendPathSegments("Bulk", "CleanAndBulkInsert")
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .WithTableNameAsQueryParam(TableName)
                    .PostJsonAsync(entities);
            }, "CleanAndBulkInsert");
        }

        public async ValueTask CleanAndBulkInsertAsync(string partitionKey, IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            await MakeCall(async () =>
            {
                await GetUrl()
                    .AppendPathSegments("Bulk", "CleanAndBulkInsert")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .PostJsonAsync(entities);
            }, "CleanAndBulkInsert");
        }


        private async ValueTask<OperationResult> ExecuteUpdateHttpAsync(T entity, string method,
            DataSynchronizationPeriod syncPeriod)
        {
            var response = await GetUrl()
                .AppendPathSegments(RowController, method)
                .WithTableNameAsQueryParam(TableName)
                .AppendDataSyncPeriod(syncPeriod)
                .AllowNonOkCodes()
                .PutJsonAsync(entity);

            return await response.GetOperationResultCodeAsync();
        }


        private async ValueTask<OperationResult> ExecuteUpdateProcessAsync(string partitionKey, string rowKey,
            string method, Func<T, bool> updateCallback,
            DataSynchronizationPeriod syncPeriod)
        {
            while (true)
            {
                var entity = await GetAsync(partitionKey, rowKey);

                if (entity == null)
                    return OperationResult.RecordNotFound;

                if (!updateCallback(entity))
                    return OperationResult.Canceled;

                var result = await ExecuteUpdateHttpAsync(entity, method, syncPeriod);

                if (result == OperationResult.RecordChangedConcurrently)
                    continue;

                return result;
            }
        }

        public ValueTask<OperationResult> ReplaceAsync(string partitionKey, string rowKey,
            Func<T, bool> updateCallback, DataSynchronizationPeriod syncPeriod = DataSynchronizationPeriod.Sec5)
        {
            return ExecuteUpdateProcessAsync(partitionKey, rowKey, "Replace", updateCallback, syncPeriod);
        }

        public ValueTask<OperationResult> MergeAsync(string partitionKey, string rowKey,
            Func<T, bool> updateCallback, DataSynchronizationPeriod syncPeriod = DataSynchronizationPeriod.Sec5)
        {
            return ExecuteUpdateProcessAsync(partitionKey, rowKey, "Merge", updateCallback, syncPeriod);
        }

        public async ValueTask<IEnumerable<T>> GetAsync()
        {
            return await MakeCall<IEnumerable<T>>(async () => await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .GetAsync()
                .ReceiveJson<T[]>(), RowController);
        }

#if NET5_0 || NETSTANDARD2_1 || NETCOREAPP3_1
        private async ValueTask<IReadOnlyList<T>> GetMultiPartDataAsync(string id, int maxRecordsCount)
        {
            var response = await GetUrl()
                .AppendPathSegments("Multipart", "Next")
                .SetQueryParam("requestId", id)
                .SetQueryParam("maxRecordsCount", maxRecordsCount)
                .AllowNonOkCodes()
                .GetAsync();

            if (response.StatusCode == 404)
                return null;

            return await response.GetJsonAsync<List<T>>();

        }
        public async IAsyncEnumerable<T> GetAllAsync(int bulkRecordsCount)
        {
            var firstResponse = await GetUrl()
                 .AppendPathSegments("Multipart", "First")
                 .WithTableNameAsQueryParam(TableName)
                 .GetAsync()
                 .ReceiveJson<StartReadingMultiPartContract>();

            var response = await GetMultiPartDataAsync(firstResponse.SnapshotId, bulkRecordsCount);

            while (response != null)
            {

                foreach (var itm in response)
                    yield return itm;

                response = await GetMultiPartDataAsync(firstResponse.SnapshotId, bulkRecordsCount);
            }
        }
#endif

        public async ValueTask<IEnumerable<T>> GetAsync(string partitionKey)
        {
            return await MakeCall<IEnumerable<T>>(async () => await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .GetAsync()
                .ReceiveJson<T[]>(), RowController);
        }

        public async ValueTask<T> GetAsync(string partitionKey, string rowKey)
        {
            return await MakeCall(async () =>
            {
                var response = await GetUrl()
                    .AppendPathSegments(RowController)
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .WithRowKeyAsQueryParam(rowKey)
                    .AllowNonOkCodes()
                    .GetAsync();

                var statusCode = await response.GetOperationResultCodeAsync();

                if (statusCode == OperationResult.RecordNotFound)
                    return default;

                return await response.GetJsonAsync<T>();
            }, RowController);
        }

        private static readonly T[] EmptyResponse = Array.Empty<T>();

        public async ValueTask<IReadOnlyList<T>> GetMultipleRowKeysAsync(string partitionKey,
            IEnumerable<string> rowKeys)
        {
            return await MakeCall<IReadOnlyList<T>>(async () =>
            {
                var response = await GetUrl()
                    .AppendPathSegments("Rows", "SinglePartitionMultipleRows")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .AllowNonOkCodes()
                    .PostJsonAsync(rowKeys);
                
                var statusCode = await response.GetOperationResultCodeAsync();

                if (statusCode == OperationResult.RecordNotFound)
                    return EmptyResponse;

                return await response.GetJsonAsync<List<T>>();
            }, "SinglePartitionMultipleRows");
        }

        public async ValueTask<T> DeleteAsync(string partitionKey, string rowKey)
        {
            return await MakeCall(async () =>
            {
                var result = await GetAsync(partitionKey, rowKey);

                if (result == null)
                    return default;

                await GetUrl()
                    .AppendPathSegments(RowController)
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .WithRowKeyAsQueryParam(rowKey)
                    .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                    .AllowNonOkCodes()
                    .DeleteAsync();

                return result;
            }, RowController);
        }

        public async ValueTask<IEnumerable<T>> QueryAsync(string query)
        {
            return await MakeCall(async () =>
            {
                var response = await GetUrl()
                    .AppendPathSegments("Query")
                    .WithTableNameAsQueryParam(TableName)
                    .SetQueryParam("query", query)
                    .GetAsync();

                return await response.GetJsonAsync<List<T>>();
            }, "Query");
        }

      public async ValueTask<IEnumerable<T>> GetHighestRowAndBelow(string partitionKey, string rowKeyFrom, int amount)
        {
            return await MakeCall(async () =>
            {
                var response = await GetUrl()
                    .AppendPathSegments("Rows", "HighestRowAndBelow")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .WithRowKeyAsQueryParam(rowKeyFrom)
                    .SetQueryParam("maxAmount", amount)
                    .GetAsync();

                return await response.GetJsonAsync<List<T>>();
            }, "HighestRowAndBelow");
        }

        public ValueTask CleanAndKeepMaxPartitions(int maxAmount)
        {
            return MakeCall(() =>
            {
                var result = GetUrl()
                    .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxPartitions")
                    .WithTableNameAsQueryParam(TableName)
                    .SetQueryParam("maxAmount", maxAmount)
                    .PostStringAsync("");

                return new ValueTask(result);
            }, "CleanAndKeepMaxPartitions");
        }

        public ValueTask CleanAndKeepMaxRecords(string partitionKey, int maxAmount)
        {
            return MakeCall(() =>
            {
                var result = GetUrl()
                    .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxRecords")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .SetQueryParam("maxAmount", maxAmount)
                    .PostStringAsync("");

                return new ValueTask(result);
            }, "CleanAndKeepMaxRecords");
        }

        public async ValueTask<int> GetCountAsync(string partitionKey)
        {
            return await MakeCall(async () =>
            {
                var response = await GetUrl()
                    .AppendPathSegments("/Count")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .GetStringAsync();

                return int.Parse(response);
            }, "Count");
        }

        public BulkDeleteBuilder<T> BulkDelete()
        {
            return new BulkDeleteBuilder<T>(this);
        }


        public async ValueTask<ITransactionsBuilder<T>> BeginTransactionAsync()
        {
            var response = await GetUrl()
                .AppendPathSegments("Transaction", "Start")
                .PostStringAsync("")
                .ReceiveString();

            var jsonModel = Newtonsoft.Json.JsonConvert.DeserializeObject<StartTransactionResponseContract>(response);

            return new TransactionsBuilder<T>(GetUrl, TableName, jsonModel.TransactionId);
        }
        
        private async ValueTask MakeCall(Func<ValueTask> task, string methodName)
        {
            try
            {
                await task();
            }
            catch (FlurlHttpException e)
            {
                Console.WriteLine($"Message: {e.Message}, Response: {e.GetResponseStringAsync()}");
                throw new MyNoSqlHttpException(
                    $"Failed to call {methodName} method. Response: {e.GetResponseStringAsync()}", e);
            }
        }

        private async ValueTask<T> MakeCall<T>(Func<ValueTask<T>> task, string methodName)
        {
            try
            {
                return await task();
            }
            catch (FlurlHttpException e)
            {
                Console.WriteLine($"Message: {e.Message}, Response: {e.GetResponseStringAsync()}");
                throw new MyNoSqlHttpException(
                    $"Failed to call {methodName} method. Response: {e.GetResponseStringAsync()}", e);
            }
        }

    }




}