using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter
{
    public class MyNoSqlServerDataWriter<T> : IMyNoSqlServerDataWriter<T> where T : IMyNoSqlDbEntity, new()
    {

        private const string RowController = "Row";
        
        private readonly Func<string> _getUrl;
        private readonly DataSynchronizationPeriod _dataSynchronizationPeriod;
        private readonly string _tableName;

        public MyNoSqlServerDataWriter(Func<string> getUrl, string tableName,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            _getUrl = getUrl;
            _dataSynchronizationPeriod = dataSynchronizationPeriod;
            _tableName = tableName.ToLower();
            Task.Run(CreateTableIfNotExistsAsync);
        }

        private async Task CreateTableIfNotExistsAsync()
        {
            await _getUrl()
                .AppendPathSegments("Tables", "CreateIfNotExists")
                .WithTableNameAsQueryParam(_tableName)
                .PostStringAsync(string.Empty);
        }

        public async ValueTask InsertAsync(T entity)
        {
            await _getUrl()
                .AppendPathSegments(RowController, "Insert")
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .WithTableNameAsQueryParam(_tableName)
                .PostJsonAsync(entity);
        }

        public async ValueTask InsertOrReplaceAsync(T entity)
        {
            await _getUrl()
                .AppendPathSegments(RowController, "InsertOrReplace")
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .WithTableNameAsQueryParam(_tableName)
                .PostJsonAsync(entity);
        }

        public async ValueTask CleanAndKeepLastRecordsAsync(string partitionKey, int amount)
        {
            await _getUrl()
                .AppendPathSegments("CleanAndKeepLastRecords")
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .SetQueryParam("amount", amount)
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .AllowNonOkCodes()
                .DeleteAsync();
        }

        public async ValueTask BulkInsertOrReplaceAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await _getUrl()
                    .AppendPathSegments("Bulk", "InsertOrReplace")
                    .WithTableNameAsQueryParam(_tableName)
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .PostJsonAsync(entities);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public async ValueTask CleanAndBulkInsertAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await _getUrl()
                    .AppendPathSegments("Bulk", "CleanAndBulkInsert")
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .WithTableNameAsQueryParam(_tableName)
                    .PostJsonAsync(entities);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public async ValueTask CleanAndBulkInsertAsync(string partitionKey, IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await _getUrl()
                    .AppendPathSegments("Bulk", "CleanAndBulkInsert")
                    .WithTableNameAsQueryParam(_tableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .PostJsonAsync(entities);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }


        private async ValueTask<OperationResult> ExecuteUpdateHttpAsync(T entity, string method)
        {
            var response = await _getUrl()
                .AppendPathSegments("Rows", method)
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(entity.PartitionKey)
                .WithRowKeyAsQueryParam(entity.RowKey)
                .AllowNonOkCodes()
                .PutJsonAsync(entity);

            return await response.GetOperationResultCodeAsync();
        }


        private async ValueTask<OperationResult> ExecuteUpdateProcessAsync(string partitionKey, string rowKey, 
            string method, Func<T, bool> updateCallback)
        {
            while (true)
            {
                var entity = await GetAsync(partitionKey, rowKey);

                if (entity == null)
                    return OperationResult.RecordNotFound;

                if (!updateCallback(entity))
                    return OperationResult.Canceled;

                var result = await ExecuteUpdateHttpAsync(entity, method);

                if (result == OperationResult.RecordChangedConcurrently)
                    continue;

                return result;
            }
        }

        public ValueTask<OperationResult> ReplaceAsync(string partitionKey, string rowKey,
            Func<T, bool> updateCallback)
        {
            return ExecuteUpdateProcessAsync(partitionKey, rowKey, "replace", updateCallback);
        }

        public ValueTask<OperationResult> MergeAsync(string partitionKey, string rowKey, Func<T, bool> updateCallback)
        {
            return ExecuteUpdateProcessAsync(partitionKey, rowKey, "merge", updateCallback);
        }

        public async ValueTask<IEnumerable<T>> GetAsync()
        {
            return await _getUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(_tableName)
                .GetAsync()
                .ReadAsJsonAsync<T[]>();
        }

        public async ValueTask<IEnumerable<T>> GetAsync(string partitionKey)
        {
            return await _getUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .GetAsync()
                .ReadAsJsonAsync<T[]>();
        }

        public async ValueTask<T> GetAsync(string partitionKey, string rowKey)
        {
            var response = await _getUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .WithRowKeyAsQueryParam(rowKey)
                .AllowNonOkCodes()
                .GetAsync();

            var statusCode = await response.GetOperationResultCodeAsync();

            if (statusCode == OperationResult.RecordNotFound)
                return default;

            return await response.ReadAsJsonAsync<T>();
        }

        private static readonly T[] EmptyResponse = new T[0];

        public async ValueTask<IReadOnlyList<T>> GetMultipleRowKeysAsync(string partitionKey,
            IEnumerable<string> rowKeys)
        {
            var response = await _getUrl()
                .AppendPathSegments("Rows", "SinglePartitionMultipleRows")
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .AllowNonOkCodes()
                .PostJsonAsync(rowKeys);

            
            var statusCode = await response.GetOperationResultCodeAsync();
            
            if (statusCode == OperationResult.RecordNotFound)
                return EmptyResponse;

            return await response.ReadAsJsonAsync<List<T>>();
        }

        public async ValueTask<T> DeleteAsync(string partitionKey, string rowKey)
        {
            var result = await GetAsync(partitionKey, rowKey);

            if (result == null)
                return default;

            await _getUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .WithRowKeyAsQueryParam(rowKey)
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .AllowNonOkCodes()
                .DeleteAsync();

            return result;

        }

        public async ValueTask<IEnumerable<T>> QueryAsync(string query)
        {
            var response = await _getUrl()
                .AppendPathSegments("Query")
                .WithTableNameAsQueryParam(_tableName)
                .SetQueryParam("query", query)
                .GetAsync();

            return await response.ReadAsJsonAsync<List<T>>();

        }

        public async ValueTask<IEnumerable<T>> GetHighestRowAndBelow(string partitionKey, string rowKeyFrom, int amount)
        {
            var response = await _getUrl()
                .AppendPathSegments("Rows", "HighestRowAndBelow")
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .WithRowKeyAsQueryParam(rowKeyFrom)
                .SetQueryParam("maxAmount", amount)
                .GetAsync();

            return await response.ReadAsJsonAsync<List<T>>();
        }

        public ValueTask CleanAndKeepMaxPartitions(int maxAmount)
        {
            var result = _getUrl()
                .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxPartitions")
                .WithTableNameAsQueryParam(_tableName)
                .SetQueryParam("maxAmount", maxAmount)
                .PostStringAsync("");

            return new ValueTask(result);
        }

        public ValueTask CleanAndKeepMaxRecords(string partitionKey, int maxAmount)
        {
            var result = _getUrl()
                .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxRecords")
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .SetQueryParam("maxAmount", maxAmount)
                .PostStringAsync("");

            return new ValueTask(result);
        }

        public async ValueTask<int> GetCountAsync(string partitionKey)
        {
            var response = await _getUrl()
                .AppendPathSegments("/Count")
                .WithTableNameAsQueryParam(_tableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .GetStringAsync();

            return int.Parse(response);
        }
    }
    
}