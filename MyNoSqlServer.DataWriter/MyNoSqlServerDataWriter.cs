using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter
{
    public class MyNoSqlServerDataWriter<T> : IMyNoSqlServerDataWriter<T> where T : IMyNoSqlEntity, new()
    {
        private readonly Func<string> _getUrl;
        private readonly DataSynchronizationPeriod _dataSynchronizationPeriod;
        private readonly string _tableName;

        private const string PartitionKey = "partitionKey";
        private const string RowKey = "rowKey";
        private const string TableName = "tableName";

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
                .SetQueryParam(TableName, _tableName)
                .PostStringAsync(string.Empty);
        }

        public async ValueTask InsertAsync(T entity)
        {
            await _getUrl()
                .AppendPathSegments("Row", "Insert")
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .SetQueryParam(TableName, _tableName)
                .PostJsonAsync(entity);
        }

        public async ValueTask InsertOrReplaceAsync(T entity)
        {
            await _getUrl()
                .AppendPathSegments("Row", "InsertOrReplace")
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .SetQueryParam(TableName, _tableName)
                .PostJsonAsync(entity);
        }

        public async ValueTask CleanAndKeepLastRecordsAsync(string partitionKey, int amount)
        {
            await _getUrl()
                .AppendPathSegments("CleanAndKeepLastRecords")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam("amount", amount)
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .DeleteAsync();
        }

        public async ValueTask BulkInsertOrReplaceAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await _getUrl()
                    .AppendPathSegments("Bulk", "InsertOrReplace")
                    .SetQueryParam(TableName, _tableName)
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
                    .SetQueryParam(TableName, _tableName)
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
                    .SetQueryParam(TableName, _tableName)
                    .SetQueryParam(PartitionKey, partitionKey)
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .PostJsonAsync(entities);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public async ValueTask<IEnumerable<T>> GetAsync()
        {
            return await _getUrl()
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .GetAsync()
                .ReadAsJsonAsync<T[]>();
        }

        public async ValueTask<IEnumerable<T>> GetAsync(string partitionKey)
        {
            return await _getUrl()
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .GetAsync()
                .ReadAsJsonAsync<T[]>();
        }

        public async ValueTask<T> GetAsync(string partitionKey, string rowKey)
        {
            var response = await _getUrl()
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam(RowKey, rowKey)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .GetAsync();


            if (response.IsRecordNotFound())
                return default(T);

            return await response.ReadAsJsonAsync<T>();
        }

        private static readonly T[] EmptyResponse = new T[0];

        public async ValueTask<IReadOnlyList<T>> GetMultipleRowKeysAsync(string partitionKey,
            IEnumerable<string> rowKeys)
        {
            var response = await _getUrl()
                .AppendPathSegments("Rows", "SinglePartitionMultipleRows")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .PostJsonAsync(rowKeys);

            if (response.IsRecordNotFound())
                return EmptyResponse;

            return await response.ReadAsJsonAsync<List<T>>();
        }

        public async ValueTask<T> DeleteAsync(string partitionKey, string rowKey)
        {
            var result = await GetAsync(partitionKey, rowKey);

            if (result == null)
                return default;

            await _getUrl()
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam(RowKey, rowKey)
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .DeleteAsync();

            return result;

        }

        public async ValueTask<IEnumerable<T>> QueryAsync(string query)
        {
            var response = await _getUrl()
                .AppendPathSegments("Query")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam("query", query)
                .GetAsync();

            return await response.ReadAsJsonAsync<List<T>>();

        }

        public async ValueTask<IEnumerable<T>> GetHighestRowAndBelow(string partitionKey, string rowKeyFrom, int amount)
        {
            var response = await _getUrl()
                .AppendPathSegments("Rows", "HighestRowAndBelow")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam(RowKey, rowKeyFrom)
                .SetQueryParam("maxAmount", amount)
                .GetAsync();

            return await response.ReadAsJsonAsync<List<T>>();
        }

        public ValueTask CleanAndKeepMaxPartitions(int maxAmount)
        {
            var result = _getUrl()
                .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxPartitions")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam("maxAmount", maxAmount)
                .PostStringAsync("");

            return new ValueTask(result);
        }

        public ValueTask CleanAndKeepMaxRecords(string partitionKey, int maxAmount)
        {
            var result = _getUrl()
                .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxRecords")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam("maxAmount", maxAmount)
                .PostStringAsync("");

            return new ValueTask(result);
        }

        public async ValueTask<int> GetCountAsync(string partitionKey)
        {
            var response = await _getUrl()
                .AppendPathSegments("/Count")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .GetStringAsync();

            return int.Parse(response);
        }
    }

    public static class MyNoSqlServerClientExt
    {

        public static async Task<T> ReadAsJsonAsync<T>(this Task<HttpResponseMessage> responseTask)
        {
            var response = await responseTask;
            return await response.ReadAsJsonAsync<T>();
        }

        public static async Task<T> ReadAsJsonAsync<T>(this HttpResponseMessage response)
        {
            var json = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        internal static bool IsRecordNotFound(this HttpResponseMessage httpResponseMessage)
        {
            return
                httpResponseMessage.StatusCode == HttpStatusCode.NotFound ||
                httpResponseMessage.StatusCode == HttpStatusCode.NoContent;
        }

    }
    
}