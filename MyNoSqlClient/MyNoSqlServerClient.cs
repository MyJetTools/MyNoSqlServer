using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;

namespace MyNoSqlClient
{
    public class MyNoSqlServerClient<T> : IMyNoSqlServerClient<T> where T: IMyNoSqlTableEntity, new()
    {
        private readonly IMySignalRConnection _myNoSqlConnection;
        private readonly DataSynchronizationPeriod _dataSynchronizationPeriod;
        private readonly string _tableName;

        private const string PartitionKey = "partitionKey";
        private const string RowKey = "rowKey";
        private const string TableName = "tableName";
                
        public MyNoSqlServerClient(IMySignalRConnection myNoSqlConnection, string tableName, 
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            
            _myNoSqlConnection = myNoSqlConnection;
            _dataSynchronizationPeriod = dataSynchronizationPeriod;
            _tableName = tableName.ToLower();
            Task.Run(CreateTableIfNotExistsAsync);
        }

        private async Task CreateTableIfNotExistsAsync()
        {
            await _myNoSqlConnection.Url
                .AppendPathSegments("Tables", "CreateIfNotExists")
                .SetQueryParam(TableName, _tableName)
                .PostStringAsync(string.Empty);
        }

        public async Task InsertAsync(T entity)
        {
            await _myNoSqlConnection.Url
                .AppendPathSegments("Row", "Insert")
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .SetQueryParam(TableName, _tableName)
                .PostJsonAsync(entity);
        }

        public async Task InsertOrReplaceAsync(T entity)
        {
                await _myNoSqlConnection.Url
                    .AppendPathSegments("Row", "InsertOrReplace")
                    .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                    .SetQueryParam(TableName, _tableName)
                    .PostJsonAsync(entity);
        }

        public async Task CleanAndKeepLastRecordsAsync(string partitionKey, int amount)
        {
            await _myNoSqlConnection.Url
                .AppendPathSegments("CleanAndKeepLastRecords")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam("amount", amount)
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .DeleteAsync();
        }

        public async Task BulkInsertOrReplaceAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await _myNoSqlConnection.Url
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

        public async Task CleanAndBulkInsertAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await _myNoSqlConnection.Url
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

        public async Task CleanAndBulkInsertAsync(string partitionKey, IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await _myNoSqlConnection.Url
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

        public async Task<IEnumerable<T>> GetAsync()
        {
            return await _myNoSqlConnection.Url
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .GetAsync()
                .ReadAsJsonAsync<T[]>();
        }

        public async Task<IEnumerable<T>> GetAsync(string partitionKey)
        {
            return await _myNoSqlConnection.Url
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)                
                .GetAsync()
                .ReadAsJsonAsync<T[]>();        }

        public async Task<T> GetAsync(string partitionKey, string rowKey)
        {
            var response = await _myNoSqlConnection.Url
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam(RowKey, rowKey)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .GetAsync();


            if (response.IsRecordNotFound())
                return default(T);

            return await response.ReadAsJsonAsync<T>();        }

        private static readonly T[] EmptyResponse = new T[0];
        
        public async Task<IReadOnlyList<T>> GetMultipleRowKeysAsync(string partitionKey, IEnumerable<string> rowKeys)
        {
            var response = await _myNoSqlConnection.Url
                .AppendPathSegments("Rows","SinglePartitionMultipleRows")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .PostJsonAsync(rowKeys);

            if (response.IsRecordNotFound())
                return EmptyResponse;

            return await response.ReadAsJsonAsync<List<T>>();
        }

        public async Task<T> DeleteAsync(string partitionKey, string rowKey)
        {
            var result = await GetAsync(partitionKey, rowKey);

            if (result == null)
                return default;
            
            await _myNoSqlConnection.Url
                .AppendPathSegments("Row")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam(RowKey, rowKey)
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .AllowHttpStatus(HttpStatusCode.NotFound)
                .DeleteAsync();

            return result;

        }

        public async Task<IEnumerable<T>> QueryAsync(string query)
        {
            var response = await _myNoSqlConnection.Url
                .AppendPathSegments("Query")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam("query", query)
                .GetAsync();

            return await response.ReadAsJsonAsync<List<T>>();

        }

        public async Task<IEnumerable<T>> GetHighestRowAndBelow(string partitionKey, string rowKeyFrom, int amount)
        {
            var response = await _myNoSqlConnection.Url
                .AppendPathSegments("Rows", "HighestRowAndBelow")
                .SetQueryParam(TableName, _tableName)
                .SetQueryParam(PartitionKey, partitionKey)
                .SetQueryParam(RowKey, rowKeyFrom)
                .SetQueryParam("maxAmount", amount)
                .GetAsync();

            return await response.ReadAsJsonAsync<List<T>>();
        }

        public async Task<int> GetCountAsync(string partitionKey)
        {
            var response = await _myNoSqlConnection.Url
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