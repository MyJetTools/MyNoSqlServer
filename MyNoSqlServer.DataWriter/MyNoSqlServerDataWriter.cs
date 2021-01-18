using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;
using Newtonsoft.Json;

namespace MyNoSqlServer.DataWriter
{

    public class TableDescription
    {
        [JsonProperty("name")]
        public string Name { get; set; }
        
        [JsonProperty("recordsCount")]
        public int RecordsCount { get; set; }
        
        [JsonProperty("partitionsCount")]
        public int PartitionsCount { get; set; }
        
        [JsonProperty("dataSize")]
        public int DataSize { get; set; }
    }
    
    public class MyNoSqlServerDataWriter<T> : IMyNoSqlServerDataWriter<T> where T : IMyNoSqlDbEntity, new()
    {

        private const string RowController = "Row";
        
        internal readonly Func<string> GetUrl;
        internal readonly DataSynchronizationPeriod DefaultDataSynchronizationPeriod;
        internal readonly string TableName;

        private readonly TimeSpan _timeOutPeriod = TimeSpan.FromSeconds(5);

        public MyNoSqlServerDataWriter(Func<string> getUrl, string tableName,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            GetUrl = getUrl;
            DefaultDataSynchronizationPeriod = dataSynchronizationPeriod;
            TableName = tableName.ToLower();
            Task.Run(CreateTableIfNotExistsAsync);
        }

        public async Task CreateTableIfNotExistsAsync()
        {
            await GetUrl()
                .AppendPathSegments("Tables", "CreateIfNotExists")
                .WithTableNameAsQueryParam(TableName)
                .WithTimeout(_timeOutPeriod)
                .PostStringAsync(string.Empty);
        }

        public async ValueTask<IReadOnlyList<TableDescription>> GetListOfTablesAsync()
        {
            return await GetUrl()
                .AppendPathSegments("Tables")
                .WithTableNameAsQueryParam(TableName)
                .WithTimeout(_timeOutPeriod)
                .GetJsonAsync<List<TableDescription>>();
        }


        public async Task DeleteTableIfExistsAsync()
        {
            await GetUrl()
                .AppendPathSegments("Tables")
                .WithTableNameAsQueryParam(TableName)
                .WithTimeout(_timeOutPeriod)
                .DeleteAsync(); 
        }

        public async ValueTask InsertAsync(T entity)
        {
            await GetUrl()
                .AppendPathSegments(RowController, "Insert")
                .AppendDataSyncPeriod(DefaultDataSynchronizationPeriod)
                .WithTableNameAsQueryParam(TableName)
                .WithTimeout(_timeOutPeriod)
                .PostJsonAsync(entity);
        }

        public async ValueTask InsertOrReplaceAsync(T entity)
        {
            await GetUrl()
                .AppendPathSegments(RowController, "InsertOrReplace")
                .WithTableNameAsQueryParam(TableName)
                .AppendDataSyncPeriod(DefaultDataSynchronizationPeriod)
                .WithTimeout(_timeOutPeriod)
                .PostJsonAsync(entity);
        }

        public async ValueTask CleanAndKeepLastRecordsAsync(string partitionKey, int amount)
        {
            await GetUrl()
                .AppendPathSegments("CleanAndKeepLastRecords")
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .SetQueryParam("amount", amount)
                .AppendDataSyncPeriod(DefaultDataSynchronizationPeriod)
                .AllowNonOkCodes()
                .WithTimeout(_timeOutPeriod)
                .DeleteAsync();
        }

        public async ValueTask BulkInsertOrReplaceAsync(IEnumerable<T> entities,
            DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5)
        {
            try
            {
                await GetUrl()
                    .AppendPathSegments("Bulk", "InsertOrReplace")
                    .WithTableNameAsQueryParam(TableName)
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .WithTimeout(_timeOutPeriod)
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
                await GetUrl()
                    .AppendPathSegments("Bulk", "CleanAndBulkInsert")
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .WithTableNameAsQueryParam(TableName)
                    .WithTimeout(_timeOutPeriod)
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
                await GetUrl()
                    .AppendPathSegments("Bulk", "CleanAndBulkInsert")
                    .WithTableNameAsQueryParam(TableName)
                    .WithPartitionKeyAsQueryParam(partitionKey)
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .WithTimeout(_timeOutPeriod)
                    .PostJsonAsync(entities);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }


        private async ValueTask<OperationResult> ExecuteUpdateHttpAsync(T entity, string method, 
            DataSynchronizationPeriod syncPeriod)
        {
            var response = await GetUrl()
                .AppendPathSegments(RowController, method)
                .WithTableNameAsQueryParam(TableName)
                .AppendDataSyncPeriod(syncPeriod)
                .AllowNonOkCodes()
                .WithTimeout(_timeOutPeriod)
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

        public async ValueTask<IReadOnlyList<T>> GetAsync()
        {
            return await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .WithTimeout(_timeOutPeriod)
                .GetAsync()
                .ReadAsJsonAsync<List<T>>();
        }

        public async ValueTask<IReadOnlyList<T>> GetAsync(string partitionKey)
        {
            return await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .GetAsync()
                .ReadAsJsonAsync<List<T>>();
        }
        
        public GetRecordsRequestsBuilder<T> GetRecords()
        {
            return new GetRecordsRequestsBuilder<T>(this);
        }

        public async ValueTask<T> GetAsync(string partitionKey, string rowKey)
        {
            var response = await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .WithRowKeyAsQueryParam(rowKey)
                .AllowNonOkCodes()
                .WithTimeout(_timeOutPeriod)
                .GetAsync();

            if (response.StatusCode ==404)
                return default;

            return await response.GetJsonAsync<T>();
        }

        private static readonly T[] EmptyResponse = new T[0];

        public async ValueTask<IReadOnlyList<T>> GetMultipleRowKeysAsync(string partitionKey,
            IEnumerable<string> rowKeys)
        {
            var response = await GetUrl()
                .AppendPathSegments("Rows", "SinglePartitionMultipleRows")
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .AllowNonOkCodes()
                .WithTimeout(_timeOutPeriod)
                .PostJsonAsync(rowKeys);

            
            var statusCode = await response.GetOperationResultCodeAsync();
            
            if (statusCode == OperationResult.RecordNotFound)
                return EmptyResponse;

            return await response.GetJsonAsync<List<T>>();
        }

        public async ValueTask<T> DeleteAsync(string partitionKey, string rowKey)
        {
            var result = await GetAsync(partitionKey, rowKey);

            if (result == null)
                return default;

            await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .WithRowKeyAsQueryParam(rowKey)
                .AppendDataSyncPeriod(DefaultDataSynchronizationPeriod)
                .AllowNonOkCodes()
                .WithTimeout(_timeOutPeriod)
                .DeleteAsync();

            return result;

        }

        public async ValueTask<IEnumerable<T>> QueryAsync(string query)
        {
            var response = await GetUrl()
                .AppendPathSegments("Query")
                .WithTableNameAsQueryParam(TableName)
                .SetQueryParam("query", query)
                .WithTimeout(_timeOutPeriod)
                .GetAsync();

            return await response.GetJsonAsync<List<T>>();

        }

        public async ValueTask<IEnumerable<T>> GetHighestRowAndBelow(string partitionKey, string rowKeyFrom, int amount)
        {
            var response = await GetUrl()
                .AppendPathSegments("Rows", "HighestRowAndBelow")
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .WithRowKeyAsQueryParam(rowKeyFrom)
                .SetQueryParam("maxAmount", amount)
                .WithTimeout(_timeOutPeriod)
                .GetAsync();

            return await response.GetJsonAsync<List<T>>();
        }

        public ValueTask CleanAndKeepMaxPartitions(int maxAmount)
        {
            var result = GetUrl()
                .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxPartitions")
                .WithTableNameAsQueryParam(TableName)
                .SetQueryParam("maxAmount", maxAmount)
                .WithTimeout(_timeOutPeriod)
                .PostStringAsync("");

            return new ValueTask(result);
        }

        public ValueTask CleanAndKeepMaxRecords(string partitionKey, int maxAmount)
        {
            var result = GetUrl()
                .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxRecords")
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .SetQueryParam("maxAmount", maxAmount)
                .WithTimeout(_timeOutPeriod)
                .PostStringAsync("");

            return new ValueTask(result);
        }

        public async ValueTask<int> GetCountAsync(string partitionKey)
        {
            var response = await GetUrl()
                .AppendPathSegments("/Count")
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .WithTimeout(_timeOutPeriod)
                .GetStringAsync();

            return int.Parse(response);
        }


        public async ValueTask PushExpiredRowsAsync()
        {
            await GetUrl()
                .AppendPathSegments("GarbageCollector", "PushRowsExpirations")
                .WithTimeout(_timeOutPeriod)
                .PostAsync();
        }


        public async ValueTask CleanTableAsync()
        {
            await GetUrl()
                .AppendPathSegments("Tables", "Clean")
                .WithTableNameAsQueryParam(TableName)
                .WithTimeout(_timeOutPeriod)
                .DeleteAsync(); 
        }
    }
    
}