﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataWriter.Builders;

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
            
            await GetUrl()
                .AppendPathSegments("Tables", "CreateIfNotExists")
                .WithTableNameAsQueryParam(TableName)
                .WithPersistTableAsQueryParam(_persist)
                .PostStringAsync(string.Empty);
        }

        public async ValueTask InsertAsync(T entity)
        {
            await GetUrl()
                .AppendPathSegments(RowController, "Insert")
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .WithTableNameAsQueryParam(TableName)
                .PostJsonAsync(entity);
        }

        public async ValueTask InsertOrReplaceAsync(T entity)
        {
            await GetUrl()
                .AppendPathSegments(RowController, "InsertOrReplace")
                .WithTableNameAsQueryParam(TableName)
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .PostJsonAsync(entity);
        }

        public async ValueTask CleanAndKeepLastRecordsAsync(string partitionKey, int amount)
        {
            await GetUrl()
                .AppendPathSegments("CleanAndKeepLastRecords")
                .WithTableNameAsQueryParam(TableName)
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
                await GetUrl()
                    .AppendPathSegments("Bulk", "InsertOrReplace")
                    .WithTableNameAsQueryParam(TableName)
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
                await GetUrl()
                    .AppendPathSegments("Bulk", "CleanAndBulkInsert")
                    .AppendDataSyncPeriod(dataSynchronizationPeriod)
                    .WithTableNameAsQueryParam(TableName)
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
            return await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .GetAsync()
                .ReceiveJson<T[]>();
        }

        public async ValueTask<IEnumerable<T>> GetAsync(string partitionKey)
        {
            return await GetUrl()
                .AppendPathSegments(RowController)
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .GetAsync()
                .ReceiveJson<T[]>();
        }

        public async ValueTask<T> GetAsync(string partitionKey, string rowKey)
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
                .AppendDataSyncPeriod(_dataSynchronizationPeriod)
                .AllowNonOkCodes()
                .DeleteAsync();

            return result;

        }

        public async ValueTask<IEnumerable<T>> QueryAsync(string query)
        {
            var response = await GetUrl()
                .AppendPathSegments("Query")
                .WithTableNameAsQueryParam(TableName)
                .SetQueryParam("query", query)
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
                .GetAsync();

            return await response.GetJsonAsync<List<T>>();
        }

        public ValueTask CleanAndKeepMaxPartitions(int maxAmount)
        {
            var result = GetUrl()
                .AppendPathSegments("GarbageCollector", "CleanAndKeepMaxPartitions")
                .WithTableNameAsQueryParam(TableName)
                .SetQueryParam("maxAmount", maxAmount)
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
                .PostStringAsync("");

            return new ValueTask(result);
        }

        public async ValueTask<int> GetCountAsync(string partitionKey)
        {
            var response = await GetUrl()
                .AppendPathSegments("/Count")
                .WithTableNameAsQueryParam(TableName)
                .WithPartitionKeyAsQueryParam(partitionKey)
                .GetStringAsync();

            return int.Parse(response);
        }

        public BulkDeleteBuilder<T> BulkDelete()
        {
            return new BulkDeleteBuilder<T>(this);
        }
        
    }
    
}