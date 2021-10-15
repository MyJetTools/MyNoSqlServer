using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNoSqlServer.Abstractions
{
    public interface IMyNoSqlServerDataWriter<T> where T : IMyNoSqlDbEntity, new()
    {
        ValueTask InsertAsync(T entity);
        ValueTask InsertOrReplaceAsync(T entity);

        ValueTask CleanAndKeepLastRecordsAsync(string partitionKey, int amount);
        ValueTask BulkInsertOrReplaceAsync(IEnumerable<T> entity, DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5);
        ValueTask CleanAndBulkInsertAsync(IEnumerable<T> entity, DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5);
        ValueTask CleanAndBulkInsertAsync(string partitionKey, IEnumerable<T> entity, DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5);


        ValueTask<OperationResult> ReplaceAsync(string partitionKey, string rowKey, Func<T, bool> updateCallback, 
            DataSynchronizationPeriod syncPeriod = DataSynchronizationPeriod.Sec5);
        ValueTask<OperationResult> MergeAsync(string partitionKey, string rowKey, Func<T, bool> updateCallback, 
            DataSynchronizationPeriod syncPeriod = DataSynchronizationPeriod.Sec5);
        
        
        ValueTask<IEnumerable<T>> GetAsync();
        
        #if NET5_0 || NETSTANDARD2_1 || NETCOREAPP3_1
        IAsyncEnumerable<T> GetAllAsync(int bulkRecordsCount);
        #endif
        
        ValueTask<IEnumerable<T>> GetAsync(string partitionKey);
        ValueTask<T> GetAsync(string partitionKey, string rowKey);
        
        ValueTask<IReadOnlyList<T>> GetMultipleRowKeysAsync(string partitionKey, IEnumerable<string> rowKeys);
        
        ValueTask<T> DeleteAsync(string partitionKey, string rowKey);

        ValueTask<IEnumerable<T>> QueryAsync(string query);

        ValueTask<IEnumerable<T>> GetHighestRowAndBelow(string partitionKey, string rowKeyFrom, int amount);

        ValueTask CleanAndKeepMaxPartitions( int maxAmount);
        ValueTask CleanAndKeepMaxRecords(string partitionKey, int maxAmount);

        ValueTask<int> GetCountAsync(string partitionKey);

        ValueTask<ITransactionsBuilder<T>> BeginTransactionAsync();
    }


}