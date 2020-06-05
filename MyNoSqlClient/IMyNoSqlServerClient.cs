using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNoSqlClient
{
    public interface IMyNoSqlTableEntity
    {
        string PartitionKey { get; set; }
        string RowKey { get; set; }
        string Timestamp { get; set; }
    }

    public class MyNoSqlTableEntity : IMyNoSqlTableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Timestamp { get; set; }
    }

    public interface IMyNoSqlServerClient<T> where T : IMyNoSqlTableEntity, new()
    {
        Task InsertAsync(T entity);
        Task InsertOrReplaceAsync(T entity);

        Task CleanAndKeepLastRecordsAsync(string partitionKey, int amount);
        Task BulkInsertOrReplaceAsync(IEnumerable<T> entity, DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5);
        Task CleanAndBulkInsertAsync(IEnumerable<T> entity, DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5);
        Task CleanAndBulkInsertAsync(string partitionKey, IEnumerable<T> entity, DataSynchronizationPeriod dataSynchronizationPeriod = DataSynchronizationPeriod.Sec5);
        
        Task<IEnumerable<T>> GetAsync();
        Task<IEnumerable<T>> GetAsync(string partitionKey);
        Task<T> GetAsync(string partitionKey, string rowKey);
        
        Task<IReadOnlyList<T>> GetMultipleRowKeysAsync(string partitionKey, IEnumerable<string> rowKeys);
        
        Task<T> DeleteAsync(string partitionKey, string rowKey);


        Task<IEnumerable<T>> QueryAsync(string query);

        Task<IEnumerable<T>> GetHighestRowAndBelow(string partitionKey, string rowKeyFrom, int amount);


        Task<int> GetCountAsync(string partitionKey);
    }

}