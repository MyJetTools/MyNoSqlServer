using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNoSqlServer.Abstractions
{
#if NET5_0 || NETSTANDARD2_1 || NETCOREAPP3_1
    public interface ITransactionsBuilder<T> : IAsyncDisposable where T : IMyNoSqlDbEntity, new()
#else
    public interface ITransactionsBuilder<T> : IDisposable where T : IMyNoSqlDbEntity, new()
#endif
    {
        ITransactionsBuilder<T> CleanTable();
        ITransactionsBuilder<T> DeletePartitions(string[] partitions);
        ITransactionsBuilder<T> DeletePartition(string partition);
        ITransactionsBuilder<T> DeleteRows(string partitionKey, string[] rowKeys);
        ITransactionsBuilder<T> InsertOrReplace(T entity);
        ITransactionsBuilder<T> InsertOrReplace(IEnumerable<T> entities);
        ValueTask<ITransactionsBuilder<T>> PostAsync();
        ValueTask CommitAsync();
    }
}