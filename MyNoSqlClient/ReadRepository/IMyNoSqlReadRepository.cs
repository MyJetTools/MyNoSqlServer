using System;
using System.Collections.Generic;

namespace MyNoSqlClient.ReadRepository
{
    public interface IMyNoSqlReadRepository<out T> where T : IMyNoSqlTableEntity
    {
        T Get(string partitionKey, string rowKey);
        
        IReadOnlyList<T> Get(string partitionKey);
        IReadOnlyList<T> Get(string partitionKey, int skip, int take);
        
        IReadOnlyList<T> Get(string partitionKey, int skip, int take, Func<T, bool> condition);
        
        IReadOnlyList<T> Get(string partitionKey, Func<T, bool> condition);
        IReadOnlyList<T> Get(Func<T, bool> condition = null);
        int Count();
        int Count(string partitionKey);
        int Count(string partitionKey, Func<T, bool> condition);
        void SubscribeToChanges(Action<IReadOnlyList<T>> changed);

    }

}