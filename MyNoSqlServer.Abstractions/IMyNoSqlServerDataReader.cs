using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Abstractions
{

    public interface IMyNoSqlServerDataReader<out T> where T : IMyNoSqlDbEntity
    {
        T Get(string partitionKey, string rowKey, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false);

        IReadOnlyList<T> Get(string partitionKey, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false);
        IReadOnlyList<T> Get(string partitionKey, int skip, int take, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false);

        IReadOnlyList<T> Get(string partitionKey, int skip, int take, Func<T, bool> condition, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false);

        IReadOnlyList<T> Get(string partitionKey, Func<T, bool> condition, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false);
        IReadOnlyList<T> Get(Func<T, bool> condition = null, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false);
        
        int Count();
        int Count(string partitionKey);
        int Count(string partitionKey, Func<T, bool> condition);
        IMyNoSqlServerDataReader<T> SubscribeToUpdateEvents(Action<IReadOnlyList<T>> updateSubscriber, Action<IReadOnlyList<T>> deleteSubscriber);
    }

}