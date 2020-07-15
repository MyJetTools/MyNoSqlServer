using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataReader
{
    public interface IMyNoSqlSubscriber
    {
        void Subscribe<T>(string tableName, Action<IReadOnlyList<T>> initAction,
            Action<string, IReadOnlyList<T>> initPartitionAction, Action<IReadOnlyList<T>> updateAction,
            Action<IEnumerable<(string partitionKey, string rowKey)>> deleteActions);
    }
}