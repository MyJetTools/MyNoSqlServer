using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataReader
{
    public static class PartitionsUtils
    {
        public static (IReadOnlyList<T> Updated, IReadOnlyList<T> Deleted) GetTotalDifference<T>(
            this SortedDictionary<string, DataReaderPartition<T>> oldCache, 
            SortedDictionary<string, DataReaderPartition<T>> newCache) where T : IMyNoSqlDbEntity
        {

            var deleted = new List<T>();
            var updated = new List<T>();

            foreach (var oldPartition in oldCache)
            {
                if (!newCache.ContainsKey(oldPartition.Key))
                {
                    deleted.AddRange(oldPartition.Value.GetRows());
                    continue;
                }

                oldPartition.Value.FindDifference( newCache[oldPartition.Key], updated.Add, deleted.Add);
            }

            foreach (var newPartition in newCache)
            {
                if (!oldCache.ContainsKey(newPartition.Key))
                {
                    updated.AddRange(newPartition.Value.GetRows());
                }
            }

            return (updated, deleted);

        }
        
        
        public static (List<T> updated, List<T> deleted) FindDifference<T>(
            this DataReaderPartition<T> oldPartition,
            DataReaderPartition<T> newPartition) where T : IMyNoSqlDbEntity
        {
            var updated = new List<T>();
            var deleted = new List<T>();

            oldPartition.FindDifference(newPartition, updated.Add, deleted.Add);
            return (updated, deleted);
        }
        public static void FindDifference<T>(this DataReaderPartition<T> oldPartition, 
            DataReaderPartition<T> newPartition,
            Action<T> updated, Action<T> deleted) where T : IMyNoSqlDbEntity
        {

            foreach (var oldRow in oldPartition.GetRows())
            {
                if (!newPartition.HasRow(oldRow.RowKey))
                {
                    deleted(oldRow);
                    continue;
                }

                var newRow = newPartition.GetRow(oldRow.RowKey);
                if (newRow.TimeStamp != oldRow.TimeStamp)
                    updated(newRow);
            }

            foreach (var newRow in newPartition.GetRows())
            {
                if (!oldPartition.HasRow(newRow.RowKey))
                    updated(newRow);
            }

        }
    }
}