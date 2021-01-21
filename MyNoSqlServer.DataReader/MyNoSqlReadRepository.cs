using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataReader
{

    public class MyNoSqlReadRepository<T> : IMyNoSqlServerDataReader<T> where T : IMyNoSqlDbEntity
    {

        private SortedDictionary<string, DataReaderPartition<T>> _cache =
            new SortedDictionary<string, DataReaderPartition<T>>();

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public MyNoSqlReadRepository(IMyNoSqlSubscriber subscriber, string tableName)
        {
            tableName = tableName.ToLower();
            subscriber.Subscribe<T>(tableName, Init, InitPartition, Update, Delete);
        }


        private void Init(IReadOnlyList<T> items)
        {

            _lock.EnterWriteLock();
            try
            {
                var oldOne = _cache;
                _cache = new SortedDictionary<string, DataReaderPartition<T>>();

                foreach (var item in items)
                {
                    if (!_cache.ContainsKey(item.PartitionKey))
                        _cache.Add(item.PartitionKey, new DataReaderPartition<T>());

                    var partition = _cache[item.PartitionKey];

                    partition.Update(item);
                }

                var (updated, deleted) = oldOne.GetTotalDifference(_cache);

                NotifyChanged(updated);

                NotifyDeleted(deleted);

            }
            finally
            {
                _lock.ExitWriteLock();
            }


        }

        private void InitPartition(string partitionKey, IReadOnlyList<T> items)
        {

            _lock.EnterWriteLock();
            try
            {

                var oldPartition = _cache.ContainsKey(partitionKey)
                    ? _cache[partitionKey]
                    : null;

                _cache[partitionKey] = new DataReaderPartition<T>();

                foreach (var item in items)
                {
                    if (!_cache.ContainsKey(item.PartitionKey))
                        _cache.Add(item.PartitionKey, new DataReaderPartition<T>());

                    var partition = _cache[item.PartitionKey];

                    partition.Update(item);
                }

                if (oldPartition == null)
                {
                    NotifyChanged(_cache[partitionKey].GetRows().ToList());
                    return;
                }

                var (updated, deleted) = oldPartition.FindDifference(_cache[partitionKey]);

                NotifyChanged(updated);

                NotifyDeleted(deleted);

            }
            finally
            {
                _lock.ExitWriteLock();
            }


        }

        private void Update(IReadOnlyList<T> items)
        {
            _lock.EnterWriteLock();
            try
            {
                foreach (var item in items)
                {
                    if (!_cache.ContainsKey(item.PartitionKey))
                        _cache.Add(item.PartitionKey, new DataReaderPartition<T>());

                    var partition = _cache[item.PartitionKey];

                    partition.Update(item);
                }

                NotifyChanged(items);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                _lock.ExitWriteLock();
            }

        }


        private void Delete(IEnumerable<(string partitionKey, string rowKey)> dataToDelete)
        {
            _lock.EnterWriteLock();
            try
            {
                List<T> deleted = null;
                foreach (var (partitionKey, rowKey) in dataToDelete)
                {
                    if (!_cache.ContainsKey(partitionKey))
                        continue;

                    var partition = _cache[partitionKey];


                    if (partition.TryDelete(rowKey, out var deletedItem))
                    {
                        deleted ??= new List<T>();
                        deleted.Add(deletedItem);
                    }

                    if (partition.Count == 0)
                        _cache.Remove(partitionKey);
                }

                NotifyDeleted(deleted);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }


        public T Get(string partitionKey, string rowKey, DateTime? updateExpirationTime = null, bool resetExpirationDate=false)
        {
            _lock.EnterReadLock();
            try
            {

                if (!_cache.ContainsKey(partitionKey))
                    return default;

                var partition = _cache[partitionKey];

                return partition.TryGetRow(rowKey);

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey, DateTime? updateExpirationTime = null, bool resetExpirationDate=false)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().ToList();
            }
            finally
            {
                _lock.ExitReadLock();
            }


        }

        public IReadOnlyList<T> Get(string partitionKey, int skip, int take, DateTime? updateExpirationTime = null, bool resetExpirationDate=false)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().Skip(skip).Take(take).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey, int skip, int take, Func<T, bool> condition, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().Where(condition).Skip(skip).Take(take).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey, Func<T, bool> condition, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().Where(condition).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        //ToDo - updateExpirationTime
        public IReadOnlyList<T> Get(Func<T, bool> condition = null, 
            DateTime? updateExpirationTime = null, bool resetExpirationDate=false)
        {

            var result = new List<T>();
            _lock.EnterReadLock();

            try
            {
                if (condition == null)
                {
                    foreach (var rows in _cache.Values)
                        result.AddRange(rows.GetRows());
                }
                else
                {
                    foreach (var rows in _cache.Values)
                        result.AddRange(rows.GetRows().Where(condition));
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }

            return result;
        }

        private int _count;
        
        public int Count()
        {
            return _count;
        }
        

        public int Count(string partitionKey)
        {
            _lock.EnterReadLock();

            try
            {
                return _cache.ContainsKey(partitionKey) ? _cache[partitionKey].Count : 0;
            }
            finally
            {
                _lock.ExitReadLock();
            }

        }
        
        public int Count(string partitionKey, Func<T, bool> condition)
        {
            _lock.EnterReadLock();
            try
            {
                return _cache.ContainsKey(partitionKey) ? _cache[partitionKey].GetRows().Count(condition) : 0;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }


        private readonly List<Action<IReadOnlyList<T>>> _changedActions = new List<Action<IReadOnlyList<T>>>();

        public IMyNoSqlServerDataReader<T> SubscribeToUpdateEvents(Action<IReadOnlyList<T>> updateSubscriber, Action<IReadOnlyList<T>> deleteSubscriber)
        {
            _changedActions.Add(updateSubscriber);
            _deletedActions.Add(deleteSubscriber);
            return this;
        }


        private void UpdateCount()
        {
            _count = 0;
            
            foreach (var rows in _cache.Values)
                _count += rows.Count;
        }
        
        private void NotifyChanged(IReadOnlyList<T> items)
        {
            UpdateCount();
            
            if (items == null)
                return;

            if (items.Count == 0)
                return;

            

            foreach (var changedAction in _changedActions)
                changedAction(items);
            

        }


        private readonly List<Action<IReadOnlyList<T>>> _deletedActions = new List<Action<IReadOnlyList<T>>>();



        private void NotifyDeleted(IReadOnlyList<T> items)
        {
            UpdateCount();
            
            if (items == null)
                return;

            if (items.Count == 0)
                return;

            foreach (var changedAction in _deletedActions)
                changedAction(items);
        }
    }


    public static class MyNoSqlReadRepositoryExtensions
    {
        public static MyNoSqlReadRepository<T> SubscribeToTable<T>(
            this IMyNoSqlSubscriber subscriber, string tableName) where T : IMyNoSqlDbEntity
        {
            return new MyNoSqlReadRepository<T>(subscriber, tableName);
        }
    }

}
