using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataReader
{
public class MyNoSqlReadRepository<T> : IMyNoSqlServerDataReader<T> where T:IMyNoSqlDbEntity
    {
        
        private readonly SortedDictionary<string, SortedDictionary<string, T>> _cache = new SortedDictionary<string, SortedDictionary<string, T>>();
        
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public MyNoSqlReadRepository(IMyNoSqlSubscriber subscriber, string tableName)
        {
            subscriber.Subscribe<T>(tableName, Init, InitPartition, Update, Delete);
        }

        private void Init(IReadOnlyList<T> items)
        {
            _lock.EnterWriteLock();
            try
            {  
                _cache.Clear();

                foreach (var item in items)
                {
                    if (!_cache.ContainsKey(item.PartitionKey))
                        _cache.Add(item.PartitionKey, new SortedDictionary<string, T>());

                    var dict = _cache[item.PartitionKey];

                    if (dict.ContainsKey(item.RowKey))
                        dict[item.RowKey] = item;
                    else
                        dict.Add(item.RowKey, item);
                }

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
                if (_cache.ContainsKey(partitionKey))
                    _cache[partitionKey].Clear();

                foreach (var item in items)
                {
                    if (!_cache.ContainsKey(item.PartitionKey))
                        _cache.Add(item.PartitionKey, new SortedDictionary<string, T>());

                    var dict = _cache[item.PartitionKey];

                    if (dict.ContainsKey(item.RowKey))
                        dict[item.RowKey] = item;
                    else
                        dict.Add(item.RowKey, item);
                    
                }
                


            }
            finally
            {
                _lock.ExitWriteLock();
            }
            
            NotifyChanged(items);
        }

        private void Update(IReadOnlyList<T> items)
        {
            _lock.EnterWriteLock();
            try
            {
                foreach (var item in items)
                {
                    if (!_cache.ContainsKey(item.PartitionKey))
                        _cache.Add(item.PartitionKey, new SortedDictionary<string, T>());

                    var dict = _cache[item.PartitionKey];

                    if (dict.ContainsKey(item.RowKey))
                        dict[item.RowKey] = item;
                    else
                        dict.Add(item.RowKey, item);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
            
            NotifyChanged(items);
        }


        private void Delete(IEnumerable<(string partitionKey, string rowKey)> dataToDelete)
        {
            _lock.EnterWriteLock();
            try
            {  
                foreach (var (partitionKey, rowKey) in dataToDelete)
                {
                    if (!_cache.ContainsKey(partitionKey))
                        continue;

                    var dict = _cache[partitionKey];

                    if (dict.ContainsKey(rowKey))
                        dict.Remove(rowKey);

                    if (dict.Count == 0)
                        _cache.Remove(partitionKey);
                }

            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }


        public T Get(string partitionKey, string rowKey)
        {
            _lock.EnterReadLock();
            try
            {

                if (!_cache.ContainsKey(partitionKey))
                    return default(T);

                var dict = _cache[partitionKey];

                if (dict.ContainsKey(rowKey))
                    return dict[rowKey];

            }
            finally
            {
                _lock.ExitReadLock();
            }

            return default(T);
        }
        
        public IReadOnlyList<T> Get(string partitionKey)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].Values.ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }


        }

        public IReadOnlyList<T> Get(string partitionKey, int skip, int take)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].Values.Skip(skip).Take(take).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey, int skip, int take, Func<T, bool> condition)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].Values.Where(condition).Skip(skip).Take(take).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey, Func<T, bool> condition)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].Values.Where(condition).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(Func<T, bool> condition = null)
        {

            var result = new List<T>();
            _lock.EnterReadLock();
            
            try
            {
                if (condition == null)
                {
                    foreach (var rows in _cache.Values)
                        result.AddRange(rows.Values);
                }
                else
                {
                    foreach (var rows in _cache.Values)
                        result.AddRange(rows.Values.Where(condition));
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }

            return result;
        }

        public int Count()
        {
            var result = 0;
            _lock.EnterReadLock();

            try
            {
                foreach (var rows in _cache.Values)
                    result += rows.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }

            return result;
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
                return _cache.ContainsKey(partitionKey) ? _cache[partitionKey].Values.Count(condition) : 0;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }


        private readonly List<Action<IReadOnlyList<T>>> _changedActions = new List<Action<IReadOnlyList<T>>>();
        public void SubscribeToChanges(Action<IReadOnlyList<T>> changed)
        {
            _changedActions.Add(changed);
        }

        private void NotifyChanged(IReadOnlyList<T> item)
        {
            foreach (var changedAction in _changedActions)
                changedAction(item);
        }
    }

}