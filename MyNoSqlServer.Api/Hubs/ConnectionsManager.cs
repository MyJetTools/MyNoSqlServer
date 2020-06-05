using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.AspNetCore.SignalR;

namespace MyNoSqlServer.Api.Hubs
{

    public interface IConnection
    {
        IClientProxy Client { get; }
        string Id { get; }
    }


    public class ConnectionsManager<T> where T:IConnection
    {
        
        private readonly Dictionary<string, T> _items = new Dictionary<string, T>();
        
        private readonly ReaderWriterLockSlim _lockSlim = new ReaderWriterLockSlim();

        public void Add(string id, T item)
        {
            _lockSlim.EnterWriteLock();
            try
            {
               _items.Add(id, item);
            }
            finally
            {
                _lockSlim.ExitWriteLock();
            }
        }
        
        public void Delete(string connectionId)
        {
            
            _lockSlim.EnterWriteLock();
            try
            {
                if (_items.ContainsKey(connectionId))
                    _items.Remove(connectionId);
            }
            finally
            {
                _lockSlim.ExitWriteLock();
            }
            
        }        

        public IReadOnlyList<T> Get(Func<T, bool> predicate)
        {
            _lockSlim.EnterReadLock();
            try
            {
                return _items.Values.Where(predicate).ToList();
            }
            finally
            {
                _lockSlim.ExitReadLock();
            }
        }


        public bool TryGet(string connectionId, out T result)
        {

            bool hasElement;
            
            _lockSlim.EnterReadLock();
            try
            {
                hasElement = _items.ContainsKey(connectionId);
                result = hasElement ? _items[connectionId] : default(T);
            }
            finally
            {
                _lockSlim.ExitReadLock();
            }

            return hasElement;
        }
        
        
        public void Update(string connectionId, Action<T> updateAction)
        {
            if (TryGet(connectionId, out var item))
                updateAction(item);
        }
        
        public IReadOnlyList<T> Get()
        {
            _lockSlim.EnterReadLock();
            try
            {
                return _items.Values.ToList();
            }
            finally
            {
                _lockSlim.ExitReadLock();
            }
        }

    }
    
}