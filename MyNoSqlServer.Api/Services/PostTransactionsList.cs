using System;
using System.Collections.Generic;
using System.Threading;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Api.Services
{

    public class UpdateTransactionsSequence
    {
        private readonly List<IDbTransactionAction> _transactions = new ();

        private readonly object _lockObject = new();

        public UpdateTransactionsSequence(string id)
        {
            Id = id;
        }
        
        public string Id { get; }


        public IReadOnlyList<IDbTransactionAction> GetTransactionsToExecute()
        {
            lock (_lockObject)
            {
                return _transactions;
            }

        }

        public void PostTransactions(IEnumerable<DbTable> tables, IEnumerable<IDbTransactionAction> transactions)
        {
            lock (_lockObject)
            {
                _transactions.AddRange(transactions);

                foreach (var dbTable in tables)
                {
                    if (!_tables.ContainsKey(dbTable.Name))
                        _tables.Add(dbTable.Name, dbTable);
                }
        
            }
            
            LastAccessTime = DateTimeOffset.UtcNow;
        }

        public DateTimeOffset LastAccessTime { get; private set; } = DateTimeOffset.UtcNow;

        private readonly Dictionary<string, DbTable> _tables = new ();

        public IReadOnlyDictionary<string, DbTable> Tables => _tables;

    }
    
    
    public class PostTransactionsList
    {

        private readonly Dictionary<string, UpdateTransactionsSequence> _transactions = new ();
        private readonly ReaderWriterLockSlim _lock = new ();

        public UpdateTransactionsSequence StartTransaction()
        {
            var id = Guid.NewGuid().ToString("N");
            
            _lock.EnterWriteLock();
            try
            {
                var result = new UpdateTransactionsSequence(id);
                _transactions.Add(id, result);
                return result;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public UpdateTransactionsSequence TryGet(string transactionId)
        {
           _lock.EnterReadLock();
           try
           {
               return _transactions.TryGetValue(transactionId, out var result) ? result : null;
           }
           finally
           {
               _lock.ExitReadLock();
           }
        }

        public UpdateTransactionsSequence TryDelete(string transactionId)
        {
            _lock.EnterWriteLock();
            try
            {
                return _transactions.Remove(transactionId, out var result) ? result : null;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }


        public static TimeSpan GcTransactionTimeSpan = TimeSpan.FromMinutes(10);

        public IReadOnlyList<UpdateTransactionsSequence> GetTransactionsToGc()
        {
            List<UpdateTransactionsSequence> result = null;

            var now = DateTimeOffset.UtcNow;
            
            _lock.EnterReadLock();
            try
            {

                foreach (var transaction in _transactions.Values)
                {

                    if (now - transaction.LastAccessTime > GcTransactionTimeSpan)
                    {
                        result ??= new List<UpdateTransactionsSequence>();
                        result.Add(transaction);
                    } 
                    
                }

            }
            finally
            {
                _lock.ExitReadLock();
            }

            return result;
        }


        public void GcTransactions()
        {
            var transactionsToGc = GetTransactionsToGc();

            if (transactionsToGc == null)
                return;

            _lock.EnterWriteLock();
            try
            {
                foreach (var transaction in transactionsToGc)
                {
                    if (_transactions.ContainsKey(transaction.Id))
                        _transactions.Remove(transaction.Id);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }

        }

    }
}