using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Transactions;

namespace MyNoSqlServer.Api.Services
{
    

    public class UpdateTransactionsSequence
    {
        private readonly Dictionary<string, List<IDbTransaction>> _transactions =
            new ();

        private readonly object _lockObject = new();

        public UpdateTransactionsSequence(string id)
        {
            Id = id;
        }
        
        public string Id { get; }


        public (string tableName, IReadOnlyList<IDbTransaction>) GetNextTransactionsToExecute()
        {
            lock (_lockObject)
            {
                var nextTable = _transactions.Keys.FirstOrDefault();

                if (nextTable == null)
                    return (null, null);

                var result = _transactions[nextTable];

                return (nextTable, result);
            }

        }
        
        public void PostTransactions(string tableName, IEnumerable<IDbTransaction> transactions)
        {
            lock (_lockObject)
            {
                if (!_transactions.ContainsKey(tableName))
                    _transactions.Add(tableName, new List<IDbTransaction>());

                var byTable = _transactions[tableName];
                
                byTable.AddRange(transactions);
            }
        }

        
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

    }
}