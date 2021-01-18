using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.Db.Tables
{

    public interface IDbTableReader
    {
        SortedList<string, DbPartition> Partitions { get; }
        
        DbPartition TryGetPartition(string partitionKey);

        IEnumerable<DbRow> GetRows();

        IReadOnlyList<DbPartition> GetAllPartitions();
    }
    
    public interface IDbTableWriter : IDbTableReader
    {
        DbPartition GetOrCreatePartition(string partitionKey);

        bool RemovePartition(string partitionKey);

        void Clear();
    }


    public class DbTable : IDbTableWriter
    {
        private DbTable(string name, bool persistTable, DateTime created)
        {
            Name = name;
            PersistTable = persistTable;
            Created = created;
        }

        public static DbTable CreateByRequest(string name, bool persistTable, DateTime created)
        {
            return new DbTable(name,persistTable, created);
        }
        
        public bool PersistTable { get; }
        
        public DateTime Created { get; }
        
        public DateTime Updated { get; private set; }

        public string Name { get; }

        private readonly ReaderWriterLockSlim _readerWriterLockSlim = new ReaderWriterLockSlim();

        private readonly SortedList<string, DbPartition> _partitions = new SortedList<string, DbPartition>();

        SortedList<string, DbPartition> IDbTableReader.Partitions => _partitions;

        private IReadOnlyList<DbPartition> _partitionsAsList;

        IReadOnlyList<DbPartition> IDbTableReader.GetAllPartitions()
        {
            if (_partitionsAsList != null)
                return _partitionsAsList;
            
            return _partitionsAsList ??= _partitions.Values.ToList();
        }

        IEnumerable<DbRow> IDbTableReader.GetRows()
        {
            return _partitions.Values.SelectMany(dbPartition => dbPartition.GetRows());
        }

        DbPartition IDbTableWriter.GetOrCreatePartition(string partitionKey)
        {
            if (_partitions.ContainsKey(partitionKey))
                return _partitions[partitionKey];

            var partition = DbPartition.Create(partitionKey);
            _partitions.Add(partition.PartitionKey, partition);

            _partitionsAsList = null;
            Updated = DateTime.UtcNow;

            return partition;
        }

        bool IDbTableWriter.RemovePartition(string partitionKey)
        {
            if (_partitions.ContainsKey(partitionKey))
                return false;

            _partitions.Remove(partitionKey);
            _partitionsAsList = null;
            Updated = DateTime.UtcNow;

            return true;
        }

        void IDbTableWriter.Clear()
        {
            _partitions.Clear();
            Updated = DateTime.UtcNow;
        }


        private DbPartition TryGetPartition(string partitionKey)
        {
            return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey] : null;
        }

        DbPartition IDbTableReader.TryGetPartition(string partitionKey) => TryGetPartition(partitionKey);

        public void GetAccessWithReadLock(Action<IDbTableReader> dbTableReader)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                dbTableReader(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
        public T GetAccessWithReadLock<T>(Func<IDbTableReader, T> dbTableReader)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return dbTableReader(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public DateTime GetAccessWithWriteLock(Func<IDbTableWriter, bool> dbTableWriter)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var updated = dbTableWriter(this);
                
                if (updated)
                    Updated = DateTime.UtcNow;


                return Updated;
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public DateTime Clean()
        {
            return GetAccessWithWriteLock(writeAccess =>
            {
                _partitions.Clear();
                _partitionsAsList = Array.Empty<DbPartition>();
                return true;
            });
        }


        public int GetRecordsCount(string partitionKey = null)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return _partitions.Sum(itm => itm.Value.GetRecordsCount());

                return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey].GetRecordsCount() : 0;
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public int GetDataSize(string partitionKey = null)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return _partitions.Sum(itm => itm.Value.DataSize);

                return _partitions.ContainsKey(partitionKey) ? _partitions[partitionKey].DataSize : 0;
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        public int GetPartitionsCount()
        {
            {
                _readerWriterLockSlim.EnterReadLock();
                try
                {
                    return _partitions.Count;
                }
                finally
                {
                    _readerWriterLockSlim.ExitReadLock();
                }
            }

        }
    }
}