using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.Query;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Db.Tables
{

    public interface IDbTableReadAccess
    {
        IReadOnlyDictionary<string, IReadOnlyList<DbRow>> GetAllRows();

        IEnumerable<IPartitionReadAccess> GetAllPartitions();

        IPartitionReadAccess TryGetPartition(string partitionKey);

        int GetPartitionsAmount();

        DbRow TryGetRow(string partitionKey, string rowKey);

    }

    public interface IDbTableWriteAccess
    {
        void InitTable(IReadOnlyDictionary<string, IReadOnlyList<DbRow>> partitions);

        void InitPartition(string partitionKey, IReadOnlyList<DbRow> rows);

        IPartitionWriteAccess TryGetPartitionWriteAccess(string partitionKey);
        IPartitionWriteAccess GetOrCreatePartition(string partitionKey);

        IReadOnlyList<DbPartition> DeletePartitions(IEnumerable<string> partitions);
        
        IReadOnlyDictionary<string, List<DbRow>> DeleteRows(string partitionKey, IEnumerable<string> rowKeys);

        IReadOnlyList<DbPartition> GetPartitionsToGc(int maxPartitionsAmount);

        DbPartition DeletePartition(string partitionKey);
        bool Clear();
    }
    

    public class DbTable : IDbTableReadAccess, IDbTableWriteAccess
    {
        public bool Persist { get; private set; }
        
        public int MaxPartitionsAmount { get; private set; }

        
        public bool SetAttributes(bool persist, int maxPartitionsAmount)
        {
            
            if (persist == Persist && MaxPartitionsAmount == maxPartitionsAmount)
                return false;
            
            Persist = persist;
            MaxPartitionsAmount = maxPartitionsAmount;
            return true;
        }

        
        public bool SetMaxPartitionsAmount(int maxPartitionsAmount)
        {
            if (MaxPartitionsAmount == maxPartitionsAmount)
                return false;
            MaxPartitionsAmount = maxPartitionsAmount;

            return true;
        }

        public DbTable(string name, bool persistThisTable, int maxPartitionsAmount)
        {
            Name = name;
            Persist = persistThisTable;

        }


        public bool UpdatePersist(bool persist, TransactionEventAttributes attributes)
        {
            if (Persist == persist)
                return false;
            Persist = persist;

            return true;
        }

        public string Name { get; }

        private readonly ReaderWriterLockSlim _readerWriterLockSlim = new ();

        private readonly DbPartitionsList _partitions = new ();


        IEnumerable<IPartitionReadAccess> IDbTableReadAccess.GetAllPartitions()
        {
            return _partitions.GetAllPartitions().ToList();
        }

        IPartitionReadAccess IDbTableReadAccess.TryGetPartition(string partitionKey)
        {
            return _partitions.TryGet(partitionKey);
        }

        int IDbTableReadAccess.GetPartitionsAmount()
        {
            return _partitions.Count;
        }


        public int GetPartitionsCount()
        {
            return _partitions.Count;
        }


        public DbRow GetEntity(string partitionKey, string rowKey)
        {
            return GetReadAccess(readAccess => readAccess.TryGetRow(partitionKey, rowKey));
        }
        
        
        
        DbRow IDbTableReadAccess.TryGetRow(string partitionKey, string rowKey)
        {
            var dbPartition = _partitions.TryGet(partitionKey) as IPartitionReadAccess;
            return dbPartition?.TryGetRow(rowKey);
        }



        public IReadOnlyList<DbRow> GetAllRecords(int? limit, int? skip)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                var records 
                    = _partitions
                        .GetAllPartitions()
                        .SelectMany(partition => partition.GetAllRows());

                if (skip != null)
                    records = records.Skip(skip.Value);

                if (limit != null)
                    records = records.Take(limit.Value);

                return records.ToList();

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
        public void GetReadAccess(Action<IDbTableReadAccess> readAccess)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                readAccess(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
        public void GetWriteAccess(Action<IDbTableWriteAccess> readAccess)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {

                readAccess(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }
        
        public T GetReadAccess<T>(Func<IDbTableReadAccess, T> writeAccess)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return writeAccess(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        
        public T GetWriteAccess<T>(Func<IDbTableWriteAccess, T> writeAccess)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {

                return writeAccess(this);
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        public IReadOnlyList<DbRow> GetRecords(string partitionKey, int? limit, int? skip)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {

                var partition = _partitions.TryGet(partitionKey);
                
                if (partition == null)
                    return Array.Empty<DbRow>();

                if (skip == null && limit == null)
                    return partition.GetAllRows();

                return partition.GetRowsWithLimit(limit, skip);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }
        

        IReadOnlyDictionary<string, List<DbRow>> IDbTableWriteAccess.DeleteRows(string partitionKey,
            IEnumerable<string> rowKeys)
        {
            Dictionary<string, List<DbRow>> deletedRows = null;

            var partition = _partitions.TryGet(partitionKey);
            if (partition == null)
                return null;

            foreach (var rowKey in rowKeys)
            {
                var dbRow = partition.DeleteRow(rowKey);
                if (dbRow == null) 
                    continue;
                
                deletedRows ??= new Dictionary<string, List<DbRow>>();

                if (!deletedRows.ContainsKey(dbRow.PartitionKey))
                    deletedRows.Add(dbRow.PartitionKey, new List<DbRow>());

                deletedRows[dbRow.PartitionKey].Add(dbRow);
            }

            return deletedRows;

        }

        /*
        public void CleanAndKeepLastRecords(string partitionKey,
            int amount, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return;

                var dbRows = partition.CleanAndKeepLastRecords(amount);
                
                
                _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRows(attributes, this, dbRows));

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }
        */

        public bool HasRecord(IMyNoSqlDbEntity entityInfo)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                var partition = _partitions.TryGet(entityInfo.PartitionKey);
                if (partition == null)
                    return false;

                return partition.HasRecord(entityInfo.RowKey);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        DbPartition IDbTableWriteAccess.DeletePartition(string partitionKey)
        {
            return _partitions.DeletePartition(partitionKey);
        }

        bool IDbTableWriteAccess.Clear()
        {
            return _partitions.Clear();
        }

        IReadOnlyList<DbPartition> IDbTableWriteAccess.DeletePartitions(IEnumerable<string> partitions)
        {

            List<DbPartition> result = null;
            
            foreach (var partitionKey in partitions)
            {
                var dbPartition = _partitions.DeletePartition(partitionKey);
                
                
                if (dbPartition != null)
                {
                    result ??= new List<DbPartition>();
                    result.Add(dbPartition);
                }
            }

            return result;

        }


        public IEnumerable<DbRow> ApplyQuery(IEnumerable<QueryCondition> queryConditions)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                return _partitions.ApplyQuery(queryConditions);
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public int GetRecordsCount(string partitionKey)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return _partitions.GetAllPartitions().Sum(itm => itm.GetRecordsCount());

                var partition = _partitions.TryGet(partitionKey);

                return partition?.GetRecordsCount() ?? 0;
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public IReadOnlyList<DbRow> GetMultipleRows(string partitionKey, string[] rowKeys)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return Array.Empty<DbRow>();
                
                var partition = _partitions.TryGet(partitionKey);

                if (partition == null)
                    return Array.Empty<DbRow>();

                return partition.GetRows(rowKeys);

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        public IEnumerable<DbRow> GetHighestRowAndBelow(string partitionKey, string rowKey, int maxAmount)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(partitionKey))
                    return Array.Empty<DbRow>();

                var partition = _partitions.TryGet(partitionKey);

                if (partition == null)
                    return Array.Empty<DbRow>();

                return partition.GetHighestRowAndBelow(rowKey, maxAmount);

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        /*
        public void KeepMaxPartitions(in int amount, TransactionEventAttributes attributes)
        {
            var partitionsToGc = GetPartitionsToGc(amount);

            if (partitionsToGc.Count == 0)
                return;


            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                List<DbPartition> deleted = null;
                foreach (var dbPartition in partitionsToGc)
                {
                    var partition = _partitions.DeletePartition(dbPartition.PartitionKey);

                    if (partition != null)
                    {
                        deleted ??= new List<DbPartition>();
                        deleted.Add(partition);
                    }
                }
                
                if (deleted != null)
                    _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.AsDeletePartitions(attributes, this, deleted));

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }
*/

        private DbPartition GetPartitionIfItHasToBeCleaned(string partitionKey, int maxAmount)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                var partition = _partitions.TryGet(partitionKey);
                if (partition == null)
                    return null;

                return partition.GetRecordsCount() <= maxAmount ? null : partition;
            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }


        /*
        public OperationResult Replace(DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(entity.PartitionKey);
                if (partition == null)
                    return OperationResult.RecordNotFound;

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return OperationResult.RecordNotFound;

                if (record.TimeStamp != entity.TimeStamp)
                    return OperationResult.RecordChangedConcurrently;

                record.Replace(entity, now);
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, this, record));

                return OperationResult.Ok;

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }



        public OperationResult Merge(
            DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {
            var dbRow = TryGetDbRowWithReadLock(entity.PartitionKey, entity.RowKey);

            if (dbRow == null)
                return OperationResult.RecordNotFound;

            if (dbRow.TimeStamp != entity.TimeStamp)
                return OperationResult.RecordChangedConcurrently;

            var newEntities = dbRow.MergeEntities(entity);

            _readerWriterLockSlim.EnterWriteLock();
            try
            {
                var partition = _partitions.TryGet(entity.PartitionKey);
                if (partition == null)
                    return OperationResult.RecordNotFound;

                var record = partition.TryGetRow(entity.RowKey);

                if (record == null)
                    return OperationResult.RecordNotFound;

                if (record.TimeStamp != entity.TimeStamp)
                    return OperationResult.RecordChangedConcurrently;

                record.Replace(newEntities, now);
                
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, this, record));

                return OperationResult.Ok;

            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }
        }

        
        public DbPartition KeepMaxRecordsAmount(string partitionKey, int maxAmount)
        {

            var dbPartition = GetPartitionIfItHasToBeCleaned(partitionKey, maxAmount);

            if (dbPartition == null)
                return null;


            _readerWriterLockSlim.EnterWriteLock();
            try
            {

                if (dbPartition.GetRecordsCount() <= maxAmount)
                    return null;


                dbPartition.CleanAndKeepLastRecords(maxAmount);
                return dbPartition;
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }
        */
        public PartitionSnapshot GetPartitionSnapshot(string partitionKey)
        {
            _readerWriterLockSlim.EnterReadLock();
            try
            {
                var dbPartition = _partitions.TryGet(partitionKey);
                if (dbPartition == null)
                    return null;

                var rowsAsBytes = dbPartition.GetAllRows().ToJsonArray().AsArray();
                return PartitionSnapshot.Create(partitionKey, rowsAsBytes);

            }
            finally
            {
                _readerWriterLockSlim.ExitReadLock();
            }
        }

        
        /*
        public void BulkDelete(Dictionary<string, List<string>> partitionsAndRows, TransactionEventAttributes attributes)
        {
            _readerWriterLockSlim.ExitWriteLock();
            try
            {
                

                foreach (var (partitionKey, rowKeys) in partitionsAndRows)
                {
                    if (rowKeys == null || rowKeys.Count == 0)
                    {
                        var deletedPartition = _partitions.DeletePartition(partitionKey);
                        if (deletedPartition != null)
                            _syncEventsDispatcher.Dispatch(InitPartitionsTransactionEvent.AsDeletePartition(attributes, this, deletedPartition));
                    }
                    else
                    {
                        var partition = _partitions.TryGet(partitionKey);

                        if (partition != null)
                        {
                            List<DbRow> deletedRows = null; 
                            foreach (var rowKey in rowKeys)
                            {
                                var dbRow = partition.DeleteRow(rowKey);
                                if (dbRow != null)
                                {
                                    deletedRows ??= new List<DbRow>();
                                    deletedRows.Add(dbRow);
                                }
                            }
                            
                            if (deletedRows != null)
                                _syncEventsDispatcher.Dispatch(DeleteRowsTransactionEvent.AsRows(attributes, this, deletedRows));
                        }
                    }

                }
            }
            finally
            {
                _readerWriterLockSlim.ExitWriteLock();
            }

        }
        */
        

        IReadOnlyList<DbPartition> IDbTableWriteAccess.GetPartitionsToGc(int maxAmount)
        {
            return _partitions.GetPartitionsToGc(maxAmount);
        }


        IReadOnlyDictionary<string, IReadOnlyList<DbRow>> IDbTableReadAccess.GetAllRows()
        {
            return _partitions.GetAllPartitions()
                .ToDictionary(
                    dbPartition => dbPartition.PartitionKey, 
                    dbPartition => dbPartition.GetAllRows());
        }

        void IDbTableWriteAccess.InitTable(IReadOnlyDictionary<string, IReadOnlyList<DbRow>> partitions)
        {
            _partitions.Clear();

            foreach (var (partitionKey, partitionData) in partitions)
            {
                var dbPartition = _partitions.GetOrCreate(partitionKey);
                dbPartition.InitPartition(partitionData);
            }
        }

        void IDbTableWriteAccess.InitPartition(string partitionKey, IReadOnlyList<DbRow> rows)
        {
            var partition = _partitions.GetOrCreate(partitionKey);

            partition.InitPartition(rows);
        }

        
        /*
        void IDbTableWriteAccess.Insert(DynamicEntity entity, DateTime now, TransactionEventAttributes attributes)
        {

            var partition = _partitions.GetOrCreate(entity.PartitionKey);

            var dbRow = DbRow.CreateNew(entity, now);

            if (partition.Insert(dbRow))
            {
                _syncEventsDispatcher.Dispatch(UpdateRowsTransactionEvent.AsRow(attributes, this, dbRow));
                return OperationResult.Ok;
            }

            return OperationResult.RecordExists;

        }
        */

        IPartitionWriteAccess IDbTableWriteAccess.TryGetPartitionWriteAccess(string partitionKey)
        {
            return _partitions.TryGet(partitionKey);
        }
        
        IPartitionWriteAccess IDbTableWriteAccess.GetOrCreatePartition(string partitionKey)
        {
            return _partitions.GetOrCreate(partitionKey);
        }

    }
    
}