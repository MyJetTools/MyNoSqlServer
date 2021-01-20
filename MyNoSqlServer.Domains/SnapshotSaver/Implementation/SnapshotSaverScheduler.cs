using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver.Implementation
{
    
    public class SnapshotSaverScheduler : ISnapshotSaverScheduler
    {
        
        private readonly object _lockObject = new object();

        private readonly Dictionary<string, PersistTableEventsQueue> _syncTables = new Dictionary<string, PersistTableEventsQueue>();
        private IReadOnlyList<PersistTableEventsQueue> _syncTablesAsList = Array.Empty<PersistTableEventsQueue>();

        public IReadOnlyList<ITableToSaveEventsQueue> GetEventsQueue()
        {
            lock (_lockObject)
            {
                return _syncTablesAsList ??= _syncTables.Values.ToList();
            }
        }

        public ITableToSaveEventsQueue TryGetEventsQueue(string tableName)
        {
            lock (_lockObject)
            {
                return _syncTables.ContainsKey(tableName) 
                    ? _syncTables[tableName] 
                    : null;
            }
        }

        public int TasksToSyncCount()
        {
            lock (_lockObject)
            {
                return _syncTables.Values.Sum(itm => itm.Count);
            }
        }

        private PersistTableEventsQueue GetQueueToEnqueue(string tableName)
        {
            if (_syncTables.TryGetValue(tableName, out var result))
                return result;

            result = new PersistTableEventsQueue(tableName);
            _syncTables.Add(tableName, result);
            _syncTablesAsList = _syncTables.Values.ToList();
            return result;
        }

        public void SynchronizeCreateTable(DbTable dbTable, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            lock (_lockObject)
            {
                GetQueueToEnqueue(dbTable.Name).Enqueue( 
                    CreateTablePersistEvent.Create(dbTable, period, snapshotDateTime));
            }
        }

        public void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            lock (_lockObject)
            {
                GetQueueToEnqueue(dbTable.Name).Enqueue( 
                    SyncTablePersistEvent.Create(dbTable,  period, snapshotDateTime));
            }
        }

        public void SynchronizePartition(DbTable dbTable,  DbPartition partition, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            lock (_lockObject)
            {
                GetQueueToEnqueue(dbTable.Name).Enqueue( 
                    SyncPartitionPersistEvent.Create(dbTable, partition, period, snapshotDateTime));
            }
        }

        public void SynchronizeDeletePartition(DbTable dbTable, DbPartition partition, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            lock (_lockObject)
            {
                GetQueueToEnqueue(dbTable.Name).Enqueue( 
                    SyncDeletePartitionPersistEvent.Create(dbTable, partition, period, snapshotDateTime));
            }
        }
        
        public void SynchronizeDeleteTable(DbTable dbTable, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            lock (_lockObject)
            {
                GetQueueToEnqueue(dbTable.Name).Enqueue( 
                    SyncDeleteTablePersistEvent.Create(dbTable, period, snapshotDateTime));
            }
        }

    }


}