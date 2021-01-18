using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver.Implementation
{


    public class CreateTablePersistEvent : IPersistTableEvent
    {
        public DataSynchronizationPeriod Period { get; private set; }
        public DbTable Table { get; private set; }
        public bool PersistTable { get; private set; }
        public DateTime SnapshotDateTime { get; private set; }

        public static CreateTablePersistEvent Create(DbTable table, bool persistTable, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new CreateTablePersistEvent
            {
                Table = table,
                PersistTable = persistTable,
                Period = period,
                SnapshotDateTime = snapshotDateTime
            };
        }
        
    }

    public class SyncTablePersistEvent : IPersistTableEvent
    {
        public DataSynchronizationPeriod Period { get; private set; }
        public DbTable Table { get; private set; }
        
        public DateTime SnapshotDateTime { get; private set; }

        public static SyncTablePersistEvent Create(DbTable table, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new SyncTablePersistEvent
            {
                Table = table,
                Period = period,
                SnapshotDateTime = snapshotDateTime
            };
        }
    }

    public class SyncPartitionPersistEvent : IPersistTableEvent
    {
        public DataSynchronizationPeriod Period { get; private set; }
        public DbTable Table { get; private set; }
        public DbPartition Partition { get; private set; }
        public DateTime SnapshotDateTime { get; private set; }
        public static SyncPartitionPersistEvent Create(DbTable table, DbPartition partition, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new SyncPartitionPersistEvent
            {
                Table = table,
                Partition = partition,
                Period = period,
                SnapshotDateTime = snapshotDateTime
            };
        }
        
    }
    
    public class SyncDeletePartitionPersistEvent : IPersistTableEvent
    {
        public DataSynchronizationPeriod Period { get; private set; }
        public DbTable Table { get; private set; }
        
        public DbPartition Partition { get; private set; }
        
        public DateTime SnapshotDateTime { get; private set; }
        
        public static SyncDeletePartitionPersistEvent Create(DbTable table, 
            DbPartition partition, DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new SyncDeletePartitionPersistEvent
            {
                Table = table,
                Partition = partition,
                Period = period,
                SnapshotDateTime = snapshotDateTime
            };
        }
    }
    
    public class SyncDeleteTablePersistEvent : IPersistTableEvent
    {
        public DataSynchronizationPeriod Period { get; private set; }
        public DbTable Table { get; private set; }
        
        public DateTime SnapshotDateTime { get; private set; }
        public static SyncDeleteTablePersistEvent Create(DbTable table, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new SyncDeleteTablePersistEvent
            {
                Table = table,
                Period = period,
                SnapshotDateTime = snapshotDateTime
            };
        }
    }
    
    public class PersistTableEventsQueue : ITableToSaveEventsQueue
    {
        private readonly Queue<IPersistTableEvent> _events = new Queue<IPersistTableEvent>();

        public PersistTableEventsQueue(string table)
        {
            Table = table;
        }
        
        public string Table { get; }
        
        public void Enqueue(IPersistTableEvent @event)
        {
            lock (_events)
            {
                _events.Enqueue(@event);
                Count = _events.Count;
            }
        }


        public IPersistTableEvent Dequeue()
        {
            lock (_events)
            {
                try
                {
                    return _events.Count > 0 ? _events.Dequeue() : default;
                }
                finally
                {
                    Count = _events.Count;
                }
            }

        }
        
        public int Count { get; private set; }



    }
}