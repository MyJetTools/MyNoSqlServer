using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver.Implementation
{


    public class CreateTablePersistEvent : IPersistTableEvent
    {
        public DateTime DequeueTime { get; set; }
        public DbTable Table { get; private set; }
        public DateTime SnapshotDateTime { get; private set; }

        public static CreateTablePersistEvent Create(DbTable table, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new CreateTablePersistEvent
            {
                Table = table,
                DequeueTime = snapshotDateTime.GetDequeueTime(period),
                SnapshotDateTime = snapshotDateTime
            };
        }
        
    }

    public class SyncTablePersistEvent : IPersistTableEvent
    {
        public DateTime DequeueTime { get; set; }
        public DbTable Table { get; private set; }
        
        public DateTime SnapshotDateTime { get; private set; }

        public static SyncTablePersistEvent Create(DbTable table, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new SyncTablePersistEvent
            {
                Table = table,
                DequeueTime = snapshotDateTime.GetDequeueTime(period),
                SnapshotDateTime = snapshotDateTime
            };
        }
    }

    public class SyncPartitionPersistEvent : IPersistTableEvent
    {
        public DateTime DequeueTime { get; set; }
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
                DequeueTime = snapshotDateTime.GetDequeueTime(period),
                SnapshotDateTime = snapshotDateTime
            };
        }
        
    }
    
    public class SyncDeletePartitionPersistEvent : IPersistTableEvent
    {
        public DateTime DequeueTime { get; set; }
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
                DequeueTime = snapshotDateTime.GetDequeueTime(period),
                SnapshotDateTime = snapshotDateTime
            };
        }
    }
    
    public class SyncDeleteTablePersistEvent : IPersistTableEvent
    {
        public DateTime DequeueTime { get; set; }
        public DbTable Table { get; private set; }
        
        public DateTime SnapshotDateTime { get; private set; }
        public static SyncDeleteTablePersistEvent Create(DbTable table, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime)
        {
            return new SyncDeleteTablePersistEvent
            {
                Table = table,
                DequeueTime = snapshotDateTime.GetDequeueTime(period),
                SnapshotDateTime = snapshotDateTime
            };
        }
    }
    
    public class PersistTableEventsQueue : ITableToSaveEventsQueue
    {
        private readonly LinkedList<IPersistTableEvent> _events = new LinkedList<IPersistTableEvent>();

        public PersistTableEventsQueue(string table)
        {
            Table = table;
        }
        
        public string Table { get; }


        private void InsertSyncPartitionEventPreExecution(SyncPartitionPersistEvent syncPartitionEvent)
        {
            if (_events.Count == 0)
                return;

            var last = _events.Last.Value;

            if (last is SyncPartitionPersistEvent lastSyncPartitionEvent)
            {
                if (lastSyncPartitionEvent.SnapshotDateTime < syncPartitionEvent.SnapshotDateTime)
                    _events.RemoveLast();
            }

        }

        private void UpdateDequeueDateIfNeeded(IPersistTableEvent newEvent)
        {

            foreach (var @event in _events)
            {
                if (@event.DequeueTime > newEvent.DequeueTime)
                    @event.DequeueTime = newEvent.DequeueTime;
            }
            
        }
        
        
        public void Enqueue(IPersistTableEvent newEvent)
        {
            lock (_events)
            {
                UpdateDequeueDateIfNeeded(newEvent);
                
                if (newEvent is SyncPartitionPersistEvent syncPartitionEvent)
                    InsertSyncPartitionEventPreExecution(syncPartitionEvent);
                
                _events.AddLast(newEvent);
                Count = _events.Count;
            }
        }


        public IPersistTableEvent Dequeue(DateTime nowTime)
        {
            lock (_events)
            {
                try
                {
                    if (_events.Count > 0)
                    {
                        var result = _events.First.Value;
                        if (result.DequeueTime > nowTime)
                            return null;
                        
                        _events.RemoveFirst();
                        return result;
                    }
                    
                    return default;
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