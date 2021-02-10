using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{

    public class SyncCreateTable : ICreateTableSyncTask
    {
        public DbTable DbTable { get; private set; }
        public DateTime SyncDateTime { get; set; }
        public long Id { get;  set; }

        public static SyncCreateTable Create(DbTable dbTable, DataSynchronizationPeriod period)
        {
            return new SyncCreateTable
            {
                DbTable = dbTable,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period)
            };
        }
    }
    
    public class SyncTable: ISyncTableTask
    {
        public long Id { get;  set; }
        public DbTable DbTable { get; private set; }
        
        public DateTime SyncDateTime { get; set; }

        public static SyncTable Create(DbTable dbTable, DataSynchronizationPeriod period)
        {
            return new SyncTable
            {
                DbTable = dbTable,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period)
            };
        }
    }
    
    public class SyncPartition : ISyncPartitionTask
    {
        public long Id { get;  set; }
        public DbTable DbTable { get; private set; }
        public DbPartition DbPartition { get; set; }


        public string PartitionKey => DbPartition.PartitionKey;
        
        public DateTime SyncDateTime { get; set; }

        public static SyncPartition Create(DbTable dbTable, DbPartition dbPartition, DataSynchronizationPeriod period)
        {
            return new SyncPartition
            {
                DbTable = dbTable,
                DbPartition = dbPartition,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period)
            };
        }
    
    }

    public class SyncDeletePartition: IDeletePartitionTask
    {
        public long Id { get;  set; }
        public DbTable DbTable { get; private set; }
        public string PartitionKey { get; private set; }
        public DateTime SyncDateTime { get; set; }
        public static SyncDeletePartition Create(DbTable table, string partitionKey, DataSynchronizationPeriod period)
        {
            return new SyncDeletePartition
            {
                DbTable = table,
                PartitionKey = partitionKey,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period)
            };
        }
    }
    
    public class SnapshotSaverScheduler : ISnapshotSaverScheduler
    {
        
        private readonly object _lockObject = new object();


        private readonly Dictionary<string, SyncQueueByTable> _syncTasks = new Dictionary<string, SyncQueueByTable>();

        
        public int TasksToSyncCount()
        {
            lock (_lockObject)
                return _syncTasks.Values.Sum(i => i.Count);
        }


        private long _taskId = 0;

        private void EnqueueTask(DbTable dbTable, ISyncTask syncTask)
        {
            
            lock (_lockObject)
            {
                _taskId++;
                syncTask.Id = _taskId;
                
                if (!_syncTasks.ContainsKey(dbTable.Name))
                    _syncTasks.Add(dbTable.Name, new SyncQueueByTable());
                
                _syncTasks[dbTable.Name].Enqueue(syncTask);
            }
        }


        public void SynchronizeCreateTable(DbTable dbTable)
        {
            EnqueueTask(dbTable, SyncCreateTable.Create(dbTable, DataSynchronizationPeriod.Immediately));
        }
        
        public void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period)
        {
            EnqueueTask(dbTable, SyncTable.Create(dbTable, period));
        }



        public void SynchronizePartition(DbTable dbTable, DbPartition partitionToSave, DataSynchronizationPeriod period)
        {
            EnqueueTask(dbTable, SyncPartition.Create(dbTable, partitionToSave, period));
        }

        public void SynchronizeDeletePartition(DbTable dbTable, string partitionKey, DataSynchronizationPeriod period)
        {
            EnqueueTask(dbTable, SyncDeletePartition.Create(dbTable, partitionKey, period));
        }
        
        public ISyncTask GetTaskToSync(bool appIsShuttingDown)
        {

            var dt = appIsShuttingDown ? DateTime.UtcNow.AddYears(20) : DateTime.UtcNow;
            lock (_lockObject)
            {
                var queue = _syncTasks.Values.FirstOrDefault(itm => itm.Count > 0);
                return queue?.Dequeue(dt);
            }
        }


    }

}