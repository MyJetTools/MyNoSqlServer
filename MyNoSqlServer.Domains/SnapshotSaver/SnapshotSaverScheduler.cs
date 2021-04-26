using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{

    public class SyncSetTableSavable : ISetTableSavable
    {
        public DbTable DbTable { get; private set; }
        public DateTime SyncDateTime { get; set; }
        public long Id { get;  set; }
        

        public bool Savable { get; private set; }
        
        public static SyncSetTableSavable Create(DbTable dbTable, bool savable, DataSynchronizationPeriod period)
        {
            return new SyncSetTableSavable
            {
                DbTable = dbTable,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period),
                Savable = savable
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

        public string PartitionKey { get; private set; }
        
        public DateTime SyncDateTime { get; set; }

        public static SyncPartition Create(DbTable dbTable, string partitionKey, DataSynchronizationPeriod period)
        {
            return new SyncPartition
            {
                DbTable = dbTable,
                PartitionKey = partitionKey,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period)
            };
        }
    
    }
    
    public class SnapshotSaverScheduler : ISnapshotSaverScheduler
    {
        
        private readonly object _lockObject = new object();


        private readonly Dictionary<string, SyncQueueByTable> _syncTasks = new ();

        
        public int TasksToSyncCount()
        {
            lock (_lockObject)
                return _syncTasks.Values.Sum(i => i.Count);
        }


        private long _taskId;

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


        public void SynchronizeSetTablePersist(DbTable dbTable, bool savable)
        {
            EnqueueTask(dbTable, SyncSetTableSavable.Create(dbTable, savable, DataSynchronizationPeriod.Immediately));
        }
        
        public void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period)
        {
            EnqueueTask(dbTable, SyncTable.Create(dbTable, period));
        }

        public void SynchronizePartition(DbTable dbTable, string partitionKey, DataSynchronizationPeriod period)
        {
            EnqueueTask(dbTable, SyncPartition.Create(dbTable, partitionKey, period));
        }
        
        public ISyncTask GetTaskToSync(bool appIsShuttingDown)
        {

            var dt = appIsShuttingDown ? DateTime.UtcNow.AddYears(20) : DateTime.UtcNow;

            lock (_lockObject)
            {
                foreach (var tableQueue in _syncTasks.Values)
                {
                    var result = tableQueue.Dequeue(dt);
                    if (result != null)
                        return result;

                }
            }

            return null;
        }

        public IReadOnlyList<ISyncTask> GetTasksToSync(string tableName)
        {
            var result = new List<ISyncTask>();
            lock (_lockObject)
            {

                if (!_syncTasks.TryGetValue(tableName, out var queueByTable))
                    return result;

                var nextTask = queueByTable.Dequeue(DateTime.UtcNow.AddYears(20));

                while (nextTask != null)
                {
                    result.Add(nextTask);
                    nextTask = queueByTable.Dequeue(DateTime.UtcNow.AddYears(20));
                }
            }

            return result;
        }


    }

}