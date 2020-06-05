using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    
    public class SyncPartition : ISyncTask
    {
        public DbTable DbTable { get; private set; }
        public DbPartition DbPartition { get; set; }
        
        public DateTime SyncDateTime { get; private set; }

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
    
    public class SyncTable: ISyncTask
    {
        public DbTable DbTable { get; private set; }
        
        public DateTime SyncDateTime { get; private set; }

        public static SyncTable Create(DbTable dbTable, DataSynchronizationPeriod period)
        {
            return new SyncTable
            {
                DbTable = dbTable,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period)
            };
        }
        
    }

    public class SyncDeletePartition: ISyncTask
    {
        public string TableName { get; private set; }
        public string PartitionKey { get; private set; }
        
        public DateTime SyncDateTime { get; set; }

        public static SyncDeletePartition Create(string tableName, string partitionKey, DataSynchronizationPeriod period)
        {
            return new SyncDeletePartition
            {
                TableName = tableName,
                PartitionKey = partitionKey,
                SyncDateTime = DateTime.UtcNow.GetNextPeriod(period)
            };
        }

    }
    
    public class SnapshotSaverScheduler : ISnapshotSaverScheduler
    {
        
        private readonly object _lockObject = new object();

        private readonly Dictionary<string, SyncTable> _syncTables = new Dictionary<string, SyncTable>();

        private readonly Dictionary<string, Dictionary<string, SyncPartition>> _syncPartitions 
            = new Dictionary<string, Dictionary<string, SyncPartition>>();

        
        private readonly Dictionary<string, Dictionary<string, SyncDeletePartition>> _deletePartitions 
            = new Dictionary<string, Dictionary<string, SyncDeletePartition>>();

        
        public int TasksToSyncCount()
        {
            lock (_lockObject)
            {
                return _syncTables.Count +
                       _syncPartitions.Values.Sum(i => i.Count) +
                       _deletePartitions.Values.Sum(i => i.Count);
            }
        }
        
        public void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period)
        {
            lock (_lockObject)
            {
                if (!_syncTables.ContainsKey(dbTable.Name))
                    _syncTables.Add(dbTable.Name, SyncTable.Create(dbTable, period));
            }
        }

        private bool CheckIfWeHaveSyncTableTaskEarlier(string tableName, DataSynchronizationPeriod period)
        {
            if (!_syncTables.ContainsKey(tableName))
                return false;

            var syncTableTask = _syncTables[tableName];

            return syncTableTask.SyncDateTime < DateTime.UtcNow.GetNextPeriod(period);
        }


        public void SynchronizePartition(DbTable dbTable, DbPartition partitionToSave, DataSynchronizationPeriod period)
        {
            lock (_lockObject)
            {
                if (CheckIfWeHaveSyncTableTaskEarlier(dbTable.Name, period))
                    return;
                
                if (!_syncPartitions.ContainsKey(dbTable.Name))
                    _syncPartitions.Add(dbTable.Name, new Dictionary<string, SyncPartition>());

                var dict = _syncPartitions[dbTable.Name];
                
                if (!dict.ContainsKey(partitionToSave.PartitionKey))
                    dict.Add(partitionToSave.PartitionKey, SyncPartition.Create(dbTable, partitionToSave, period));
            }
        }

        public void SynchronizeDeletePartition(string tableName, string partitionKey, DataSynchronizationPeriod period)
        {
            lock (_lockObject)
            {
                if (CheckIfWeHaveSyncTableTaskEarlier(tableName, period))
                    return;
                
                if (!_deletePartitions.ContainsKey(tableName))
                    _deletePartitions.Add(tableName, new Dictionary<string, SyncDeletePartition>());
                
                var dict = _deletePartitions[tableName];
                
                if (!dict.ContainsKey(partitionKey))
                    dict.Add(partitionKey, SyncDeletePartition.Create(tableName, partitionKey, period));  
            }
        }


        private SyncDeletePartition GetDeleteTask(DateTime nowTime)
        {
            foreach (var tasksByTable in _deletePartitions.Values)
            {
                foreach (var partitionToDelete in tasksByTable.Values)
                {
                    if (partitionToDelete.SyncDateTime <= nowTime)
                        return partitionToDelete;
                }
            }

            return null;
        }

        private SyncPartition GetSyncPartitionTask(DateTime nowTime)
        {
            foreach (var tasksByTable in _syncPartitions.Values)
            {
                foreach (var partitionTask in tasksByTable.Values)
                {
                    if (partitionTask.SyncDateTime <= nowTime)
                        return partitionTask;
                    
                }
            }

            return null;
        }
        
        private SyncTable GetSyncTableTask(DateTime nowTime)
        {
            foreach (var syncTableValue in _syncTables.Values)
            {
                if (syncTableValue.SyncDateTime <= nowTime)
                    return syncTableValue;
            }

            return null;
        }
        
        public ISyncTask GetTaskToSync(bool appIsShuttingDown)
        {

            var dt = appIsShuttingDown ? DateTime.UtcNow.AddYears(20) : DateTime.UtcNow;
            lock (_lockObject)
            {

                var taskToDelete = GetDeleteTask(dt);

                if (taskToDelete != null)
                {
                    _deletePartitions.Remove(taskToDelete);
                    return taskToDelete;
                }

                var syncPartitionTask = GetSyncPartitionTask(dt);
                if (syncPartitionTask != null)
                {
                    _syncPartitions.Remove(syncPartitionTask);
                    return syncPartitionTask;
                }


                var syncTableTask = GetSyncTableTask(dt);
                if (syncTableTask != null)
                    _syncTables.Remove(syncTableTask);

                return syncTableTask;

            }
        }


    }



    public static class SyncQueueDecorators
    {
        public static void Remove(this Dictionary<string, Dictionary<string, SyncDeletePartition>> dictionary,
            SyncDeletePartition taskToDelete)
        {
            dictionary[taskToDelete.TableName].Remove(taskToDelete.PartitionKey);
            if (dictionary[taskToDelete.TableName].Count == 0)
                dictionary.Remove(taskToDelete.TableName);
        }
        
        public static void Remove(this Dictionary<string, Dictionary<string, SyncPartition>> dictionary,
            SyncPartition taskToSync)
        {
            dictionary[taskToSync.DbTable.Name].Remove(taskToSync.DbPartition.PartitionKey);
            
            if (dictionary[taskToSync.DbTable.Name].Count == 0)
                dictionary.Remove(taskToSync.DbTable.Name);
        }

        public static void Remove(this Dictionary<string, SyncTable> dictionary, SyncTable syncTable)
        {
            dictionary.Remove(syncTable.DbTable.Name);
        }
        
    }
}