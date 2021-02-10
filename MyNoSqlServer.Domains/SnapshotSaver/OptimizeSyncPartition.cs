using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    public static class OptimizeSyncPartition
    {
        private static ISyncTask TryToFindHigherTask(this SortedDictionary<long, ISyncTask> syncTasks)
        {
            foreach (var syncTask in syncTasks.Values)
            {
                switch (syncTask)
                {
                    
                    case ISyncTableTask syncTableTask:
                        return syncTableTask;
                    
                    case ISetTableSavable _:
                        return null;
                }
            }

            return null;
        }


        private static IReadOnlyList<long> GetUpdatePartitionsEVent(this SortedDictionary<long, ISyncTask> syncTasks, long taskId, string partitionKey)
        {
            List<long> result = null;
            foreach (var syncTask in syncTasks.Values)
            {
                
                if (syncTask.Id >= taskId)
                    break;
                
                if (syncTask is ISyncPartitionTask syncPartitionTask)
                {
                    if (syncPartitionTask.PartitionKey == partitionKey)
                    {
                        result ??= new List<long>();
                        result.Add(syncPartitionTask.Id);
                    }
                }
            }

            return result ?? (IReadOnlyList<long>)Array.Empty<long>();
        }
        
        

        private static void Optimize(this SortedDictionary<long, ISyncTask> syncTasks, long taskId, string partitionKey)
        {
            foreach (var keyToDelete in syncTasks.GetUpdatePartitionsEVent(taskId, partitionKey))
                syncTasks.Remove(keyToDelete);
        }

        private static IReadOnlyList<long> GetSameTasks(this SortedDictionary<long, ISyncTask> syncTasks, string partitionKey)
        {
            List<long> result = null;
            foreach (var syncTask in syncTasks.Values)
            {
                if (syncTask is ISyncPartitionTask syncPartitionTask)
                {
                    if (syncPartitionTask.PartitionKey == partitionKey)
                    {
                        result ??= new List<long>();
                        result.Add(syncPartitionTask.Id);
                    }
                }
            }

            return result ?? (IReadOnlyList<long>)Array.Empty<long>();
        }

        private static void DeleteSameTasks(this SortedDictionary<long, ISyncTask> syncTasks, string partitionKey)
        {
            foreach (var taskId in syncTasks.GetSameTasks(partitionKey))
                syncTasks.Remove(taskId);
        }


        public static ISyncTask OptimizeSyncPartitionTask(this SortedDictionary<long, ISyncTask> syncTasks, ISyncPartitionTask syncPartitionTask)
        {

            var higherTask = syncTasks.TryToFindHigherTask();

            if (higherTask == null)
            {
                syncTasks.DeleteSameTasks(syncPartitionTask.PartitionKey);
                return syncPartitionTask;
            }
            
            syncTasks.Optimize(higherTask.Id, syncPartitionTask.PartitionKey);
            syncTasks.Remove(higherTask.Id);
            return higherTask;

        }
        
    }
}