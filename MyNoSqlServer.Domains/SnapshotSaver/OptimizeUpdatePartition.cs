using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    public static class OptimizeUpdatePartition
    {
        private static ISyncTask TryToFindHigherTask(this SortedDictionary<long, ISyncTask> syncTasks, ISyncPartitionTask syncPartitionTask)
        {
            foreach (var syncTask in syncTasks.Values)
            {
                switch (syncTask)
                {
                    case IDeletePartitionTask deletePartitionTask 
                        when deletePartitionTask.PartitionKey == syncPartitionTask.PartitionKey:
                        return deletePartitionTask;
                    
                    case ISyncTableTask syncTableTask:
                        return syncTableTask;
                    
                    case ICreateTableSyncTask _:
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


        public static ISyncTask OptimizeSyncPartitionTask(this SortedDictionary<long, ISyncTask> syncTasks, ISyncPartitionTask syncPartitionTask)
        {

            var higherTask = syncTasks.TryToFindHigherTask(syncPartitionTask);

            if (higherTask == null)
                return syncPartitionTask;
            
            syncTasks.Optimize(higherTask.Id, syncPartitionTask.PartitionKey);
            return higherTask;

        }
        
    }
}