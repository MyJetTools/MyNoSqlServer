using System;
using System.Collections.Generic;
using System.Linq;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    public class SyncQueueByTable
    {

        private readonly SortedDictionary<long, ISyncTask> _queue = new SortedDictionary<long, ISyncTask>();

        public void Enqueue(ISyncTask syncTask)
        {
            _queue.Add(syncTask.Id, syncTask);
        }


        private void OptimizeTime()
        {
            var lowestTime = _queue.Values.Min(itm => itm.SyncDateTime);

            var taskWithLowestTime = _queue.Values.FirstOrDefault(itm => itm.SyncDateTime == lowestTime);
            
            if (taskWithLowestTime == null)
                return;

            foreach (var syncTask in _queue.Values)
            {
                if (syncTask.Id >= taskWithLowestTime.Id)
                    break;
                
                if (syncTask.SyncDateTime > taskWithLowestTime.SyncDateTime)
                    syncTask.SyncDateTime = lowestTime;
            }
        }


        public ISyncTask Dequeue(DateTime utcNow)
        {

            var nextTaskToDo = _queue.Values.FirstOrDefault();
            if (nextTaskToDo == null)
                return null;
            
            OptimizeTime();

            if (nextTaskToDo.SyncDateTime > utcNow)
                return null;

            _queue.Remove(nextTaskToDo.Id);

            if (nextTaskToDo is ISyncPartitionTask syncPartitionTask)
                return _queue.OptimizeSyncPartitionTask(syncPartitionTask);

            return nextTaskToDo;

        }

        public int Count => _queue.Count;
    }
}