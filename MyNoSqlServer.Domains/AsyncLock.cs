using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNoSqlServer.Domains
{
    
    public class AsyncLock
    {

        private readonly Queue<TaskCompletionSource<int>> _tasks = new Queue<TaskCompletionSource<int>>();

        private int _locked = 0;

        public ValueTask LockAsync()
        {
            lock (_tasks)
            {
                try
                {
                    if (_locked == 0)
                        return new ValueTask();
                    var task = new TaskCompletionSource<int>();
                    _tasks.Enqueue(task);
                    return new ValueTask(task.Task);
                }
                finally
                {
                    _locked++;
                }
            }
        }

        public void Unlock()
        {
            lock (_tasks)
            {
                _locked--;

                if (_tasks.Count == 0)
                    return;
                
                var result = _tasks.Dequeue();
                result.SetResult(0);
            }
        }
        
    }
}