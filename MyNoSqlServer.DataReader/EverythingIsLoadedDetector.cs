using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNoSqlServer.DataReader
{
    public class EverythingIsLoadedDetector
    {
        private readonly Dictionary<string, string> _remainsToLoad = new Dictionary<string, string>();

        private readonly TaskCompletionSource<int> _task = new TaskCompletionSource<int>();

        public Task GetTask() => _task.Task;

        public void ChargeTheTable(string tableName)
        {
            _remainsToLoad.Add(tableName, tableName);
        }

        public void Check(string tableName)
        {
            if (_remainsToLoad.ContainsKey(tableName))
            {
                _remainsToLoad.Remove(tableName);

                if (_remainsToLoad.Count == 0)
                {
                    _task.SetResult(0);
                }
            }
        }
    }
}