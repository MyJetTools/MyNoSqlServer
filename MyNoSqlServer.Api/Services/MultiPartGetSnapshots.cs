using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Api.Services
{

    public class MultiPartGetItem
    {

        private readonly Queue<string> _items = new();

        public MultiPartGetItem(string id, string tableName, IEnumerable<string> initPartitions)
        {
            Id = id;
            TableName = tableName;
            foreach (var partition in initPartitions)
                _items.Enqueue(partition);
        }

        public DateTimeOffset LastAccess { get; private set; } = DateTimeOffset.UtcNow;

        public string TableName { get; }

        public string GetNext()
        {
            LastAccess = DateTimeOffset.UtcNow;
            return _items.Dequeue();
        }

        public string Id { get; }


        public int Count => _items.Count;
    }


    public class MultiPartGetSnapshots
    {
        private readonly Dictionary<string, MultiPartGetItem> _partitionsToDeliver =
            new ();

        private readonly object _lockObject = new();

        public string Init(string tableName, IEnumerable<string> partitions)
        {
            lock (_lockObject)
            {
                var requestId = Guid.NewGuid().ToString("N");

                _partitionsToDeliver.Add(requestId, new MultiPartGetItem(requestId, tableName, partitions));
                return requestId;
            }
        }


        public (string tableName, string partitionKey) GetNextPartitionId(string requestId)
        {
            lock (_lockObject)
            {
                if (!_partitionsToDeliver.TryGetValue(requestId, out var multiPartGetItem))
                    return (null, null);

                var nextPartitionKey = multiPartGetItem.GetNext();
                
                Console.WriteLine("Amount: "+multiPartGetItem.Count);

                if (multiPartGetItem.Count == 0)
                    _partitionsToDeliver.Remove(requestId);

                return (multiPartGetItem.TableName, nextPartitionKey);
            }
        }

        public static TimeSpan GcTimeSpan = TimeSpan.FromMinutes(1);

        private IReadOnlyList<MultiPartGetItem> FindItemsToGc()
        {
            List<MultiPartGetItem> result = null;

            var now = DateTimeOffset.UtcNow;

            foreach (var partitionToDeliver in _partitionsToDeliver.Values)
            {
                if (now - partitionToDeliver.LastAccess > GcTimeSpan)
                {
                    result ??= new List<MultiPartGetItem>();

                    result.Add(partitionToDeliver);
                }

            }

            return result;
        }



        public void Gc()
        {

            lock (_lockObject)
            {
                var itemsToGc = FindItemsToGc();

                if (itemsToGc == null)
                    return;

                foreach (var item in itemsToGc)
                    _partitionsToDeliver.Remove(item.Id);
            }
        }

    }
}