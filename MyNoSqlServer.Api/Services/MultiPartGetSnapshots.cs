using System;
using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Services
{

    public class MultiPartGetItem
    {

        private readonly Queue<DbRow> _items = new();

        public MultiPartGetItem(string id, string tableName, IEnumerable<DbRow> records)
        {
            Id = id;
            TableName = tableName;
            foreach (var dbRow in records)
                _items.Enqueue(dbRow);
        }

        public DateTimeOffset LastAccess { get; private set; } = DateTimeOffset.UtcNow;

        public string TableName { get; }

        public DbRow GetNext()
        {
            LastAccess = DateTimeOffset.UtcNow;
            return _items.Count == 0 ? null : _items.Dequeue();
        }

        public string Id { get; }


        public int Count => _items.Count;
    }


    public class MultiPartGetSnapshots
    {
        private readonly Dictionary<string, MultiPartGetItem> _partitionsToDeliver =
            new ();

        private readonly object _lockObject = new();

        public string Init(string tableName, IEnumerable<DbRow> records)
        {
            lock (_lockObject)
            {
                var requestId = Guid.NewGuid().ToString("N");

                _partitionsToDeliver.Add(requestId, new MultiPartGetItem(requestId, tableName, records));
                return requestId;
            }
        }


        public IReadOnlyList<DbRow> GetNextPartitionId(string requestId, int maxAmount)
        {
            List<DbRow> result = null;
            lock (_lockObject)
            {
                if (!_partitionsToDeliver.TryGetValue(requestId, out var multiPartGetItem))
                    return Array.Empty<DbRow>();

                var dbRow = multiPartGetItem.GetNext();

                while (dbRow != null)
                {

                    result ??= new List<DbRow>();
                    result.Add(dbRow);
                    
                    if (result.Count >= maxAmount)
                        break;
                    
                    dbRow = multiPartGetItem.GetNext();
                }
            }
            return result;
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