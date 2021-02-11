using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyNoSqlServer.DataReader
{
    public class MyNoSqlSubscriber : IMyNoSqlSubscriber
    {
        protected const string SystemAction = "system";

        protected readonly Dictionary<string, Func<byte[], IEnumerable<object>>> Deserializers
            = new Dictionary<string, Func<byte[], IEnumerable<object>>>();

        private readonly Dictionary<string, Action<IEnumerable<object>>> _initCallbacks
            = new Dictionary<string, Action<IEnumerable<object>>>();

        private readonly Dictionary<string, Action<string, IEnumerable<object>>> _initPartitionCallbacks
            = new Dictionary<string, Action<string, IEnumerable<object>>>();

        private readonly Dictionary<string, Action<IEnumerable<object>>> _updateCallbacks
            = new Dictionary<string, Action<IEnumerable<object>>>();

        private readonly Dictionary<string, Action<IEnumerable<(string partitionKey, string rowKey)>>> _deleteCallbacks
            = new Dictionary<string, Action<IEnumerable<(string partitionKey, string rowKey)>>>();


        public IEnumerable<string> GetTablesToSubscribe()
        {
            return Deserializers.Keys;
        }
        public void Subscribe<T>(string tableName, Action<IReadOnlyList<T>> initAction, Action<string, IReadOnlyList<T>> initPartitionAction, Action<IReadOnlyList<T>> updateAction,
            Action<IEnumerable<(string partitionKey, string rowKey)>> deleteActions)
        {
            if (tableName == SystemAction)
                throw new Exception("Table can not have name: " + SystemAction);

            Deserializers.Add(tableName, data =>
            {
                var json = Encoding.UTF8.GetString(data);
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T[]>(json).Cast<object>();
            });

            _initCallbacks.Add(tableName, items => { initAction(items.Cast<T>().ToList()); });

            _initPartitionCallbacks.Add(tableName,
                (partitionKey, items) => { initPartitionAction(partitionKey, items.Cast<T>().ToList()); });

            _updateCallbacks.Add(tableName, items => { updateAction(items.Cast<T>().ToList()); });

            _deleteCallbacks.Add(tableName, deleteActions);
        }
        
        public void HandleInitTableEvent(string tableName, byte[] data)
        {
            var items = Deserializers[tableName](data);
            _initCallbacks[tableName](items);
        }
        public void HandleInitPartitionEvent(string tableName, string partitionKey, byte[] data)
        {
            var items = Deserializers[tableName](data);
            _initPartitionCallbacks[tableName](partitionKey, items);
        }

        public void HandleUpdateRowEvent(string tableName, byte[] data)
        {
            var items = Deserializers[tableName](data);
            _updateCallbacks[tableName](items);

        }

        public void HandleDeleteRowEvent(string tableName, IEnumerable<(string partitionKey, string rowKey)> items)
        {
            _deleteCallbacks[tableName](items);
        }

        
    }

}