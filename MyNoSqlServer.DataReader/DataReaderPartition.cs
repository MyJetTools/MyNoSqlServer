using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataReader
{
    public class DataReaderPartition<T> where T: IMyNoSqlDbEntity
    {
        private readonly SortedDictionary<string, T> _rows = new SortedDictionary<string, T>();

        public void Update(T itm)
        {
            if (_rows.ContainsKey(itm.RowKey))
                _rows[itm.RowKey] = itm;
            else
                _rows.Add(itm.RowKey, itm);
        }

        public bool TryDelete(string rowKey, out T value)
        {
            if (!_rows.ContainsKey(rowKey))
            {
                value = default;
                return false;
            }

            value = _rows[rowKey];
            _rows.Remove(rowKey);

            return true;
        }

        public bool HasRow(string rowKey)
        {
            return _rows.ContainsKey(rowKey);
        }

        public T GetRow(string rowKey)
        {
            return _rows[rowKey];
        }

        public T TryGetRow(string rowKey)
        {
            return _rows.ContainsKey(rowKey) ? _rows[rowKey] : default;
        }

        public IEnumerable<T> GetRows()
        {
            return _rows.Values;
        }

        public int Count => _rows.Count;
    }
}