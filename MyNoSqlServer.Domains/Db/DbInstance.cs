using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Domains.Db
{
    public class DbInstance
    {

        private readonly object _lockObject = new object();
        
        private Dictionary<string, DbTable> _tables = new Dictionary<string, DbTable>();
        public IReadOnlyList<DbTable> Tables { get; private set; } = Array.Empty<DbTable>();
        public IReadOnlyList<string> TableNames { get; private set; } = Array.Empty<string>();


        private void UpdateCaches()
        {
            TableNames = _tables.Keys.ToList();

            Tables = _tables.Values.ToList();
        }

        private DbTable CreateTableAndUpdateDictionary(string tableName, bool persist, DateTime created)
        {
            var tableInstance = DbTable.CreateByRequest(tableName, persist, created);

            _tables = _tables.AddByCreatingNewDictionary(tableInstance.Name, tableInstance);

            UpdateCaches();

            return tableInstance;
        }
                
        public DbTable CreateTableIfNotExists(string tableName, bool persist, DateTime created)
        {
            lock (_lockObject)
            {
                return _tables.TryGetValue(tableName, out var result) 
                    ? result 
                    : CreateTableAndUpdateDictionary(tableName,  persist, created);
            }
        }
        
        
        
        public (bool, DbTable) CreateTable(string tableName, bool persist, DateTime created)
        {
            lock (_lockObject)
            {
                if (_tables.ContainsKey(tableName))
                    return (false, _tables[tableName]);

                var result = CreateTableAndUpdateDictionary(tableName, persist, created);
                return (true, result);
            }
        }
        
        
        public DbTable TryGetTable(string tableName)
        {
            var tables = _tables;
            return tables.ContainsKey(tableName) ? tables[tableName] : null;

        }

        public DbTable DeleteTable(string tableName)
        {
            lock (_lockObject)
            {
                if (!_tables.ContainsKey(tableName))
                    return null;

                var result = _tables[tableName];

                _tables = _tables.RemoveByCreatingNewDictionary(tableName);

                UpdateCaches();

                return result;
            }

        }
    }
}