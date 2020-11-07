using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Db
{
    public class DbInstance
    {
        private readonly object _lockObject = new object();
        
        private Dictionary<string, DbTable> _tables = new Dictionary<string, DbTable>();
        public IReadOnlyList<DbTable> Tables { get; private set; } = Array.Empty<DbTable>();
        public IReadOnlyList<string> TableNames { get; private set; } = Array.Empty<string>();

        private DbTable CreateTableAndUpdateDictionary(string tableName)
        {
            var tableInstance = DbTable.CreateByRequest(tableName);

            var tables = new Dictionary<string, DbTable>(_tables) {{tableInstance.Name, tableInstance}};

            _tables = tables;

            TableNames = _tables.Keys.ToList();

            Tables = _tables.Values.ToList();

            return tableInstance;
        }

        public DbTable CreateTableIfNotExists(string tableName)
        {
            lock (_lockObject)
            {
                return _tables.ContainsKey(tableName) 
                    ? _tables[tableName] 
                    : CreateTableAndUpdateDictionary(tableName);
            }

        }

        public bool CreateTable(string tableName)
        {

            lock (_lockObject)
            {
                if (_tables.ContainsKey(tableName))
                    return false;

                CreateTableAndUpdateDictionary(tableName);
                return true;
            }
            
        }
        
        public DbTable TryGetTable(string tableName)
        {
            var tables = _tables;
            return tables.ContainsKey(tableName) ? tables[tableName] : null;

        }


    }
}