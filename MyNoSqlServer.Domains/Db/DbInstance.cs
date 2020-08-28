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
        private IReadOnlyList<string> _tableNames = Array.Empty<string>();
        private IReadOnlyList<DbTable> _tablesAsArray = Array.Empty<DbTable>();


        private DbTable CreateTableAndUpdateDictionary(string tableName)
        {
            var tableInstance = DbTable.CreateByRequest(tableName);

            var tables = new Dictionary<string, DbTable>(_tables)
            {
                {tableInstance.Name, tableInstance}
            };


            _tables = tables;

            _tableNames = _tables.Keys.ToList();

            _tablesAsArray = _tables.Values.ToList();

            return tableInstance;
        }

        public DbTable CreateTableIfNotExists(string tableName)
        {
            var tables = _tables;
            if (tables.ContainsKey(tableName))
                return tables[tableName];

            lock (_lockObject)
            {
                if (_tables.ContainsKey(tableName))
                    return _tables[tableName];

                return CreateTableAndUpdateDictionary(tableName);
            }

        }

        public bool CreateTable(string tableName)
        {

            var tables = _tables;
            if (tables.ContainsKey(tableName))
                return false;

            lock (_lockObject)
            {
                if (_tables.ContainsKey(tableName))
                    return false;

                CreateTableAndUpdateDictionary(tableName);
                return true;
            }
            
        }
        
        public IReadOnlyList<DbTable> GetTables()
        {
            return _tablesAsArray;
        }


        public IReadOnlyList<string> GetTablesList()
        {
            return _tableNames;

        }
        
        public DbTable TryGetTable(string tableName)
        {
            var tables = _tables;
            return tables.ContainsKey(tableName) ? tables[tableName] : null;

        }


    }
}