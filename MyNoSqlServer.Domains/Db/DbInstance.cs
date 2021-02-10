using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Domains.Db
{
    public class DbInstance
    {
        private readonly ISnapshotSaverScheduler _snapshotSaverScheduler;
        private readonly object _lockObject = new object();
        
        private Dictionary<string, DbTable> _tables = new Dictionary<string, DbTable>();
        private IReadOnlyList<string> _tableNames = Array.Empty<string>();
        private IReadOnlyList<DbTable> _tablesAsArray = Array.Empty<DbTable>();


        public DbInstance(ISnapshotSaverScheduler snapshotSaverScheduler)
        {
            _snapshotSaverScheduler = snapshotSaverScheduler;
        }

        private (DbTable table, bool createdNow) TryToCreateNewTable(string tableName, bool persistTable)
        {

            DbTable syncCreateTable = null;
            try
            {
                lock (_lockObject)
                {

                    if (_tables.TryGetValue(tableName, out var result))
                    {
                        if (result.Persist != persistTable)
                        {
                            result.UpdatePersist(persistTable);
                            syncCreateTable = result;
                        }
                        return (result, false);
                    }
                        

        
                    
                    var tableInstance = DbTable.CreateByRequest(tableName, persistTable);

                    var tables = new Dictionary<string, DbTable>(_tables)
                    {
                        {tableInstance.Name, tableInstance}
                    };

                    _tables = tables;

                    _tableNames = _tables.Keys.ToList();

                    _tablesAsArray = _tables.Values.ToList();
                    syncCreateTable = tableInstance;

                    return (tableInstance, true);
                }
            }
            finally
            {
                if (syncCreateTable != null)
                    _snapshotSaverScheduler.SynchronizeSetTablePersist(syncCreateTable, persistTable);
            }

  
        }
        

        public DbTable CreateTableIfNotExists(string tableName, bool persistTable)
        {
            var tables = _tables;

            if (tables.TryGetValue(tableName, out var foundTable))
            {
                if (foundTable.Persist != persistTable)
                {
                    foundTable.UpdatePersist(persistTable);
                    _snapshotSaverScheduler.SynchronizeSetTablePersist(foundTable, persistTable);
                }

                return foundTable;
            }

            var createdTable = TryToCreateNewTable(tableName, persistTable);
            return createdTable.table;
        }

        public DbTable RestoreTable(string tableName, bool persist)
        {
            lock (_lockObject)
            {
                var result = new DbTable(tableName, persist);
                _tables.Add(tableName, result);
                
                _tableNames = _tables.Keys.ToList();

                _tablesAsArray = _tables.Values.ToList();

                return result;
            }
        }

        public bool CreateTable(string tableName, bool persistTable)
        {

            var tables = _tables;
            if (tables.TryGetValue(tableName, out var foundTable))
            {
                if (foundTable.Persist != persistTable)
                {
                    foundTable.UpdatePersist(persistTable);
                    _snapshotSaverScheduler.SynchronizeSetTablePersist(foundTable, persistTable);
                }

                return false;
            }

            var createdTable = TryToCreateNewTable(tableName, persistTable);

            return createdTable.createdNow;
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