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
        
        public DbTable TryGetTable(string tableName)
        {
            var tables = _tables;
            
            return tables.TryGetValue(tableName, out var result)  ? result : null;

        }

        public DbTable GetTable(string tableName)
        {
            var tables = _tables;

            if (tables.TryGetValue(tableName, out var result))
                return result;

            throw new Exception($"Table with {tableName} is not found");

        }


    }
}