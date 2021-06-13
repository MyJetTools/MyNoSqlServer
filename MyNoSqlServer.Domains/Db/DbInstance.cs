using System;
using System.Collections.Generic;
using System.Linq;

using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Db
{
    public class DbInstance
    {
        private readonly object _lockObject = new ();
        
        private Dictionary<string, DbTable> _tables = new ();
        private IReadOnlyList<DbTable> _tablesAsArray = Array.Empty<DbTable>();

        private readonly SyncEventsDispatcher _syncEventsDispatcher;

        public DbInstance(SyncEventsDispatcher syncEventsDispatcher)
        {
            _syncEventsDispatcher = syncEventsDispatcher;
        }

        private (DbTable table, bool createdNow) TryToCreateNewTable(string tableName, bool persistTable, TransactionEventAttributes attributes)
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
                            result.UpdatePersist(persistTable, attributes);
                            syncCreateTable = result;
                        }
                        return (result, false);
                    }

        
                    
                    var tableInstance = DbTable.CreateByRequest(tableName, persistTable, _syncEventsDispatcher);

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
                    _syncEventsDispatcher.Dispatch( SyncTableAttributes.Create(attributes, syncCreateTable));
            }
  
        }
        

        public DbTable CreateTableIfNotExists(string tableName, bool persistTable, TransactionEventAttributes attributes)
        {
            var tables = _tables;

            if (tables.TryGetValue(tableName, out var foundTable))
            {
                if (foundTable.Persist != persistTable)
                {
                    foundTable.UpdatePersist(persistTable, attributes);
                    
                }

                return foundTable;
            }

            var createdTable = TryToCreateNewTable(tableName, persistTable, attributes);
            return createdTable.table;
        }


        public void SetMaxPartitionsAmount(string tableName, int maxPartitionsAmount, TransactionEventAttributes attributes)
        {
            var table = GetTable(tableName);
            
            if (table.MaxPartitionsAmount == maxPartitionsAmount)
                return;
            
            table.SetMaxPartitionsAmount(maxPartitionsAmount, attributes);
     
        }

        public DbTable RestoreTable(string tableName, bool persist)
        {
            lock (_lockObject)
            {
                var result = new DbTable(tableName, persist, _syncEventsDispatcher);
                _tables.Add(tableName, result);
                _tablesAsArray = _tables.Values.ToList();
                return result;
            }
        }

        public bool CreateTable(string tableName, bool persistTable, TransactionEventAttributes attributes)
        {

            var tables = _tables;
            if (tables.TryGetValue(tableName, out var foundTable))
            {
                if (foundTable.Persist != persistTable)
                {
                    foundTable.UpdatePersist(persistTable, attributes);
                }

                return false;
            }

            var createdTable = TryToCreateNewTable(tableName, persistTable, attributes);

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

        public void Gc()
        {
            foreach (var table in _tablesAsArray)
            {
                table.Gc();
            }
        }

        public void ReplaceTable(DbTable table)
        {
            var tables = new Dictionary<string, DbTable>(_tables) ;

            lock (_lockObject)
            {

                if (tables.ContainsKey(table.Name))
                    tables[table.Name] = table;
                else
                    tables.Add(table.Name, table);

                _tables = tables;

            }
        }
    }
}