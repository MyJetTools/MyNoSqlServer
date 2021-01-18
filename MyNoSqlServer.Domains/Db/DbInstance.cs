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
        private readonly PersistenceHandler _persistenceHandler;

        private readonly object _lockObject = new object();
        
        private Dictionary<string, DbTable> _tables = new Dictionary<string, DbTable>();
        public IReadOnlyList<DbTable> Tables { get; private set; } = Array.Empty<DbTable>();
        public IReadOnlyList<string> TableNames { get; private set; } = Array.Empty<string>();

        public DbInstance(PersistenceHandler persistenceHandler)
        {
            _persistenceHandler = persistenceHandler;
        }
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

        
        private async Task<OperationResult> CreateAndPersistTableAsync(string tableName, bool persist, DateTime created)
        {
            var tableToSave = CreateTableAndUpdateDictionary(tableName, persist, created);

            await _persistenceHandler.SynchronizeCreateTableAsync(tableToSave, DataSynchronizationPeriod.Immediately, tableToSave.Updated);
            return OperationResult.Ok;
        }
        
        
        public ValueTask<OperationResult> CreateTableAsync(string tableName, bool persist, DateTime created)
        {
            lock (_lockObject)
            {
                if (_tables.ContainsKey(tableName))
                    return new ValueTask<OperationResult>(OperationResult.CanNotCreateObject);

                return new ValueTask<OperationResult>(CreateAndPersistTableAsync(tableName, persist, created));
            }
        }
        
        public ValueTask CreateTableIfNotExistsAsync(string tableName, bool persist, DateTime created)
        {
            lock (_lockObject)
            {
                return _tables.ContainsKey(tableName) 
                    ? new ValueTask() 
                    : new ValueTask(CreateAndPersistTableAsync(tableName, persist, created));
            }
        }
        
        public DbTable TryGetTable(string tableName)
        {
            var tables = _tables;
            return tables.ContainsKey(tableName) ? tables[tableName] : null;

        }

        public async Task<OperationResult> DeleteTableAsync(string tableName)
        {
            DbTable dbTable;
            lock (_lockObject)
            {
                if (!_tables.ContainsKey(tableName))
                    return OperationResult.TableNotFound;

                dbTable = _tables[tableName];

                _tables = _tables.RemoveByCreatingNewDictionary(tableName);

                UpdateCaches();
            }

            if (dbTable != null)
                await _persistenceHandler.SynchronizeDeleteTableAsync(dbTable, DataSynchronizationPeriod.Immediately, dbTable.Updated);
            return OperationResult.Ok;
        }
    }
}