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
        private readonly ISnapshotStorage _snapshotStorage;
        private readonly object _lockObject = new object();
        
        private Dictionary<string, DbTable> _tables = new Dictionary<string, DbTable>();
        public IReadOnlyList<DbTable> Tables { get; private set; } = Array.Empty<DbTable>();
        public IReadOnlyList<string> TableNames { get; private set; } = Array.Empty<string>();


        public DbInstance(ISnapshotStorage snapshotStorage)
        {
            _snapshotStorage = snapshotStorage;
        }

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
                return _tables.TryGetValue(tableName, out var result) 
                    ? result 
                    : CreateTableAndUpdateDictionary(tableName);
            }
        }

        
        private async Task<OperationResult> CreateAndPersistTableAsync(string tableName)
        {
            var tableToSave = CreateTableAndUpdateDictionary(tableName);

            await _snapshotStorage.CreateTableAsync(tableToSave);
            return OperationResult.Ok;
        }
        
        
        public ValueTask<OperationResult> CreateTableAsync(string tableName)
        {
            lock (_lockObject)
            {
                if (_tables.ContainsKey(tableName))
                    return new ValueTask<OperationResult>(OperationResult.CanNotCreateObject);

                return new ValueTask<OperationResult>(CreateAndPersistTableAsync(tableName));
            }
        }
        
        public ValueTask CreateTableIfNotExistsAsync(string tableName)
        {
            lock (_lockObject)
            {
                return _tables.ContainsKey(tableName) 
                    ? new ValueTask() 
                    : new ValueTask(CreateAndPersistTableAsync(tableName));
            }
        }
        
        public DbTable TryGetTable(string tableName)
        {
            var tables = _tables;
            return tables.ContainsKey(tableName) ? tables[tableName] : null;

        }


    }
}