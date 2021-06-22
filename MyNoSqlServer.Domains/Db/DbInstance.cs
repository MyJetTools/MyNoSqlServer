using System;
using System.Collections.Generic;
using System.Linq;

using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Db
{

    public interface IDbInstanceWriteAccess
    {
        DbTable CreateTable(string tableName, bool persist, int maxPartitionsAmount);
        DbTable TryGetTable(string tableName);

    }
    
    public class DbInstance: IDbInstanceWriteAccess
    {
        private readonly object _lockObject = new ();
        
        private Dictionary<string, DbTable> _tables = new ();
        private IReadOnlyList<DbTable> _tablesAsArray = Array.Empty<DbTable>();


        public void GetWriteAccess(Action<IDbInstanceWriteAccess> writeAccess)
        {
            lock (_lockObject)
            {
                writeAccess(this);
            }
        }
        
        public T GetWriteAccess<T>(Func<IDbInstanceWriteAccess, T> writeAccess)
        {
            lock (_lockObject)
            {
                return writeAccess(this);
            }
        }

        DbTable IDbInstanceWriteAccess.CreateTable(string tableName, bool persist, int maxPartitionsAmount)
        {


            if (_tables.TryGetValue(tableName, out var result))
                return result;

            var tableInstance = new DbTable(tableName, persist, maxPartitionsAmount);

            var tables = new Dictionary<string, DbTable>(_tables)
            {
                { tableInstance.Name, tableInstance }
            };

            _tables = tables;

            _tablesAsArray = _tables.Values.ToList();

            return tableInstance;
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