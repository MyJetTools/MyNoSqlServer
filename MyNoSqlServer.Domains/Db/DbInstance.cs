using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.Db
{
    public static class DbInstance
    {
        
        private static readonly ReaderWriterLockSlim ReaderWriterLockSlim = new ReaderWriterLockSlim();
        
        private static readonly Dictionary<string, DbTable> Tables = new Dictionary<string, DbTable>();

        public static DbTable CreateTableIfNotExists(string tableName)
        {
            tableName = tableName.ToLowerInvariant();

            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (Tables.ContainsKey(tableName))
                    return Tables[tableName];

                var tableInstance = DbTable.CreateByRequest(tableName);

                Tables.Add(tableName, tableInstance);

                return tableInstance;

            }
            finally
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }

        }

        public static bool CreateTable(string tableName)
        {

            tableName = tableName.ToLowerInvariant();
            ReaderWriterLockSlim.EnterWriteLock();
            try
            {
                if (Tables.ContainsKey(tableName))
                    return false;
                
                var tableInstance = DbTable.CreateByRequest(tableName);
                
                Tables.Add(tableName, tableInstance);

                return true;

            }
            finally 
            {
                ReaderWriterLockSlim.ExitWriteLock();
            }
            
        }
        
        public static DbTable[] GetTables()
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                
                return Tables.Values.ToArray();
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
            
        }


        public static string[] GetTablesList()
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                
                return Tables.Keys.ToArray();
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
            
        }
        
        public static DbTable GetTable(string tableName)
        {
            ReaderWriterLockSlim.EnterReadLock();
            try
            {
                return Tables.ContainsKey(tableName) ? Tables[tableName] : null;
            }
            finally
            {
                ReaderWriterLockSlim.ExitReadLock();
            }
            
        }


    }
}