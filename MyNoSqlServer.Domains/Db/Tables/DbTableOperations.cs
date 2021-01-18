using System;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Domains.Db.Tables
{
    public class DbTableOperations
    {
        private readonly DbInstance _dbInstance;
        private readonly PersistenceHandler _persistenceHandler;

        public DbTableOperations(DbInstance dbInstance, PersistenceHandler persistenceHandler)
        {
            _dbInstance = dbInstance;
            _persistenceHandler = persistenceHandler;
        }

        public async Task<OperationResult> CreateTableAsync(string tableName, bool persist, DateTime now)
        {
            var (created, dbTable) = _dbInstance.CreateTable(tableName, persist, now);

            if (!created)
                return OperationResult.CanNotCreateObject;
            
            await _persistenceHandler.SynchronizeCreateTableAsync(dbTable, DataSynchronizationPeriod.Immediately, dbTable.Updated);
            return OperationResult.Ok;
        }


        public async Task<OperationResult> DeleteTableAsync(string tableName)
        {

            var dbTable = _dbInstance.DeleteTable(tableName);

            if (dbTable != null)
            {
                await _persistenceHandler.SynchronizeDeleteTableAsync(dbTable, DataSynchronizationPeriod.Immediately, dbTable.Updated);
                return OperationResult.Ok;
            }

            return OperationResult.TableNotFound;
        }
        
    }
}