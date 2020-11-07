using System;
using MyDependencies;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.GarbageCollection;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Tests
{
    public static class TestScripts
    {

        public static DbTable CreateTable(this MyIoc ioc, string tableName)
        {
            var dbInstance = ioc.GetService<DbInstance>();
            return dbInstance.CreateTableIfNotExists(tableName);
        }



        public static void InsertToTable(this MyIoc ioc, DbTable table, IMyNoSqlDbEntity entity, DateTime? dt = null)
        {
            var dbOperations =  ioc.GetService<DbTableWriteOperations>();

            dt ??= DateTime.UtcNow;
            
            dbOperations.InsertAsync(table, entity.ToDynamicEntity(), DataSynchronizationPeriod.Sec1, dt.Value).AsTask().Wait();
        }


        public static void ExpirationTimerTick(this MyIoc ioc, DateTime now)
        {
            var expirationService = ioc.GetService<ExpiredEntitiesGarbageCollector>();

            expirationService.DetectAndExpireAsync(now).AsTask().Wait();
        }
        
        
        public static DynamicEntity ToDynamicEntity(this IMyNoSqlDbEntity entity)
        {
            return entity.ToMemory().ParseDynamicEntity();
        }
        
        public static DbRow ToDbRow(this IMyNoSqlDbEntity entity, DateTime? dt = null)
        {
            dt ??= DateTime.UtcNow;
            var dynamicEntity = entity.ToMemory().ParseDynamicEntity();
            return DbRow.CreateNew(dynamicEntity, dt.Value);
        }
        
    }
}