using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Tests
{
    public static class TestUtils
    {

        public static TransactionEventAttributes GetTestEventAttributes(DataSynchronizationPeriod? syncPeriod = DataSynchronizationPeriod.Immediately)
        {
            return new TransactionEventAttributes(new List<string>{"TEST"}, 
                DataSynchronizationPeriod.Sec1,
                EventSource.ClientRequest,
                new Dictionary<string, string>()
                );
        }
        

        public static ServiceProvider GetTestIoc()
        {
            var result = new ServiceCollection();
            result.BindDomainsServices();
            result.AddSingleton<ITablePersistenceStorage>(new SnapshotStorageMock());
            

            return result.BuildServiceProvider();
        }


        public static IMyMemory ToMemory(this object src)
        {
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(src);
            return new MyMemoryAsByteArray(Encoding.UTF8.GetBytes(json));
        }

        public static T AsResult<T>(this DbRow dbRow)
        {
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(dbRow.Data));
        }

        public static void InsertTestEntity(this DbTable dbTable, IMyNoSqlDbEntity insertEntity)
        {
            var mem = insertEntity.ToMemory();

            var entity = mem.ParseDynamicEntity();
            var dbRow = DbRow.CreateNew(entity, DateTime.UtcNow);

            dbTable.GetWriteAccess(writeAccess =>
            {
                var partition = writeAccess.GetOrCreatePartition(dbRow.PartitionKey);
                partition.InsertOrReplace(dbRow);
            });
            

        }
        
    }

    public class SnapshotStorageMock : ITablePersistenceStorage
    {
        public ValueTask SaveTableAttributesAsync(DbTable dbTable, UpdateTableAttributesTransactionEvent data)
        {
            return new ValueTask();
        }

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable, InitTableTransactionEvent data)
        {
            return new ValueTask();
        }

        public ValueTask SavePartitionSnapshotAsync(DbTable dbTable, InitPartitionsTransactionEvent data)
        {
            return new ValueTask();
        }

        public ValueTask SaveRowUpdatesAsync(DbTable dbTable, UpdateRowsTransactionEvent eventData)
        {
            return new ValueTask();
        }

        public ValueTask SaveRowDeletesAsync(DbTable dbTable, DeleteRowsTransactionEvent eventData)
        {
            return new ValueTask();
        }

        public ValueTask FlushIfNeededAsync()
        {
            return new ValueTask();
        }

        public IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {
            return Array.Empty<ITableLoader>().ToAsyncEnumerable();
        }

        public bool HasDataAtSaveProcess => false;
    }
    

}