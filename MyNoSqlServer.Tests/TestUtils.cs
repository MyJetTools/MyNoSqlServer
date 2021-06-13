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
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Tests
{
    public static class TestUtils
    {

        public static TransactionEventAttributes GetTestEventAttributes(DataSynchronizationPeriod? syncPeriod = DataSynchronizationPeriod.Immediately)
        {
            return new TransactionEventAttributes("TEST", 
                DataSynchronizationPeriod.Sec1,
                new Dictionary<string, string>());
        }
        

        public static ServiceProvider GetTestIoc()
        {
            var result = new ServiceCollection();
            result.BindDomainsServices();
            result.AddSingleton<ISnapshotStorage>(new SnapshotStorageMock());
            

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
        
    }

    public class SnapshotStorageMock : ISnapshotStorage
    {
        public ValueTask SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot, Dictionary<string, string> headers)
        {
            return new ValueTask();
        }

        public ValueTask SavePartitionSnapshotAsync(DbTable dbTable, PartitionSnapshot partitionSnapshot, Dictionary<string, string> headers)
        {
            return new ValueTask();
        }

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable, Dictionary<string, string> headers)
        {
            return new ValueTask();
        }

        public ValueTask DeleteTablePartitionAsync(DbTable dbTable, string partitionKey, Dictionary<string, string> headers)
        {
            return new ValueTask();
        }

        public IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {
            return Array.Empty<ITableLoader>().ToAsyncEnumerable();
        }

        public ValueTask SetTableAttributesAsync(DbTable dbTable, Dictionary<string, string> headers)
        {
            return new ValueTask();
        }

    }

}