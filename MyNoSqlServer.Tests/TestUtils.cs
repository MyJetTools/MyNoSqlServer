using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.Tests
{
    public static class TestUtils
    {

        public static ServiceProvider GetTestIoc()
        {
            var result = new ServiceCollection();
            result.BindDomainsServices();
            result.AddSingleton<ISnapshotStorage>(new SnapshotStorageMock());
            
            result.AddSingleton<IReplicaSynchronizationService>(new ReplicaSynchronizationServiceMock());

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
        public ValueTask SavePartitionSnapshotAsync(PartitionSnapshot partitionSnapshot)
        {
            return new ValueTask();
        }

        public ValueTask SavePartitionSnapshotAsync(DbTable dbTable, PartitionSnapshot partitionSnapshot)
        {
            return new ValueTask();
        }

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable)
        {
            return new ValueTask();
        }

        public ValueTask DeleteTablePartitionAsync(DbTable dbTable, string partitionKey)
        {
            return new ValueTask();
        }

        public async IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {
            foreach (var itm in Array.Empty<ITableLoader>())
            {
                yield return itm;
            }
        }

        public ValueTask SetTableSavableAsync(DbTable dbTable, bool savable)
        {
            return new ValueTask();
        }
    }


    public class ReplicaSynchronizationServiceMock : IReplicaSynchronizationService
    {
        public void PublishInitTable(DbTable dbTable)
        {
            
        }

        public void PublishInitPartition(DbTable dbTable, DbPartition partition)
        {
        }

        public void SynchronizeUpdate(DbTable dbTable, IReadOnlyList<DbRow> dbRow)
        {
        }

        public void SynchronizeDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
        }
    }

}