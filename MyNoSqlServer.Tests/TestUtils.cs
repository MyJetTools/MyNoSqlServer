using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using MyDependencies;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.ChangesBroadcasting;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Tests
{
    public static class TestUtils
    {

        public static MyIoc GetTestIoc()
        {
            var result = new MyIoc();
            result.BindDomainsServices();
            result.Register<ISnapshotStorage>(new SnapshotStorageMock());
            
            return result;
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

        public ValueTask SaveTableSnapshotAsync(DbTable dbTable)
        {
            return new ValueTask();
        }

        public ValueTask DeleteTablePartitionAsync(string tableName, string partitionKey)
        {
            return new ValueTask();
        }

        public IAsyncEnumerable<PartitionSnapshot> LoadSnapshotsAsync()
        {
            throw new System.NotImplementedException();
        }
    }

}