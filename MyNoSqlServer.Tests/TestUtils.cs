using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using MyDependencies;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Tests.MockServices;

namespace MyNoSqlServer.Tests
{

    public class TestEntity : MyNoSqlDbEntity
    {
        public string Value { get; set; }
    }
    
    
    public static class TestUtils
    {
        public static MyIoc GetTestIoc()
        {
            var result = new MyIoc();
            result.BindDomainsServices();
            result.Register<ISnapshotStorage>(new SnapshotStorageMock());
            result.Register<IReplicaSynchronizationService>(new ReplicaSynchronizationServiceMock());
            
            result.Register<IMetricsWriter>(new MetricsWriter());

            return result;
        }


        public static IMyMemory ToMemory(this object src)
        {
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(src);
            return new MyMemoryAsByteArray(Encoding.UTF8.GetBytes(json));
        }

        public static T AsResult<T>(this DbRow dbRow) where T:IMyNoSqlDbEntity
        {
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(dbRow.Data));
        }
        
    }



}