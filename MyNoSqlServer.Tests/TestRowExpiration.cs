using System;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestRowExpiration
    {


        [Test]
        public async Task TestBasicExpiration()
        {
            var ioc = TestUtils.GetTestIoc();

            var dt = DateTime.UtcNow;

            var entity = new TestEntity
            {
                PartitionKey = "testPK",
                RowKey = "testPK",
                Expires = dt.AddSeconds(2)
            };

            var dbTable = ioc.CreateTable("test");

            await ioc.InsertToTableAsync(dbTable, entity, dt);
            var dbRow =  dbTable.TryGetRow("testPK", "testPK");
            
            Assert.IsNotNull(dbRow);
            
            await ioc.ExpirationTimerTickAsync(dt);
            
            dbRow =  dbTable.TryGetRow("testPK", "testPK");
            
            Assert.IsNotNull(dbRow);
                        
            await ioc.ExpirationTimerTickAsync(dt.AddSeconds(3));
            
            dbRow =  dbTable.TryGetRow("testPK", "testPK");
            
            Assert.IsNull(dbRow);
        }
    }
}