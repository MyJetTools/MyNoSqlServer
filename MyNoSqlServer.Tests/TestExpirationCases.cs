using System;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Operations;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestExpirationCases
    {
        
        
        [Test]
        public async Task TestExpireFieldWorks()
        {
            var ioc = TestUtils.GetTestIoc();

            var table = ioc.CreateTable("myTable");


            var expires = DateTime.Parse("2020-01-01T00:00:00");
            var testEntity = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                Expires = expires,
                Value = "TEST"
            };
            
            await ioc.InsertToTableAsync(table, testEntity);

            var resultEntity = table.TryGetRow("PK", "RK");
            
            Assert.AreEqual(expires, resultEntity.Expires);
        }
        
        
        [Test]
        public void TestEntityIsExpiredCase()
        {
            /*
            var ioc = TestUtils.GetTestIoc();

            var table = ioc.CreateTable("myTable");


            var expires = DateTime.Parse("2020-01-01T00:00:00");
            var testEntity = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                Expires = expires,
                Value = "TEST"
            };
            
            ioc.InsertToTable(table, testEntity);

            var resultEntity = table.TryGetRow("PK", "RK");
            
            Assert.IsNotNull(resultEntity);
            
            ioc.ExpirationTimerTick(expires);
            
            resultEntity = table.TryGetRow("PK", "RK");
            
            Assert.IsNull(resultEntity);
        */
        }
        
        
        
    }
}