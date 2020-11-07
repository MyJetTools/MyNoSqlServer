using System;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.Json;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestInsertCases
    {

        public class InsertEntity: MyNoSqlDbEntity
        {
            public string Value { get; set; }
        }

        [Test]
        public async Task TestInsert()
        {

            var insertEntity = new InsertEntity
            {
                PartitionKey = "PartitionKey",
                RowKey = "rowKey",
                Value = "Value"
            };

            var ioc = TestUtils.GetTestIoc();
            
            var dbOperations =  ioc.GetService<DbTableWriteOperations>();

            var dbInstance = ioc.GetService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable");

            var now = DateTime.UtcNow;

            var result = await dbOperations.InsertAsync(table, insertEntity.ToMemory().ParseDynamicEntity(), DataSynchronizationPeriod.Asap, now);
            
            Assert.AreEqual(OperationResult.Ok, result);

            var resultEntity = table.TryGetRow(insertEntity.PartitionKey, insertEntity.RowKey).AsResult<InsertEntity>();
            
            Assert.AreEqual(insertEntity.PartitionKey, resultEntity.PartitionKey);
            Assert.AreEqual(insertEntity.RowKey, resultEntity.RowKey);
            Assert.AreEqual(insertEntity.Value, resultEntity.Value);
            Assert.AreEqual(now.ToTimeStampString(), resultEntity.TimeStamp);

        }
        
    }
}