using System;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
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
        public void TestInsert()
        {

            var insertEntity = new InsertEntity
            {
                PartitionKey = "PartitionKey",
                RowKey = "rowKey",
                Value = "Value"
            };

            var ioc = TestUtils.GetTestIoc();
            
            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false);

            var now = DateTime.UtcNow;

            var result = dbOperations.Insert(table, insertEntity.ToMemory(), DataSynchronizationPeriod.Asap, now);
            
            Assert.AreEqual(OperationResult.Ok, result);

            var resultEntity = table.GetEntity(insertEntity.PartitionKey, insertEntity.RowKey).AsResult<InsertEntity>();
            
            Assert.AreEqual(insertEntity.PartitionKey, resultEntity.PartitionKey);
            Assert.AreEqual(insertEntity.RowKey, resultEntity.RowKey);
            Assert.AreEqual(insertEntity.Value, resultEntity.Value);
            Assert.AreEqual(now.ToTimeStampString(), resultEntity.TimeStamp);

        }
        
    }
}