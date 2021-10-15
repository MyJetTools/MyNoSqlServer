using System;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains;
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

            var table = dbOperations.CreateTableIfNotExists("mytable",
                false,  0,TestUtils.GetTestEventAttributes());

            var now = DateTime.UtcNow;

            var result = dbOperations.Insert(table, insertEntity.ToMemory(), now, TestUtils.GetTestEventAttributes());
            
            Assert.AreEqual(OperationResult.Ok, result);

            var resultEntity = table.GetEntity(insertEntity.PartitionKey, insertEntity.RowKey).AsResult<InsertEntity>();
            
            Assert.AreEqual(insertEntity.PartitionKey, resultEntity.PartitionKey);
            Assert.AreEqual(insertEntity.RowKey, resultEntity.RowKey);
            Assert.AreEqual(insertEntity.Value, resultEntity.Value);
            Assert.AreEqual(now.ToTimeStampString(), resultEntity.TimeStamp);

        }
        
    }
}