using System;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using NUnit.Framework;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Tests
{

    public class TestReplaceEntity : IMyNoSqlDbEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string TimeStamp { get; set; }
        public DateTime? Expires { get; set; }
        public string Value { get; set; }
    }
    
    
    public class TestReplaceCases
    {

        [Test]
        public void TestOkReplace()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false);

            var rawClass = new TestReplaceEntity
            {
                PartitionKey = "test",
                RowKey = "test",
                Value = "123"
            };

            var dt = DateTime.UtcNow;

            dbOperations.Insert(table, rawClass.ToMemory(), DataSynchronizationPeriod.Sec1, dt);
            
            rawClass = table.GetEntity("test", "test").AsResult<TestReplaceEntity>();

            rawClass.Value = "456";
            
            dt = DateTime.UtcNow.AddSeconds(1);
            
            var opResult = dbOperations.Replace(table, rawClass.ToMemory(), DataSynchronizationPeriod.Immediately, dt);
            
            Assert.AreEqual(OperationResult.Ok, opResult);

            var result = table.GetEntity("test", "test").AsResult<TestReplaceEntity>();

            Assert.AreEqual("456", result.Value);
            Assert.AreEqual(dt.ToTimeStampString(), result.TimeStamp);
        }
        
        [Test]
        public void TestConflictReplace()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false);

            var rawClass = new TestReplaceEntity
            {
                PartitionKey = "test",
                RowKey = "test",
                TimeStamp = "12:00:00",
                Value = "123"
            };

            var memory = rawClass.ToMemory();

            dbOperations.Insert(table, memory, DataSynchronizationPeriod.Sec1, DateTime.UtcNow);
            
            rawClass = table.GetEntity("test", "test").AsResult<TestReplaceEntity>();

            rawClass.Value = "456";
            rawClass.TimeStamp = "111";
            
            memory = rawClass.ToMemory();

            var opResult = dbOperations.Replace(table, memory, DataSynchronizationPeriod.Immediately, DateTime.UtcNow);
            
            Assert.AreEqual(OperationResult.RecordChangedConcurrently, opResult);

        }
        
    }
}