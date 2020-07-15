using System;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using NUnit.Framework;
using System.Threading.Tasks;
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
        public async Task TestOkReplace()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetService<DbOperations>();

            var dbInstance = ioc.GetService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable");

            var rawClass = new TestReplaceEntity
            {
                PartitionKey = "test",
                RowKey = "test",
                Value = "123"
            };

            var dt = DateTime.UtcNow;

            await dbOperations.InsertAsync(table, rawClass.ToMemory(), DataSynchronizationPeriod.Sec1, dt);
            
            rawClass = table.GetEntity("test", "test").AsResult<TestReplaceEntity>();

            rawClass.Value = "456";
            
            dt = DateTime.UtcNow.AddSeconds(1);
            
            var opResult = await dbOperations.ReplaceAsync(table, rawClass.ToMemory(), DataSynchronizationPeriod.Immediately, dt);
            
            Assert.AreEqual(OperationResult.Ok, opResult);

            var result = table.GetEntity("test", "test").AsResult<TestReplaceEntity>();

            Assert.AreEqual("456", result.Value);
            Assert.AreEqual(dt.ToTimeStampString(), result.TimeStamp);
        }
        
        [Test]
        public async Task TestConflictReplace()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetService<DbOperations>();

            var dbInstance = ioc.GetService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable");

            var rawClass = new TestReplaceEntity
            {
                PartitionKey = "test",
                RowKey = "test",
                TimeStamp = "12:00:00",
                Value = "123"
            };

            var memory = rawClass.ToMemory();

            await dbOperations.InsertAsync(table, memory, DataSynchronizationPeriod.Sec1, DateTime.UtcNow);
            
            rawClass = table.GetEntity("test", "test").AsResult<TestReplaceEntity>();

            rawClass.Value = "456";
            rawClass.TimeStamp = "111";
            
            memory = rawClass.ToMemory();

            var opResult = await dbOperations.ReplaceAsync(table, memory, DataSynchronizationPeriod.Immediately, DateTime.UtcNow);
            
            Assert.AreEqual(OperationResult.RecordChangedConcurrently, opResult);

        }
        
    }
}