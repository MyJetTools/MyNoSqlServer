using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Json;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    
    public class TestMergeCases
    {
        private class MergeEntity1 : MyNoSqlDbEntity
        {
            public string Value1 { get; set; }
        }

        private class MergeEntity2 : MyNoSqlDbEntity
        {
            public string Value2 { get; set; }
        }

        private class MergedEntity : MyNoSqlDbEntity
        {
            public string Value1 { get; set; }
            public string Value2 { get; set; }
        }
        
        [Test]
        public async Task TestOkMerge()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false);

            var rawClass1 = new MergeEntity1
            {
                PartitionKey = "test",
                RowKey = "test",
                TimeStamp = "12:00:00",
                Value1 = "123"
            };

            var memory1 = rawClass1.ToMemory();

            await dbOperations.InsertAsync(table, 
                memory1, 
                DataSynchronizationPeriod.Sec1, 
                DateTime.UtcNow);

            var insertedEntity = table
                .GetEntity("test", "test")
                .AsResult<MergeEntity2>();

            insertedEntity.Value2 = "456";
            
            var memory2 = insertedEntity.ToMemory();

            var opResult = await dbOperations
                .MergeAsync(table,
                    memory2,
                    DataSynchronizationPeriod.Immediately, 
                    DateTime.UtcNow);
            
            Assert.AreEqual(OperationResult.Ok, opResult);

            var result = table.GetEntity("test", "test").AsResult<MergedEntity>();

            Assert.AreEqual("123", result.Value1);
            Assert.AreEqual("456", result.Value2);
        }
        
        [Test]
        public async Task TestConcurrentNotOkMerge()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false);

            var rawClass1 = new MergeEntity1
            {
                PartitionKey = "test",
                RowKey = "test",
                TimeStamp = "12:00:00",
                Value1 = "123"
            };

            var memory1 = rawClass1.ToMemory();

            await dbOperations.InsertAsync(table, memory1, DataSynchronizationPeriod.Sec1, DateTime.UtcNow);

            var insertedEntity1 = table.GetEntity("test", "test").AsResult<MergeEntity2>();

            insertedEntity1.Value2 = "456";
            
            var insertedEntity2 = table.GetEntity("test", "test").AsResult<MergeEntity2>();

            insertedEntity2.Value2 = "789";

            var mergeDateTime1 = DateTime.UtcNow.AddSeconds(1);

            var opResult1 = await dbOperations.MergeAsync(table, insertedEntity1.ToMemory(), 
                DataSynchronizationPeriod.Immediately, mergeDateTime1);

            var entity = table.GetEntity(rawClass1.PartitionKey, rawClass1.RowKey);
            
             Assert.AreEqual(mergeDateTime1.ToTimeStampString(), entity.TimeStamp);
            
            var mergeDateTime2 = DateTime.UtcNow.AddSeconds(1);
            var opResult2 = await dbOperations.MergeAsync(table, insertedEntity2.ToMemory(), 
                DataSynchronizationPeriod.Immediately, mergeDateTime2);

            Assert.AreEqual(OperationResult.Ok, opResult1);
            Assert.AreEqual(OperationResult.RecordChangedConcurrently, opResult2);

            var result = table.GetEntity("test", "test").AsResult<MergedEntity>();

            Assert.AreEqual("123", result.Value1);
            Assert.AreEqual("456", result.Value2);
        }        
    }
}