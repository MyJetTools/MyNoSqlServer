using System;
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
        public void TestOkMerge()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false, TestUtils.GetTestEventAttributes());

            var rawClass1 = new MergeEntity1
            {
                PartitionKey = "test",
                RowKey = "test",
                TimeStamp = "12:00:00",
                Value1 = "123"
            };

            var memory1 = rawClass1.ToMemory();

            dbOperations.Insert(table, 
                memory1,
                DateTime.UtcNow, 
                TestUtils.GetTestEventAttributes());

            var insertedEntity = table
                .GetEntity("test", "test")
                .AsResult<MergeEntity2>();

            insertedEntity.Value2 = "456";
            
            var memory2 = insertedEntity.ToMemory();

            var opResult = dbOperations
                .Merge(table,
                    memory2,
                    DateTime.UtcNow, 
                    TestUtils.GetTestEventAttributes());
            
            Assert.AreEqual(OperationResult.Ok, opResult);

            var result = table.GetEntity("test", "test").AsResult<MergedEntity>();

            Assert.AreEqual("123", result.Value1);
            Assert.AreEqual("456", result.Value2);
        }
        
        [Test]
        public void TestConcurrentNotOkMerge()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false, 
                TestUtils.GetTestEventAttributes());

            var rawClass1 = new MergeEntity1
            {
                PartitionKey = "test",
                RowKey = "test",
                TimeStamp = "12:00:00",
                Value1 = "123"
            };

            var memory1 = rawClass1.ToMemory();

            dbOperations.Insert(table, memory1,  DateTime.UtcNow, 
                TestUtils.GetTestEventAttributes());

            var insertedEntity1 = table.GetEntity("test", "test").AsResult<MergeEntity2>();

            insertedEntity1.Value2 = "456";
            
            var insertedEntity2 = table.GetEntity("test", "test").AsResult<MergeEntity2>();

            insertedEntity2.Value2 = "789";

            var mergeDateTime1 = DateTime.UtcNow.AddSeconds(1);

            var opResult1 = dbOperations.Merge(table, insertedEntity1.ToMemory(), 
                mergeDateTime1, 
                TestUtils.GetTestEventAttributes());

            var entity = table.GetEntity(rawClass1.PartitionKey, rawClass1.RowKey);
            
             Assert.AreEqual(mergeDateTime1.ToTimeStampString(), entity.TimeStamp);
            
            var mergeDateTime2 = DateTime.UtcNow.AddSeconds(1);
            var opResult2 = dbOperations.Merge(table, insertedEntity2.ToMemory(), 
                 mergeDateTime2, 
                 TestUtils.GetTestEventAttributes());

            Assert.AreEqual(OperationResult.Ok, opResult1);
            Assert.AreEqual(OperationResult.RecordChangedConcurrently, opResult2);

            var result = table.GetEntity("test", "test").AsResult<MergedEntity>();

            Assert.AreEqual("123", result.Value1);
            Assert.AreEqual("456", result.Value2);
        }        
    }
}