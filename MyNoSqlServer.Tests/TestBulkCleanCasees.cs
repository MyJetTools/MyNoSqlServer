using System;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestBulkCleanCases
    {
        [Test]
        public void TestCleanAndKeepLastRecords()
        {
            var ioc = TestUtils.GetTestIoc();

            var dbOperations =  ioc.GetRequiredService<DbOperations>();

            var dbInstance = ioc.GetRequiredService<DbInstance>();

            var table = dbInstance.CreateTableIfNotExists("mytable", false);

            for (var i = 0; i < 10; i++)
            {
                var rawClass = new TestReplaceEntity
                {
                    PartitionKey = "test",
                    RowKey = i.ToString("00"),
                    Value = "123"
                };  
                
                var dt = DateTime.UtcNow;

                dbOperations.Insert(table, rawClass.ToMemory(), DataSynchronizationPeriod.Sec1, dt);
            }

            var records = table.GetRecords("test", null ,null);
            
            
            Assert.AreEqual(10, records.Count);
            
            var (_, dbRows) = table.CleanAndKeepLastRecords("test", 2);


            var resultRecords = table.GetRecords("test", null, null);
            
            Assert.AreEqual(2, resultRecords.Count);

            for (var i = 0; i < 8; i++)
            {
                var deletedRow = dbRows[i];
                Assert.AreEqual(i.ToString("00"), deletedRow.RowKey);
            }

        }

    }
}