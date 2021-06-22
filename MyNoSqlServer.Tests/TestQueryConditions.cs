using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Query;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{

    public class TestRecord : MyNoSqlDbEntity
    {
        public string TestField { get; set; }
    }
    
    public class TestQueryConditions
    {

        [Test]
        public void TestSimpleQuery()
        {
            
            
            var dbTable = new DbTable("myTable", false, 0);

            var recordToInsert = new TestRecord
            {
                PartitionKey = "MyPartition",
                RowKey = "MyRow",
                TestField = "Test"
            };

            var recordIsByteArray = recordToInsert.AsJsonByteArray();

            var dynEntity = recordIsByteArray.AsMyMemory().ParseDynamicEntity();
            
            dbTable.Insert(dynEntity);

            var query = "PartitionKey eq 'MyPartition' and RowKey eq 'MyRow'";

            var queryCondition = query.ParseQueryConditions();
            
            var foundItems = dbTable.ApplyQuery(queryCondition).ToArray();

            Assert.AreEqual(recordToInsert.TestField, foundItems.First().GetValue("TestField"));

        }
        
        [Test]
        public void TestSimpleRangeQuery()
        {
            var dbTable = new DbTable("myTable", false, 0);

            for (var i = 0; i < 100; i++)
            {
                var key = (i * 2).ToString("000");
                var recordToInsert = new TestRecord
                {
                    PartitionKey = "MyPartition",
                    RowKey = key,
                    TestField = key
                };
                
                var recordIsByteArray = recordToInsert.AsJsonByteArray().AsMyMemory();

                var entity = recordIsByteArray.ParseDynamicEntity();
            
                dbTable.Insert(entity);
            }

            var query = "PartitionKey eq 'MyPartition' and RowKey ge '001' and RowKey le '003'";

            var queryCondition = query.ParseQueryConditions();
            
            var foundRecords = dbTable.ApplyQuery(queryCondition).ToArray();

            Assert.AreEqual(1, foundRecords.Length);

            Assert.AreEqual("002", foundRecords[0].GetValue("TestField"));

        }
        
        [Test]
        public void TestSimpleRangeAboveQuery()
        {
            var dbTable = new DbTable("myTable", false, 0);

            for (var i = 0; i <= 100; i++)
            {
                var key = (i * 2).ToString("000");
                var recordToInsert = new TestRecord
                {
                    PartitionKey = "MyPartition",
                    RowKey = key,
                    TestField = key
                };
                
                var recordIsByteArray = recordToInsert.AsJsonByteArray().AsMyMemory();

                var entity = recordIsByteArray.ParseDynamicEntity();
            
                dbTable.Insert(entity);
            }

            var query = "PartitionKey eq 'MyPartition' and RowKey ge '199'";

            var queryCondition = query.ParseQueryConditions();
            
            var foundRecords = dbTable.ApplyQuery(queryCondition).ToArray();

            Assert.AreEqual(1, foundRecords.Length);

            Assert.AreEqual("200", foundRecords[0].GetValue("TestField"));
        } 
        
    }
    
}