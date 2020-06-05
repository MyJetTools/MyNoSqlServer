using System.Linq;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Query;
using Xunit;

namespace MyNoSqlServerUnitTests
{

    public class TestRecord : MyNoSqlDbEntity
    {
        public string TestField { get; set; }
    }
    
    public class TestQueryConditions
    {

        [Fact]
        public void TestSimpleQuery()
        {
            var dbTable = DbTable.CreateByRequest("myTable");

            var recordToInsert = new TestRecord
            {
                PartitionKey = "MyPartition",
                RowKey = "MyRow",
                TestField = "Test"
            };

            var recordIsByteArray = recordToInsert.AsJsonByteArray();

            var fields = recordIsByteArray.AsMyMemory().ParseFirstLevelOfJson();

            var entityInfo = fields.GetEntityInfo();
            
            dbTable.Insert(entityInfo, fields);

            var query = "PartitionKey eq 'MyPartition' and RowKey eq 'MyRow'";

            var queryCondition = query.ParseQueryConditions();
            
            var foundItems = dbTable.ApplyQuery(queryCondition).ToArray();

            Assert.Equal(recordToInsert.TestField, foundItems.First().GetValue("TestField"));

        }
        
        [Fact]
        public void TestSimpleRangeQuery()
        {
            var dbTable = DbTable.CreateByRequest("myTable");

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

                var fields = recordIsByteArray.ParseFirstLevelOfJson();
            
                dbTable.Insert(fields.GetEntityInfo(), fields);
            }

            var query = "PartitionKey eq 'MyPartition' and RowKey ge '001' and RowKey le '003'";

            var queryCondition = query.ParseQueryConditions();
            
            var foundRecords = dbTable.ApplyQuery(queryCondition).ToArray();

            Assert.Single(foundRecords);

            Assert.Equal("002", foundRecords[0].GetValue("TestField"));

        }
        
        [Fact]
        public void TestSimpleRangeAboveQuery()
        {
            var dbTable = DbTable.CreateByRequest("myTable");

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

                var fields = recordIsByteArray.ParseFirstLevelOfJson();
            
                dbTable.Insert(fields.GetEntityInfo(), fields);
            }

            var query = "PartitionKey eq 'MyPartition' and RowKey ge '199'";

            var queryCondition = query.ParseQueryConditions();
            
            var foundRecords = dbTable.ApplyQuery(queryCondition).ToArray();

            Assert.Single(foundRecords);

            Assert.Equal("200", foundRecords[0].GetValue("TestField"));
        } 
        
    }
    
}