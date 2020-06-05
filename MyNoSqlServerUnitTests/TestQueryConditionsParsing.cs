using System.Linq;
using MyNoSqlServer.Domains.Query;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    public class TestQueryConditionsParsing
    {

        [Fact]
        public void TestSimpleQueryCondition()
        {

            var query = "MyField eq 'My string'";

            var conditions = query.ParseQueryConditions().ToArray();

            Assert.Single(conditions);
            Assert.Equal("MyField", conditions[0].FieldName);
            Assert.Equal(QueryOperation.Eq, conditions[0].Operation);
            Assert.Equal("My string", conditions[0].AsString(0));

        }
        
        [Fact]
        public void TestQueryConditionWithOneAnd()
        {

            var query = "PartitionKey eq 'My string' and RowKey ge '45' and RowKey lt 50 and RowKey in ['15',16, '18' ]";

            var conditions = query.ParseQueryConditions().ToArray();

            Assert.Equal(4, conditions.Length);
            Assert.Equal("PartitionKey", conditions[0].FieldName);
            Assert.Equal(QueryOperation.Eq, conditions[0].Operation);
            Assert.Equal("My string", conditions[0].AsString(0));

            Assert.Equal("RowKey", conditions[1].FieldName);
            Assert.Equal(QueryOperation.Ge, conditions[1].Operation);
            Assert.Equal("45", conditions[1].AsString(0));

            Assert.Equal("RowKey", conditions[2].FieldName);
            Assert.Equal(QueryOperation.Lt, conditions[2].Operation);
            Assert.Equal("50", conditions[2].AsString(0));
            
            Assert.Equal("RowKey", conditions[3].FieldName);
            Assert.Equal(QueryOperation.In, conditions[3].Operation);

            Assert.Equal("15", conditions[3].AsString(0));
            Assert.Equal("16", conditions[3].AsString(1));
            Assert.Equal("18", conditions[3].AsString(2));

        }
        
    }
}