using System.Linq;
using MyNoSqlServer.Domains.Query;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestQueryConditionsParsing
    {

        [Test]
        public void TestSimpleQueryCondition()
        {
            var query = "MyField eq 'My string'";

            var conditions = query.ParseQueryConditions().ToArray();

            Assert.AreEqual(1, conditions.Length);
            Assert.AreEqual("MyField", conditions[0].FieldName);
            Assert.AreEqual(QueryOperation.Eq, conditions[0].Operation);
            Assert.AreEqual("My string", conditions[0].AsString(0));
        }
        
        [Test]
        public void TestQueryConditionWithOneAnd()
        {

            var query = "PartitionKey eq 'My string' and RowKey ge '45' and RowKey lt 50 and RowKey in ['15',16, '18' ]";

            var conditions = query.ParseQueryConditions().ToArray();

            Assert.AreEqual(4, conditions.Length);
            Assert.AreEqual("PartitionKey", conditions[0].FieldName);
            Assert.AreEqual(QueryOperation.Eq, conditions[0].Operation);
            Assert.AreEqual("My string", conditions[0].AsString(0));

            Assert.AreEqual("RowKey", conditions[1].FieldName);
            Assert.AreEqual(QueryOperation.Ge, conditions[1].Operation);
            Assert.AreEqual("45", conditions[1].AsString(0));

            Assert.AreEqual("RowKey", conditions[2].FieldName);
            Assert.AreEqual(QueryOperation.Lt, conditions[2].Operation);
            Assert.AreEqual("50", conditions[2].AsString(0));
            
            Assert.AreEqual("RowKey", conditions[3].FieldName);
            Assert.AreEqual(QueryOperation.In, conditions[3].Operation);

            Assert.AreEqual("15", conditions[3].AsString(0));
            Assert.AreEqual("16", conditions[3].AsString(1));
            Assert.AreEqual("18", conditions[3].AsString(2));

        }
        
    }
}