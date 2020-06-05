using System;
using System.Linq;
using System.Text;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Rows;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    public class JsonUnitTests
    {


        [Fact]
        public void CheckReadingKeysWhileInsertingTimestamp()
        {
            var fieldA = "\"Fiel\\\"dA\"";
            var valueA = "\"ValueA\"";
            var fieldB = "\"PartitionKey\"";
            var valueB = "\"ABC\"";
            
            var fieldC = "\"FieldC\"";
            var valueC = "[{\"A\":[1,2,3]},{\"B\":\"[\"}]";

            var fieldD = "\"FieldD\"";
            var valueD = "-5";

            
            var example = "{"+
                          $"{fieldA}:{valueA},{fieldB}:{valueB},{fieldC}:{valueC},{fieldD}:{valueD}"+
                          "}";

            var bytes = Encoding.UTF8.GetBytes(example).AsMyMemory();


            var timeStamp = DateTime.UtcNow.ToTimeStampString();
            
            var items = bytes
                .ParseFirstLevelOfJson()
                .InjectTimeStamp(timeStamp)
                .ToDictionary(itm => itm.Field.AsJsonString());
            
            Assert.Equal(timeStamp, items[DbRowDataUtils.TimeStampField].Value.AsJsonString());

        }
        

        
    }



}