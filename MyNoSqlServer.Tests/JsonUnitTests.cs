using System;
using System.Text;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Json;
using NUnit.Framework;


namespace MyNoSqlServer.Tests
{
    public class JsonUnitTests
    {

        [Test]
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
                .ParseDynamicEntity()
                .UpdateTimeStamp(timeStamp);
            
            Assert.AreEqual(timeStamp, items.Raw[RowJsonUtils.TimeStampFieldName].Value.AsJsonString());

        }
        

        
    }



}