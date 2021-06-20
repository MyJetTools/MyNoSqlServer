using System;
using System.Collections.Generic;
using System.Diagnostics;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Nodes;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    
    
    public class TestDbRowsSerialization
    {
        
        public class InsertEntity: MyNoSqlDbEntity
        {
            public string Value { get; set; }
        }


        [Test]
        public void TestDictionarySerialization()
        {

            var row1 = new InsertEntity
            {
                PartitionKey = "Pk1",
                RowKey = "Rk1",
                Value = "1"
            };


            var row2 = new InsertEntity
            {
                PartitionKey = "Pk1",
                RowKey = "Rk2",
                Value = "2"
            };


            var dictionary = new Dictionary<string, IReadOnlyList<DbRow>>
            {
                [row1.PartitionKey] = new[]
                {
                    DbRow.CreateNew(row1.ToMemory().ParseDynamicEntity(), DateTime.UtcNow),
                    DbRow.CreateNew(row2.ToMemory().ParseDynamicEntity(), DateTime.UtcNow)
                }
            };
            var sw = new Stopwatch();


            sw.Start();
            var jsonDictionary = dictionary.SerializeProtobufPartitionsData();
            sw.Stop();

            Console.WriteLine($"Duration seialization: {sw.Elapsed}");

            sw.Reset();
            sw.Start();
            var result = jsonDictionary.DeserializeProtobufPartitionsData();
            sw.Stop();

            Console.WriteLine($"Duration deserialization: {sw.Elapsed}");


            Assert.AreEqual(1, result.Count);

            Assert.AreEqual(2, result["Pk1"].Count);



        }

    }
}