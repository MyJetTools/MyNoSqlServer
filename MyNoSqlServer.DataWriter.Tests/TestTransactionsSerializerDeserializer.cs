using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.DataWriter.Builders;
using MyNoSqlServer.Domains.Transactions;
using NUnit.Framework;

namespace MyNoSqlServer.DataWriter.Tests
{
    public class TestTransactionsSerializerDeserializer
    {

        public class TestEntity : MyNoSqlDbEntity
        {
            
            public string MyField { get; set; }
            
        }

        [Test]
        public void TestAllObjects()
        {
            var serializer = new TransactionDataSerializer<TestEntity>();
            
            serializer.CleanTable();
            serializer.DeletePartitions(new []{"DeletePartition1","DeletePartition2"});
            
            serializer.DeleteRows( "DeletePartitionKey",new []{"DeleteRow1","DeleteRow2"});


            var entities = new List<TestEntity>()
            {
                new TestEntity
                {
                    PartitionKey = "PK1",
                    RowKey = "RK1",
                    MyField = "1"
                },
                
                new TestEntity
                {
                    PartitionKey = "PK2",
                    RowKey = "RK2",
                    MyField = "2"
                },

            };

            
            serializer.InsertOrReplace(entities);

            var json = serializer.Serialize();

            Console.WriteLine(json);
            
            var memory = new MyMemoryAsByteArray(Encoding.UTF8.GetBytes(json));
            
            
            var transactions = DbTransactionsJsonDeserializer.GetTransactions(memory).ToArray();
            
            
            Assert.AreEqual(4,  transactions.Length);
            
            Assert.IsTrue(transactions[0] is ICleanTableTransaction);

            var clearPartitionsTransaction = (ICleanPartitionsTransaction) transactions[1];
            Assert.AreEqual(2,  clearPartitionsTransaction.Partitions.Length);
            Assert.AreEqual("DeletePartition1",  clearPartitionsTransaction.Partitions[0]);
            Assert.AreEqual("DeletePartition2",  clearPartitionsTransaction.Partitions[1]);
            
            
            
            var deleteRowsTransaction = (IDeleteRowsTransaction) transactions[2];
            Assert.AreEqual("DeletePartitionKey",  deleteRowsTransaction.PartitionKey);
            Assert.AreEqual(2,  deleteRowsTransaction.RowKeys.Length);
            Assert.AreEqual("DeleteRow1",  deleteRowsTransaction.RowKeys[0]);
            Assert.AreEqual("DeleteRow2",  deleteRowsTransaction.RowKeys[1]);
            
            
            
            var entitiesTransaction = (IInsertOrReplaceEntitiesTransaction) transactions[3];
            Assert.AreEqual(2,  entitiesTransaction.Entities.Count);
            Assert.AreEqual("PK1",  entitiesTransaction.Entities[0].PartitionKey);
            Assert.AreEqual("RK1",  entitiesTransaction.Entities[0].RowKey);
            Assert.IsTrue(entitiesTransaction.Entities[0].Raw.ContainsKey("MyField"));

            
            Assert.AreEqual("PK2",  entitiesTransaction.Entities[1].PartitionKey);
            Assert.AreEqual("RK2",  entitiesTransaction.Entities[1].RowKey);
            Assert.IsTrue(entitiesTransaction.Entities[1].Raw.ContainsKey("MyField"));
        }
    }
}