using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using NUnit.Framework;

namespace MyNoSqlServer.DataReader.Tests
{
    internal class TestEntity : IMyNoSqlDbEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string TimeStamp { get; set; }
        public DateTime? Expires { get; set; }
    }
    
    public class TestDataReaderPartitionDifferenceDetection
    {


        [Test]
        public void TestDetectAddElement()
        {
            var oldCache = new DataReaderPartition<TestEntity>();
            var newCache = new DataReaderPartition<TestEntity>();
            var newElement = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = "0"
            };
            newCache.Update(newElement);

            var updated = new List<TestEntity>();
            var deleted = new List<TestEntity>();


            oldCache.FindDifference(newCache,
                itm => updated.Add(itm),
                itm => deleted.Add(itm));
            
            Assert.AreEqual(1, updated.Count);
            Assert.AreEqual(0, deleted.Count);
        }
        
        [Test]
        public void TestDetectDeleteElementAsNewCacheIsEmpty()
        {
            var oldCache = new DataReaderPartition<TestEntity>();
            var newCache = new DataReaderPartition<TestEntity>();
            var newElement = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = "0"
            };
            oldCache.Update(newElement);

            var updated = new List<TestEntity>();
            var deleted = new List<TestEntity>();


            oldCache.FindDifference(newCache,
                itm => updated.Add(itm),
                itm => deleted.Add(itm));
            
            Assert.AreEqual(0, updated.Count);
            Assert.AreEqual(1, deleted.Count);
        }   
        
        
        
        [Test]
        public void TestNothingChanges()
        {
            var oldCache = new DataReaderPartition<TestEntity>();
            var newCache = new DataReaderPartition<TestEntity>();
            var newElement = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = "0"
            };
            oldCache.Update(newElement);
            newCache.Update(newElement);

            var updated = new List<TestEntity>();
            var deleted = new List<TestEntity>();


            oldCache.FindDifference(newCache,
                itm => updated.Add(itm),
                itm => deleted.Add(itm));
            
            Assert.AreEqual(0, updated.Count);
            Assert.AreEqual(0, deleted.Count);
        }
        
        
        [Test]
        public void TestComplexUpdateDeleteChanges()
        {
            var oldCache = new DataReaderPartition<TestEntity>();
            var newCache = new DataReaderPartition<TestEntity>();
            var element1 = new TestEntity
            {
                PartitionKey = "PK1",
                RowKey = "RK1",
                TimeStamp = "1"
            };
            var element2 = new TestEntity
            {
                PartitionKey = "PK2",
                RowKey = "RK2",
                TimeStamp = "2"
            };    
            
            var element3 = new TestEntity
            {
                PartitionKey = "PK3",
                RowKey = "RK3",
                TimeStamp = "3"
            }; 
            
            
            oldCache.Update(element1);
            oldCache.Update(element2);
            
            newCache.Update(element2);
            newCache.Update(element3);

            var updated = new List<TestEntity>();
            var deleted = new List<TestEntity>();


            oldCache.FindDifference(newCache,
                itm => updated.Add(itm),
                itm => deleted.Add(itm));
            
            Assert.AreEqual(1, updated.Count);
            Assert.AreEqual(1, deleted.Count);
            Assert.AreEqual("PK3", updated[0].PartitionKey);
            Assert.AreEqual("PK1", deleted[0].PartitionKey);
        }   
        
    }



}