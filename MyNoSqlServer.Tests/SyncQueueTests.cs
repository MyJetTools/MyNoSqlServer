using System;
using MyNoSqlServer.Domains.SnapshotSaver;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{

    public class SyncPartitionMockTask : ISyncPartitionTask
    {
        public DateTime SyncDateTime { get; set; }
        public long Id { get; set; }
        public string PartitionKey { get; set; }
    }
    
    public class SyncTableMockTask : ISyncTableTask
    {
        public DateTime SyncDateTime { get; set; }
        public long Id { get; set; }
    }
    
    
    public class SyncQueueTests
    {

        [Test]
        public void TestTwoPartitionsToSync()
        {
            var time = DateTime.UtcNow;

            var syncEvents = new SyncQueueByTable();


            var syncTask1 = new SyncPartitionMockTask
            {
                Id = 1,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask1);

            var syncTask2 = new SyncPartitionMockTask
            {
                Id = 2,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask2);

            var result = syncEvents.Dequeue(time);

            Assert.IsNotNull(result);

            Assert.AreEqual(0, syncEvents.Count);
        }
        
        [Test]
        public void TestTwoPartitionsByTimeIsNotCame()
        {
            var time = DateTime.UtcNow;

            var syncEvents = new SyncQueueByTable();

            var syncTask1 = new SyncPartitionMockTask
            {
                Id = 1,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask1);

            var syncTask2 = new SyncPartitionMockTask
            {
                Id = 2,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask2);

            var result = syncEvents.Dequeue(time.AddSeconds(-1));

            Assert.IsNull(result);

            Assert.AreEqual(2, syncEvents.Count);
        }
        
        
        [Test]
        public void TestSecondEventTimeCame()
        {

            var syncEvents = new SyncQueueByTable();

            var time = DateTime.UtcNow;
            var syncTask1 = new SyncPartitionMockTask
            {
                Id = 1,
                PartitionKey = "Test",
                SyncDateTime = time.AddSeconds(1)
            };

            syncEvents.Enqueue(syncTask1);

            var syncTask2 = new SyncPartitionMockTask
            {
                Id = 2,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask2);

            var result = syncEvents.Dequeue(time);

            Assert.IsNotNull(result);

            Assert.AreEqual(0, syncEvents.Count);
        }       
        
        
        [Test]
        public void TestWithSyncTable()
        {

            var syncEvents = new SyncQueueByTable();

            var time = DateTime.UtcNow;
            var syncTask1 = new SyncPartitionMockTask
            {
                Id = 1,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask1);

            var syncTask2 = new SyncPartitionMockTask
            {
                Id = 2,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask2);
            
            var syncTask3 = new SyncPartitionMockTask
            {
                Id = 3,
                PartitionKey = "Test",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask3);

            var result = syncEvents.Dequeue(time);

            Assert.True(result is SyncPartitionMockTask);

            Assert.AreEqual(0, syncEvents.Count);
        }   
        
        
        [Test]
        public void TestDifferentPartitionsOptimization()
        {
            var time = DateTime.UtcNow;

            var syncEvents = new SyncQueueByTable();


            var syncTask1 = new SyncPartitionMockTask
            {
                Id = 1,
                PartitionKey = "PK1",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask1);

            var syncTask2 = new SyncPartitionMockTask
            {
                Id = 2,
                PartitionKey = "PK2",
                SyncDateTime = time.AddSeconds(1)
            };

            syncEvents.Enqueue(syncTask2);
            
            var syncTask3 = new SyncPartitionMockTask
            {
                Id = 3,
                PartitionKey = "PK1",
                SyncDateTime = time
            };

            syncEvents.Enqueue(syncTask3);

            var result = syncEvents.Dequeue(time);

            Assert.AreEqual("PK1", ((SyncPartitionMockTask)result).PartitionKey);

            Assert.AreEqual(1, syncEvents.Count);
            
            result = syncEvents.Dequeue(time);
            
            Assert.IsNull(result);
            Assert.AreEqual(1, syncEvents.Count);
            
            result = syncEvents.Dequeue(time.AddSeconds(1));

            Assert.AreEqual("PK2", ((SyncPartitionMockTask)result).PartitionKey);

            Assert.AreEqual(0, syncEvents.Count);
        }
        
    }
}