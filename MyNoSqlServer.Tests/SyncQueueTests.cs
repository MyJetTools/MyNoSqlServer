using System;
using System.Collections.Generic;
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
    }
}