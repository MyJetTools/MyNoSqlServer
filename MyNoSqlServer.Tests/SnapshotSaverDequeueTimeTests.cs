using System;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.SnapshotSaver.Implementation;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class SnapshotSaverDequeueTimeTests
    {


        [Test]
        public void Test5Sec()
        {
            var db = new DateTime(2020, 1, 1, 12, 0, 0);
            
            var resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec5);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 5), resultDate);
            
            db = new DateTime(2020, 1, 1, 12, 0, 4);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec5);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 5), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 5);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec5);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 10), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 54);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec5);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 55), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 55);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec5);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);
        }
        
        [Test]
        public void Test15Sec()
        {
            var db = new DateTime(2020, 1, 1, 12, 0, 0);
            
            var resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec15);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 15), resultDate);
            
            db = new DateTime(2020, 1, 1, 12, 0, 14);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec15);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 15), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 15);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec15);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 30), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 44);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec15);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 45), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 45);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec15);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);
        }     
        
        [Test]
        public void Test30Sec()
        {
            var db = new DateTime(2020, 1, 1, 12, 0, 0);
            
            var resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec30);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 30), resultDate);
            
            db = new DateTime(2020, 1, 1, 12, 0, 29);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec30);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 0, 30), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 30);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec30);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 45);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec30);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 59);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Sec30);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);
        }  
        
        
        [Test]
        public void Test1Min()
        {
            var db = new DateTime(2020, 1, 1, 12, 0, 0);
            
            var resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Min1);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);
            
            db = new DateTime(2020, 1, 1, 12, 0, 29);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Min1);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 30);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Min1);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 45);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Min1);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);

            db = new DateTime(2020, 1, 1, 12, 0, 59);
            resultDate = db.GetDequeueTime(DataSynchronizationPeriod.Min1);
            Assert.AreEqual(new DateTime(2020, 1, 1, 12, 1, 0), resultDate);
        }  
        
    }
}