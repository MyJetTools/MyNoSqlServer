using System;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestExpirationIndexMaintenance
    {

        private class TestEntity : MyNoSqlDbEntity
        {
            
        }
        
        
        [Test]
        public void TestIfWeCreateNonExpiredDbRow()
        {
            
            var dbPartition = DbPartition.Create("TestPartition");

            var testEntity = new TestEntity
            {
                PartitionKey = dbPartition.PartitionKey,
                RowKey = "RK"
            };

            var dbRow = testEntity.ToDbRow();

            dbPartition.Insert(dbRow, DateTime.UtcNow);
            
            Assert.AreEqual(0, dbPartition.GetRowsWithExpiration().Count());
        }
        
        [Test]
        public void TestIfWeCreateExpiredDbRow()
        {
            
            var dbPartition = DbPartition.Create("TestPartition");

            var testEntity = new TestEntity
            {
                PartitionKey = dbPartition.PartitionKey,
                RowKey = "RK",
                Expires = DateTime.Parse("2020-01-01T00:00:00")
            };

            var dbRow = testEntity.ToDbRow();

            dbPartition.Insert(dbRow, DateTime.UtcNow);
            
            Assert.AreEqual(1, dbPartition.GetRowsWithExpiration().Count());

        }

        [Test]
        public void TestIfWeDeleteExpiredDbRow()
        {
            
            var dbPartition = DbPartition.Create("TestPartition");

            var testEntity = new TestEntity
            {
                PartitionKey = dbPartition.PartitionKey,
                RowKey = "RK",
                Expires = DateTime.Parse("2020-01-01T00:00:00")
            };

            var dbRow = testEntity.ToDbRow();

            dbPartition.Insert(dbRow, DateTime.UtcNow);
            
            
            dbPartition.TryDeleteRow(dbRow.RowKey);
            
            Assert.AreEqual(0, dbPartition.GetRowsWithExpiration().Count());
        }
        
        
        [Test]
        public void TestIfWeUpdateExpiredToNonExpiredDbRow()
        {
            
            var dbPartition = DbPartition.Create("TestPartition");

            var testEntity = new TestEntity
            {
                PartitionKey = dbPartition.PartitionKey,
                RowKey = "RK",
                Expires = DateTime.Parse("2020-01-01T00:00:00")
            };

            var dbRow = testEntity.ToDbRow();

            dbPartition.Insert(dbRow, DateTime.UtcNow);
            
            Assert.AreEqual(1, dbPartition.GetRowsWithExpiration().Count());
            
            
            var entityToUpdate = new TestEntity
            {
                PartitionKey = dbPartition.PartitionKey,
                RowKey = "RK",
            };
            
            dbPartition.InsertOrReplace(entityToUpdate.ToDbRow());
            Assert.AreEqual(0, dbPartition.GetRowsWithExpiration().Count());
        }
        
        [Test]
        public void TestIfWeUpdateNonExpiredToExpiredDbRow()
        {
            
            var dbPartition = DbPartition.Create("TestPartition");

            var testEntity = new TestEntity
            {
                PartitionKey = dbPartition.PartitionKey,
                RowKey = "RK"
            };

            var dbRow = testEntity.ToDbRow();

            dbPartition.Insert(dbRow, DateTime.UtcNow);
            
            Assert.AreEqual(0, dbPartition.GetRowsWithExpiration().Count());
            
            
            var entityToUpdate = new TestEntity
            {
                PartitionKey = dbPartition.PartitionKey,
                RowKey = "RK",
                Expires = DateTime.Parse("2020-01-01T00:00:00")
            };
            
            dbPartition.InsertOrReplace(entityToUpdate.ToDbRow());
            Assert.AreEqual(1, dbPartition.GetRowsWithExpiration().Count());
        }
    }
}