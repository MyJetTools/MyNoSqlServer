using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace MyNoSqlServer.DataReader.Tests
{
    public class TestTableDifferenceDetection
    {

        [Test]
        public void TestFirstInitTable()
        {

            var entity1 = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = "0"
            };

            var subscriberMock = new MyNoSqlSubscriberMock();
            var dataReader = new MyNoSqlReadRepository<TestEntity>(subscriberMock,"test");

            var updates = new List<TestEntity>();
            var deletes = new List<TestEntity>();
            dataReader.SubscribeToUpdateEvents(updates.AddRange, deletes.AddRange);

            subscriberMock.InitAction(new[] {entity1});
            
            Assert.AreEqual(1, updates.Count);
            Assert.AreEqual(0, deletes.Count);

        }
        
        [Test]
        public void TestInitTableWithComplietlyNewData()
        {

            var entity1 = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = "0"
            };

            var subscriberMock = new MyNoSqlSubscriberMock();
            var dataReader = new MyNoSqlReadRepository<TestEntity>(subscriberMock,"test");

            var updates = new List<TestEntity>();
            var deletes = new List<TestEntity>();
            dataReader.SubscribeToUpdateEvents(updates.AddRange, deletes.AddRange);

            subscriberMock.InitAction(new[] {entity1});
            
            Assert.AreEqual(1, updates.Count);
            Assert.AreEqual(0, deletes.Count);
            updates.Clear();
            
            
            var entity2 = new TestEntity
            {
                PartitionKey = "PK2",
                RowKey = "RK2",
                TimeStamp = "2"
            };
            
            subscriberMock.InitAction(new[] {entity2});
            
            
            Assert.AreEqual(1, updates.Count);
            Assert.AreEqual(1, deletes.Count);

        }        
        
        
        [Test]
        public void TestInitTableWithMixedNewData()
        {

            var entity1 = new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = "0"
            };
            
            var entity2 = new TestEntity
            {
                PartitionKey = "PK2",
                RowKey = "RK2",
                TimeStamp = "2"
            };

            var subscriberMock = new MyNoSqlSubscriberMock();
            var dataReader = new MyNoSqlReadRepository<TestEntity>(subscriberMock,"test");

            var updates = new List<TestEntity>();
            var deletes = new List<TestEntity>();
            dataReader.SubscribeToUpdateEvents(updates.AddRange, deletes.AddRange);

            subscriberMock.InitAction(new[] {entity1});
            subscriberMock.UpdateAction(new[] {entity2});
            
            Assert.AreEqual(2, updates.Count);
            Assert.AreEqual(0, deletes.Count);
            updates.Clear();
            
            

            
            subscriberMock.InitAction(new[] {entity2});
            
            
            Assert.AreEqual(0, updates.Count);
            Assert.AreEqual(1, deletes.Count);

        }    
        
    }
    
    
    


    internal class MyNoSqlSubscriberMock : IMyNoSqlSubscriber
    {
        public Action<IReadOnlyList<object>> InitAction { get; set; }
        public Action<string, IReadOnlyList<object>> InitPartitionAction { get; set; }
        
        public Action<IReadOnlyList<object>> UpdateAction { get; set; }
        
        public Action<IEnumerable<(string partitionKey, string rowKey)>> DeleteAction { get; set; }

        public void Subscribe<T>(string tableName,
            Action<IReadOnlyList<T>> initAction,
            Action<string, IReadOnlyList<T>> initPartitionAction,
            Action<IReadOnlyList<T>> updateAction,
            Action<IEnumerable<(string partitionKey, string rowKey)>> deleteActions)
        {
            InitAction = items => initAction(items.Cast<T>().ToList());
            InitPartitionAction = (partitionKey, items) =>
                initPartitionAction(partitionKey, items.Cast<T>().ToList());

            UpdateAction = items => { updateAction(items.Cast<T>().ToList()); };
            DeleteAction = deleteActions;
        }

        public void UpdateExpirationDate(string tableName, string partitionKey, string[] rowKeys, DateTime? expirationTime,
            bool cleanExpirationTime)
        {
            throw new NotImplementedException();
        }
    }
}