using System;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataWriter;

namespace MyNoSqlServer.ConsoleTest
{
    public class TestDataWriter
    {

        public class ReplaceEntity : MyNoSqlDbEntity
        {
            public string Value { get; set; }
        }
        
        public class MergeEntity : MyNoSqlDbEntity
        {
            public string Value2 { get; set; }
        }
        
        public class MergedEntity : MyNoSqlDbEntity
        {
            public string Value { get; set; }
            public string Value2 { get; set; }
        }


        public static async Task TestReplaceAsync(string serverUrl)
        {
            var dataWriter =
                new MyNoSqlServerDataWriter<ReplaceEntity>(() => serverUrl, "test");
            
            var replaceEntity = new ReplaceEntity
            {
                PartitionKey = "test",
                RowKey = "test",
                Value = "123"
            };

            await dataWriter.InsertOrReplaceAsync(replaceEntity);

            var operationResult = await dataWriter.ReplaceAsync(replaceEntity.PartitionKey, replaceEntity.RowKey, entityToEdit =>
            {
                entityToEdit.Value = "456";
                return true;
            });
            
            Assert.AreEqual(OperationResult.Ok, operationResult);
            
            var entity = await dataWriter.GetAsync(replaceEntity.PartitionKey, replaceEntity.RowKey);

            Assert.AreEqual("456", entity.Value);
            
            Console.WriteLine("TestReplaceAsync OK");
        }
        
        public static async Task TestMergeAsync(string serverUrl)
        {
            var dataWriter1 =
                new MyNoSqlServerDataWriter<ReplaceEntity>(() => serverUrl, "test");

            var dataWriter2 =
                new MyNoSqlServerDataWriter<MergeEntity>(() => serverUrl, "test");

            var dataWriterMerged =
                new MyNoSqlServerDataWriter<MergedEntity>(() => serverUrl, "test");
            
            var replaceEntity = new ReplaceEntity
            {
                PartitionKey = "test",
                RowKey = "test",
                Value = "123"
            };

            await dataWriter1.InsertOrReplaceAsync(replaceEntity);

            var operationResult = await dataWriter2.MergeAsync(replaceEntity.PartitionKey, replaceEntity.RowKey, entityToEdit =>
            {
                entityToEdit.Value2 = "456";
                return true;
            });
            
            Assert.AreEqual(OperationResult.Ok, operationResult);
            
            var entity = await dataWriterMerged.GetAsync(replaceEntity.PartitionKey, replaceEntity.RowKey);

            Assert.AreEqual("123", entity.Value);
            Assert.AreEqual("456", entity.Value2);
            
            Console.WriteLine("TestMergeAsync OK");
        }
        
    }
}