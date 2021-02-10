using System;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataReader;
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
                new MyNoSqlServerDataWriter<ReplaceEntity>(() => serverUrl, "test", false);
            
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
                new MyNoSqlServerDataWriter<ReplaceEntity>(() => serverUrl, "test", false);

            var dataWriter2 =
                new MyNoSqlServerDataWriter<MergeEntity>(() => serverUrl, "test", false);

            var dataWriterMerged =
                new MyNoSqlServerDataWriter<MergedEntity>(() => serverUrl, "test", false);
            
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

    
    public class TestDataReader
    {
        public class ReplaceEntity : MyNoSqlDbEntity
        {
            public string Value { get; set; }
        }


        public static async Task TestReplaceAsync(string hostPort, string serverUrl)
        {
            var dataWriter = new MyNoSqlServerDataWriter<ReplaceEntity>(() => serverUrl, "test", false);

            var client = new MyNoSqlTcpClient(() => hostPort, "test-app");
            client.Start();
            

            var dataReader = new MyNoSqlReadRepository<ReplaceEntity>(client, "test");
            await Task.Delay(3000);



            GetData(dataReader);
            await UpdateData(dataWriter);
            await Task.Delay(3000);
            GetData(dataReader);
            await Task.Delay(3000);

            GetData(dataReader);
            await Task.Delay(3000);

            GetData(dataReader);
            await Task.Delay(3000);

            GetData(dataReader);
            await Task.Delay(3000);

            GetData(dataReader);
            await Task.Delay(3000);

            GetData(dataReader);
            await Task.Delay(3000);

            //client.Stop();
        }

        private static async Task UpdateData(MyNoSqlServerDataWriter<ReplaceEntity> dataWriter)
        {
            var replaceEntity = new ReplaceEntity
            {
                PartitionKey = "test",
                RowKey = "test",
                Value = DateTime.Now.ToString("HH:mm:ss")
            };

            await dataWriter.InsertOrReplaceAsync(replaceEntity);
            Console.WriteLine("Update Complite");
        }

        private static void GetData(MyNoSqlReadRepository<ReplaceEntity> dataReader)
        {
            Console.WriteLine("--------");
            var data = dataReader.Get();
            var i = 0;
            foreach (ReplaceEntity item in data)
            {
                Console.WriteLine($"{++i} :{item.Value}");
            }

            Console.WriteLine("========");
        }
    }


}