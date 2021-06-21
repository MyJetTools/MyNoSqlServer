using System;
using System.Threading.Tasks;
using MyNoSqlServer.DataWriter;

namespace MyNoSqlServer.ConsoleTest
{
    class Program
    {
        static async Task Main()
        {
            const string writerUrl = "http://192.168.1.160:5123";
            //const string readHostPort = "192.168.1.160:5125";


            var writer = new MyNoSqlServerDataWriter<TestDataReader.ReplaceEntity>(() => writerUrl,
                "test-table", 
                true);

            await writer.InsertOrReplaceAsync(new TestDataReader.ReplaceEntity
            {
                PartitionKey = "Pk1",
                RowKey = "Rk1",
                Value = "Test2"
            });

          //  await TestDataReader.TestReplaceAsync(readHostPort, writerUrl);

            //await TestDataWriter.TestReplaceAsync(writerUrl);
            //await TestDataWriter.TestMergeAsync(writerUrl);
            
            Console.WriteLine("Hello World!");
        }
    }
    
    
}