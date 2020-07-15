using System;
using System.Threading.Tasks;

namespace MyNoSqlServer.ConsoleTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            const string writerUrl = "http://127.0.0.1:5123";
            const string readHostPort = "127.0.0.1:5125";

            await TestDataReader.TestReplaceAsync(readHostPort, writerUrl);

            //await TestDataWriter.TestReplaceAsync(writerUrl);
            //await TestDataWriter.TestMergeAsync(writerUrl);
            
            Console.WriteLine("Hello World!");
        }
    }
    
    
}