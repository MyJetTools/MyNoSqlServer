using System;
using System.Threading;
using MyTcpSockets.Extensions;
using NUnit.Framework;

namespace MyNoSqlServer.TcpContracts.Tests
{
    public class TcpContractsTests
    {

        private const int ReadBufferSize = 2048 * 2048;

        [Test]
        public void TestPing()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var pingContract = new PingContract();

            var rawData = serializer.Serialize(pingContract);


            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);

            var tc = new CancellationTokenSource();
            
            var result
                = serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.IsTrue(typeof(PingContract) == result.GetType());
        }

        [Test]
        public void TestPong()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new PongContract();

            var rawData = serializer.Serialize(testContract);


            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);

            var tc = new CancellationTokenSource();
            
            var result
                = serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.IsTrue(typeof(PongContract) == result.GetType());
        }

        [Test]
        public void TestGreetingContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new GreetingContract
            {
                Name = "Test"
            };

            var rawData = serializer.Serialize(testContract);


            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);

            
            var tc = new CancellationTokenSource();

            var result
                = (GreetingContract)serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.AreEqual(testContract.Name, result.Name);
        }


        [Test]
        public void TestInitTableContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new InitTableContract
            {
                TableName = "Test",
                Data = new byte[] {1, 2, 3}
            };

            var rawData = serializer.Serialize(testContract);

            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);
            
            var tc = new CancellationTokenSource();
            var result
                = (InitTableContract) serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.AreEqual(testContract.TableName, result.TableName);
            Assert.AreEqual(testContract.Data.Length, result.Data.Length);
            for (var i = 0; i < testContract.Data.Length; i++)
                Assert.AreEqual(testContract.Data[i], result.Data[i]);

        }


        [Test]
        public void TestInitPartitionContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new InitPartitionContract
            {
                TableName = "Test",
                PartitionKey = "PK",
                Data = new byte[] {1, 2, 3}
            };

            var rawData = serializer.Serialize(testContract);

            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);


            var tc = new CancellationTokenSource();
            var result
                = (InitPartitionContract) serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.AreEqual(testContract.TableName, result.TableName);
            Assert.AreEqual(testContract.PartitionKey, result.PartitionKey);
            Assert.AreEqual(testContract.Data.Length, result.Data.Length);
            for (var i = 0; i < testContract.Data.Length; i++)
                Assert.AreEqual(testContract.Data[i], result.Data[i]);

        }        
        
        
        [Test]
        public void TestUpdateRowsContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new UpdateRowsContract
            {
                TableName = "Test",
                Data = new byte[] {1, 2, 3}
            };

            var rawData = serializer.Serialize(testContract);

            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);


            var tc = new CancellationTokenSource();
            var result
                = (UpdateRowsContract) serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.AreEqual(testContract.TableName, result.TableName);
            Assert.AreEqual(testContract.Data.Length, result.Data.Length);
            for (var i = 0; i < testContract.Data.Length; i++)
                Assert.AreEqual(testContract.Data[i], result.Data[i]);

        }     
        
        
        [Test]
        public void TestSubscribeContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new SubscribeContract
            {
                TableName = "Test"
            };

            var rawData = serializer.Serialize(testContract);

            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);


            
            var tc = new CancellationTokenSource();
            
            var result
                = (SubscribeContract) serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.AreEqual(testContract.TableName, result.TableName);

        }
        
        [Test]
        public void TestDeleteRowsContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new DeleteRowsContract
            {
                TableName = "Test",
                RowsToDelete = new[]{("pk1", "rk1"), ("pk2", "rk2")}
            };

            var rawData = serializer.Serialize(testContract);

            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);

            
            var tc = new CancellationTokenSource();

            var result
                = (DeleteRowsContract) serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.AreEqual(testContract.TableName, result.TableName);
            
            Assert.AreEqual(testContract.RowsToDelete.Count, result.RowsToDelete.Count);
            for (var i = 0; i < testContract.RowsToDelete.Count; i++)
            {
                Assert.AreEqual(testContract.RowsToDelete[i].PartitionKey, result.RowsToDelete[i].PartitionKey);
                Assert.AreEqual(testContract.RowsToDelete[i].RowKey, result.RowsToDelete[i].RowKey);
                
            }

        }        
        
        
        
        [Test]
        public void TestUpdateExpirationsContract()
        {

            var serializer = new MyNoSqlTcpSerializer();

            var testContract = new UpdateExpiresTimeTcpContract
            {
                TableName= "TableName",
                PartitionKey = "PartitionKey",
                Expires = DateTime.UtcNow,
                RowKeys = new[]{"rk1", "rk2"}
            };

            var rawData = serializer.Serialize(testContract);

            var dataReader = new TcpDataReader(ReadBufferSize, 1024);
            var memToWrite = dataReader.AllocateBufferToWrite();
            rawData.CopyTo(memToWrite);
            dataReader.CommitWrittenData(rawData.Length);
            
            var tc = new CancellationTokenSource();

            var result
                = (UpdateExpiresTimeTcpContract) serializer
                    .DeserializeAsync(dataReader, tc.Token)
                    .AsTestResult();

            Assert.AreEqual(testContract.TableName, result.TableName);
            
            Assert.AreEqual(testContract.PartitionKey, result.PartitionKey);
            
            testContract.Expires.AssertExpirationDate(result.Expires);
            
            Assert.AreEqual(testContract.RowKeys.Length, result.RowKeys.Length);
            for (var i = 0; i < testContract.RowKeys.Length; i++)
            {
                Assert.AreEqual(testContract.RowKeys[i], result.RowKeys[i]);
            }

        }   
        
        

    }



}