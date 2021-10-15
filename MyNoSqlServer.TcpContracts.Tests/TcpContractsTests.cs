using System.Threading;
using System.Threading.Tasks;
using MyTcpSockets.Extensions;
using NUnit.Framework;

namespace MyNoSqlServer.TcpContracts.Tests
{
    public class TcpContractsTests
    {

        private const int ReadBufferSize = 1024;
        

        [Test]
        public async Task TestPing()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var pingContract = new PingContract();
            
            var incomingTraffic = new IncomingTcpTrafficMock();


            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);
            
            var rawData = serializer.Serialize(pingContract);

            incomingTraffic.NewPackageAsync(rawData);

            var tc = new CancellationTokenSource();

            var result
                = await serializer
                    .DeserializeAsync(dataReader, tc.Token);

            Assert.IsTrue(typeof(PingContract) == result.GetType());
        }

        [Test]
        public async Task TestPong()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new PongContract();
            
            var incomingTraffic = new IncomingTcpTrafficMock();

            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);

            var rawData = serializer.Serialize(testContract);

            incomingTraffic.NewPackageAsync(rawData);

            var tc = new CancellationTokenSource();

            var result
                = await serializer
                    .DeserializeAsync(dataReader, tc.Token);

            Assert.IsTrue(typeof(PongContract) == result.GetType());
        }

        [Test]
        public async Task TestGreetingContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new GreetingContract
            {
                Name = "Test"
            };

            var incomingTraffic = new IncomingTcpTrafficMock();

            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);
            
            var rawData = serializer.Serialize(testContract);

            incomingTraffic.NewPackageAsync(rawData);

            var result
                = (GreetingContract)
                await serializer
                    .DeserializeAsync(dataReader, CancellationToken.None);

            Assert.AreEqual(testContract.Name, result.Name);
        }


        [Test]
        public async Task TestInitTableContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new InitTableContract
            {
                TableName = "Test",
                Data = new byte[] {1, 2, 3}
            };

            var incomingTraffic = new IncomingTcpTrafficMock();

            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);
            
            var rawData = serializer.Serialize(testContract);
            incomingTraffic.NewPackageAsync(rawData);
            
            var result
                = (InitTableContract) await serializer
                    .DeserializeAsync(dataReader, CancellationToken.None);

            Assert.AreEqual(testContract.TableName, result.TableName);
            Assert.AreEqual(testContract.Data.Length, result.Data.Length);
            for (var i = 0; i < testContract.Data.Length; i++)
                Assert.AreEqual(testContract.Data[i], result.Data[i]);

        }


        [Test]
        public async Task TestInitPartitionContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new InitPartitionContract
            {
                TableName = "Test",
                PartitionKey = "PK",
                Data = new byte[] {1, 2, 3}
            };
            
            var incomingTraffic = new IncomingTcpTrafficMock();


            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);
            
            var rawData = serializer.Serialize(testContract);
            incomingTraffic.NewPackageAsync(rawData);

            var result
                = (InitPartitionContract) await serializer
                    .DeserializeAsync(dataReader, CancellationToken.None);

            Assert.AreEqual(testContract.TableName, result.TableName);
            Assert.AreEqual(testContract.PartitionKey, result.PartitionKey);
            Assert.AreEqual(testContract.Data.Length, result.Data.Length);
            for (var i = 0; i < testContract.Data.Length; i++)
                Assert.AreEqual(testContract.Data[i], result.Data[i]);

        }        
        
        
        [Test]
        public async Task TestUpdateRowsContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new UpdateRowsContract
            {
                TableName = "Test",
                Data = new byte[] {1, 2, 3}
            };

            var incomingTraffic = new IncomingTcpTrafficMock();

            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);
            
            var rawData = serializer.Serialize(testContract);
            incomingTraffic.NewPackageAsync(rawData);

            var result
                = (UpdateRowsContract) await serializer
                    .DeserializeAsync(dataReader, CancellationToken.None);

            Assert.AreEqual(testContract.TableName, result.TableName);
            Assert.AreEqual(testContract.Data.Length, result.Data.Length);
            for (var i = 0; i < testContract.Data.Length; i++)
                Assert.AreEqual(testContract.Data[i], result.Data[i]);

        }     
        
        
        [Test]
        public async Task TestSubscribeContract()
        {

            var serializer = new MyNoSqlTcpSerializer();

            var testContract = new SubscribeContract
            {
                TableName = "Test"
            };


            var incomingTraffic = new IncomingTcpTrafficMock();

            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);
            
            var rawData = serializer.Serialize(testContract);
            incomingTraffic.NewPackageAsync(rawData);
            
            var result
                = (SubscribeContract) await serializer
                    .DeserializeAsync(dataReader, CancellationToken.None);

            Assert.AreEqual(testContract.TableName, result.TableName);

        }
        
        [Test]
        public async Task TestDeleteRowsContract()
        {

            var serializer = new MyNoSqlTcpSerializer();


            var testContract = new DeleteRowsContract
            {
                TableName = "Test",
                RowsToDelete = new[]{("pk1", "rk1"), ("pk2", "rk2")}
            };

            var incomingTraffic = new IncomingTcpTrafficMock();

            var dataReader = new TcpDataReader(incomingTraffic, ReadBufferSize);
            
            var rawData = serializer.Serialize(testContract);
            
            incomingTraffic.NewPackageAsync(rawData);

            var result
                = (DeleteRowsContract) await serializer
                    .DeserializeAsync(dataReader, CancellationToken.None);

            Assert.AreEqual(testContract.TableName, result.TableName);
            
            Assert.AreEqual(testContract.RowsToDelete.Count, result.RowsToDelete.Count);
            for (var i = 0; i < testContract.RowsToDelete.Count; i++)
            {
                Assert.AreEqual(testContract.RowsToDelete[i].PartitionKey, result.RowsToDelete[i].PartitionKey);
                Assert.AreEqual(testContract.RowsToDelete[i].RowKey, result.RowsToDelete[i].RowKey);
                
            }

        }        
        
        

    }



}