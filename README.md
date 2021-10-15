
# RELEASE

**Workflows**:
* ![Release Service](https://github.com/MyJetTools/MyNoSqlServer/workflows/Release%20Server/badge.svg)
* ![Release Client Nugets (Reader and Writer)](https://github.com/MyJetTools/MyNoSqlServer/workflows/Release%20Client%20Nugets%20(Reader%20and%20Writer)/badge.svg)
* ![netstandard 2.0 - Release Client Nugets for  (Reader and Writer)](https://github.com/MyJetTools/MyNoSqlServer/workflows/netstandard%202.0%20-%20Release%20Client%20Nugets%20for%20%20(Reader%20and%20Writer)/badge.svg)
* ![check commit](https://github.com/MyJetTools/MyNoSqlServer/workflows/check%20commit/badge.svg)


**Client library:** 
* ![MyNoSqlServer.DataReader](https://img.shields.io/nuget/v/MyNoSqlServer.DataReader?label=MyNoSqlServer.DataReader&style=social)
* ![MyNoSqlServer.DataWriter](https://img.shields.io/nuget/v/MyNoSqlServer.DataWriter?label=MyNoSqlServer.DataWriter&style=social)

* ![MyNoSqlServer20.DataReader](https://img.shields.io/nuget/v/MyNoSqlServer20.DataReader?label=MyNoSqlServer20.DataReader&style=social)
* ![MyNoSqlServer20.DataWriter](https://img.shields.io/nuget/v/MyNoSqlServer20.DataWriter?label=MyNoSqlServer20.DataWriter&style=social)

**Docker**:
* ![myjettools/my-nosql-server](https://img.shields.io/docker/v/myjettools/my-nosql-server?label=myjettools%2Fmy-nosql-server&style=flat-square)


# mynosqlserver



Keeps data like azure table storage - but - in memory and it has a swagger.

Save snapshot to Azure blobs of each table once it changed eventually.

To create snapshot saving we have to create azure storage account and create containers with same names as tables have. No Containier - means no snapshot saving.

As well it has a feature - to subscribe to table changes as SignalR topic to get changes of the table in realtime.


MyNoSqlServerClient - has a nuget https://www.nuget.org/packages/MyNoSqlClient


# DEVELOP

**Workflows:**
* ![DEV Server release](https://github.com/MyJetTools/MyNoSqlServer/workflows/DEV%20Server%20release/badge.svg)
* ![DEV Client nuget release](https://github.com/MyJetTools/MyNoSqlServer/workflows/DEV%20Client%20nuget%20release/badge.svg)

**Client library:** 
* ![MyNoSqlServer.DataReader](https://img.shields.io/nuget/v/MyNoSqlServer.DataReader.dev?label=MyNoSqlServer.DataReader.dev&style=social)
* ![MyNoSqlServer.DataWriter](https://img.shields.io/nuget/v/MyNoSqlServer.DataWriter.dev?label=MyNoSqlServer.DataWriter.dev&style=social)

* ![MyNoSqlServer20.DataReader](https://img.shields.io/nuget/v/MyNoSqlServer20.DataReader.dev?label=MyNoSqlServer20.DataReader.dev&style=social)
* ![MyNoSqlServer20.DataWriter](https://img.shields.io/nuget/v/MyNoSqlServer20.DataWriter.dev?label=MyNoSqlServer20.DataWriter.dev&style=social)

**Docker**:
* ![myjettools/my-nosql-server](https://img.shields.io/docker/v/myjettools/my-nosql-server-dev?label=myjettools%2Fmy-nosql-server-dev&style=flat-square)


# GrpcWriter:

The Library which facilitates the ability to make updates though GRPC Transport

### Example of creating GRPC Writer:

```csharp        
using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Net.Client;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Grpc;
using MyNoSqlServer.GrpcDataWriter;
using ProtoBuf.Grpc.Client;

namespace MyNoSqlServerTestGrpcClient
{
  class Program
  {
        static void Main(string[] args)
        {
            TestGrpcContracts().Wait();

            Console.WriteLine("Done");
        }

        private static async Task TestGrpcContracts()
        {
            GrpcClientFactory.AllowUnencryptedHttp2 = true;
            
            var transportChannel = GrpcChannel
                .ForAddress("http://127.0.0.1:5124")
                .CreateGrpcService<IMyNoSqlTransportGrpcService>();

            var grpcWriter = new MyNoSqlGrpcDataWriter(transportChannel);

            grpcWriter.RegisterSupportedEntity<TestEntity>("testtbl");
  
            var sw = new Stopwatch();
            sw.Start();
            
            var transaction = grpcWriter.BeginTransaction();

            transaction.InsertOrReplaceEntity(new TestEntity
            {
                PartitionKey = "PK",
                RowKey = "RK",
                Value = "Value"
            });

            await transaction.CommitAsync();

            sw.Stop();
            
            Console.WriteLine($"Record is inserted in {sw.Elapsed}");
            
            
            sw.Reset();
            var i = 0;
            sw.Start();
            await foreach (var _ in grpcWriter.GetRowsAsync<TestEntity>())
            {
                i++;
            }
            sw.Stop();
            
            Console.WriteLine($"Got {i} records in {sw.Elapsed}");
        }
    }

    public class TestEntity : MyNoSqlDbEntity
    {
        public string Value { get; set; }
    }
}

```


# RELEASE NOTES

Server 1.0.54-RC

* Added ability to get all data Partition by partition;
* Added GRPC Writer Support which can:
  * Create Table if not exists
  * Specify Table Attributes:
    * Whether table has to be persistent or not;
    * Max amount of partitions table can hold;
  * All GET Operations by combinations of PartitionsKey and RowKey;
  * Transactional Updates;
* Added ability to update data transactionally (only 100% executable operations are support):
  * Clean table;
  * Clean Partitions;
  * Delete Rows;
  * InsertOrReplace partitions;


