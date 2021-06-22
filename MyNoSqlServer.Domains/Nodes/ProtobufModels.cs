using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{


    public static class ProtobufSerializers
    {

        public static IAsyncEnumerable<PayloadWrapperGrpcModel> SplitAndWrap(this byte[] payload, int batchSize)
        {
            return payload.SplitPayload(batchSize).Select(itm => new PayloadWrapperGrpcModel
            {
                Payload = itm
            }).ToAsyncEnumerable();
        }

        public static IEnumerable<PartitionDataGrpcModel> ToPartitionDataGrpcModel(
            this IReadOnlyDictionary<string, IReadOnlyList<DbRow>> snapshot)
        {
            return snapshot.Select(itm => new PartitionDataGrpcModel
            {
                PartitionKey = itm.Key,
                Snapshot = itm.Value.ToJsonArray().AsArray()
            });
        }
        
        
        public static byte[] SerializeProtobufPartitionsData(this IReadOnlyDictionary<string, IReadOnlyList<DbRow>> snapshot)
        {
            var memoryStream = new MemoryStream();

            var items = snapshot.Select(itm => new PartitionDataGrpcModel
            {
                PartitionKey = itm.Key,
                Snapshot = itm.Value.ToJsonArray().AsArray()
            });
            
            ProtoBuf.Serializer.Serialize(memoryStream, items);

            return memoryStream.ToArray();
        }
        
        public static IReadOnlyDictionary<string, IReadOnlyList<DbRow>> DeserializeProtobufPartitionsData(this IEnumerable<PartitionDataGrpcModel> partitionsData)
        {
            

            var result = new Dictionary<string, IReadOnlyList<DbRow>>();


            foreach (var partition in partitionsData)
            {

                Console.WriteLine(partition.PartitionKey);
                var rows = partition.Snapshot.AsMyMemory().SplitJsonArrayToObjects()
                    .Select(itm =>
                    {
                        var entity = itm.ParseDynamicEntity();
                        return DbRow.CreateNew(entity, DateTime.Parse(entity.TimeStamp));
                    });
                    
                result.Add(partition.PartitionKey,rows.AsReadOnlyList());
            }

            return result;
        }
    }


}