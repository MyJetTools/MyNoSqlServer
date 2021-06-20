using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{


    public static class ProtobufSerializers
    {
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
        
        public static IReadOnlyDictionary<string, IReadOnlyList<DbRow>> DeserializeProtobufPartitionsData(this byte[] protobufPayload)
        {
            
            var protobuf = ProtoBuf.Serializer.Deserialize<PartitionDataGrpcModel[]>(protobufPayload.AsSpan());


            var result = new Dictionary<string, IReadOnlyList<DbRow>>();


            foreach (var partition in protobuf)
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