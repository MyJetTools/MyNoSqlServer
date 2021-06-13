using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.DataCompression;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.NodePersistence
{
    public static class CompressUtils
    {
        
        public static ReadOnlyMemory<byte> ToReadOnlyMemory(this MemoryStream stream)
        {
            return new ReadOnlyMemory<byte>(stream.GetBuffer(), 0, (int)stream.Length);
        }
        
        public static async IAsyncEnumerable<PayloadWrapperGrpcModel> CompressAndSplitAsync(this object contract, int batchSize, bool compress)
        {
            var stream = new MemoryStream();
            ProtoBuf.Serializer.Serialize(stream, contract);

            stream.Position = 0;

            var payload = compress ? stream.ZipPayload().ToArray() : stream.ToArray();

            foreach (var batch in payload.SplitPayload(batchSize))
            {
                yield return new PayloadWrapperGrpcModel
                {
                    Payload = batch
                };
            }

        }

        public static async Task<T> MergePayloadAndDeserialize<T>(this IAsyncEnumerable<PayloadWrapperGrpcModel> payLoadAsync, bool compressed)
        {
            var stream = new MemoryStream();

            await foreach (var batch in payLoadAsync)
            {
                stream.Write(batch.Payload);
            }

            var content = compressed ? MyNoSqlServerDataCompression.UnZipPayload(stream.ToArray()).AsMemory() : stream.ToReadOnlyMemory();
            return ProtoBuf.Serializer.Deserialize<T>(content);

        }
    }
}