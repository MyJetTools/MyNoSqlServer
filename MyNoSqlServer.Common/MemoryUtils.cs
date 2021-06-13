using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Common
{
    public static class MemoryUtils
    {
        public static IMyMemory AsMyMemory(this byte[] src)
        {
            return ChunkedStream.Create(src);
        }

        public static IMyMemory AsMyMemory(this ReadOnlyMemory<byte> src)
        {
            return ChunkedStream.Create(src);
        }

        public static IEnumerable<byte[]> SplitPayload(this byte[] payload, int batchSize)
        {
            if (payload.Length <= batchSize)
            {
                yield return payload;
            }
            else
            {

                var remains = payload.Length;
                var position = 0;

                while (remains>0)
                {
                    var chunkSize = remains > batchSize ? batchSize : remains;
                    yield return payload.AsMemory(position, chunkSize).ToArray();

                    position += chunkSize;
                    remains -= chunkSize;
                }


            }
        }
        

    }
}