using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Common
{
    public interface IMyMemory
    {
        byte[] AsArray();
        IEnumerable<(byte item, int index)> Enumerate();
        ReadOnlyMemory<byte> Slice(int startPosition, int len);
        ReadOnlySpan<byte> Span { get; }
    }


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
        

        
    }
}