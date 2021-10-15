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

}