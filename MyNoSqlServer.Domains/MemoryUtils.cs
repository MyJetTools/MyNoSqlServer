using System;

namespace MyNoSqlServer.Domains
{
    public static class MemoryUtils
    {

        public static bool MemoriesEqual(this in ReadOnlyMemory<byte> src, in ReadOnlyMemory<byte> dest)
        {
            if (src.Length != dest.Length)
                return false;

            var srcSpan = src.Span;
            var destSpan = dest.Span;
            
            for (var i = 0; i < src.Length; i++)
            {
                if (srcSpan[i] != destSpan[i])
                    return false;
            }

            return true;

        }
        
    }
}