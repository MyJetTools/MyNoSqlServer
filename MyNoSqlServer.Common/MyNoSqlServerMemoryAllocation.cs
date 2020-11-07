using System;

namespace MyNoSqlServer.Common
{
    public static class MyNoSqlServerMemory
    {

        public static Func<int, byte[]> AllocateByteArray = size => new byte[size];

    }
}