using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts.Tests
{
    public static class TestUtils
    {
        public static async Task<IReadOnlyList<T>> AsReadOnlyListAsync<T>(this IAsyncEnumerable<T> src)
        {
            var result = new List<T>();
            await foreach (var itm in src)
            {
                result.Add(itm);
            }

            return result;
        }

        public static IReadOnlyList<T> AsReadOnlyList<T>(this IAsyncEnumerable<T> src)
        {
            return src.AsReadOnlyListAsync().Result;
        }

        public static T AsTestResult<T>(this IAsyncEnumerable<T> src)
        {
            return src.AsReadOnlyListAsync().Result.First();
        }

    }
}