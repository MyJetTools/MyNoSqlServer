using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

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
        
        
        public static void AssertExpirationDate(this DateTime? srcExpirationDate, DateTime? destExpirationDate)
        {
            if (srcExpirationDate == null && destExpirationDate == null)
                return;

            if (srcExpirationDate == null && destExpirationDate != null)
                throw new Exception("srcExpirationDate == null && destExpirationDate != null");
            
            if (srcExpirationDate != null && destExpirationDate == null)
                throw new Exception("srcExpirationDate != null && destExpirationDate == null");
            
            Assert.AreEqual(srcExpirationDate.Value.ToString("s"), destExpirationDate.Value.ToString("s"));

        }
    }
}