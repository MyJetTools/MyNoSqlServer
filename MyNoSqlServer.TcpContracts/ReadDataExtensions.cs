using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MyTcpSockets.Extensions;

namespace MyNoSqlServer.TcpContracts
{
    public static class ReadDataExtensions
    {
        public static void WritePascalStringArray(this Stream stream, string[]src)
        {
            
            stream.WriteInt(src.Length);
            
            foreach (var s in src)
            {
                stream.WritePascalString(s);
            }
            
        }
        
        public static async ValueTask<string[]> ReadPascalStringArrayAsync(this TcpDataReader stream, CancellationToken ct)
        {

            var len = await stream.ReadIntAsync(ct);

            var result = new string[len];

            for (var i = 0; i < len; i++)
            {
                var line = await stream.ReadPascalStringAsync(ct);
                result[i] = line;
            }

            return result;

        }


        private static DateTime UnixTime = new DateTime(1970, 1, 1, 0, 0, 0);
        
        public static void WriteDateTime(this Stream stream, DateTime date)
        {
            var dateAsLong = (date - UnixTime).TotalMilliseconds;
            stream.WriteLong((long)dateAsLong);
        }
        
        public static async ValueTask<DateTime> ReadDateTimeAsync(this TcpDataReader stream, CancellationToken ct)
        {
            var unixTime = await stream.ReadLongAsync(ct);
            return UnixTime.AddMilliseconds(unixTime);
        }
    }
}