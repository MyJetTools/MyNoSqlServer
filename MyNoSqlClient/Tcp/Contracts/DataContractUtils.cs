using System;
using System.IO;
using System.Threading.Tasks;
using MyTcpSockets;

namespace MyNoSqlClient.Tcp.Contracts
{
    public static class DataContractUtils
    {
        public static  ValueTask<int> ReadIntAsync(this Stream stream)
        {
            
            var task = stream.ReadFromSocket(4)
                .ContinueWith(theTask => (int) theTask.Result.ParseuInt());
            
            return new ValueTask<int>(task);
        }
        
        public static async Task<byte[]> ReadAsArrayOfBytesAsync(this Stream stream)
        {
            var l = await stream.ReadIntAsync();
            return await stream.ReadFromSocket(l);
        }
        
        public static void WriteInt(this Stream stream, int value)
        {
            var b = (byte) value;
            stream.WriteByte(b);

            value >>= 8;
            b = (byte)value;
            stream.WriteByte(b);

            value >>= 8;
            b = (byte)value;
            stream.WriteByte(b);

            value >>= 8;
            b = (byte)value;
            stream.WriteByte(b);
        }
        
        public static void WriteReadOnlyMemory(this Stream stream, in ReadOnlyMemory<byte> src)
        {
            stream.WriteInt(src.Length);
            stream.Write(src.Span);
        }
        
    }
}