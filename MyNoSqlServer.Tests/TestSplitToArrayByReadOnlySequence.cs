using System;
using System.Linq;
using System.Text;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Json;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestSplitToArrayByReadOnlySequence
    {


        [Test]
        public void TestSingleArray()
        {
            var str = "[{A}, {B}]";

            var bytes = Encoding.UTF8.GetBytes(str);


            var mem = new MyMemoryAsByteArray(bytes);

            var result = mem.SplitJsonArrayToObjects().ToArray();
            
            Assert.AreEqual("{A}", Encoding.UTF8.GetString(result[0].Span));
            Assert.AreEqual("{B}", Encoding.UTF8.GetString(result[1].Span));
        }
        
        
        [Test]
        public void TestTwoArrays()
        {
            var str1 = "[{A}, {";
            var str2 = "B}]";

            var bytes1 = Encoding.UTF8.GetBytes(str1);
            var bytes2 = Encoding.UTF8.GetBytes(str2);

            var mem1 = new ReadOnlyMemory<byte>(bytes1);
            var mem2 = new ReadOnlyMemory<byte>(bytes2);
            
            var chunkMem = new ChunkedStream();
            chunkMem.Write(mem1);
            chunkMem.Write(mem2);
            
            var result = chunkMem.SplitJsonArrayToObjects().ToArray();
            
            Assert.AreEqual("{A}", Encoding.UTF8.GetString(result[0].Span));
            Assert.AreEqual("{B}", Encoding.UTF8.GetString(result[1].Span));
        }
        
        [Test]
        public void TestThreeArrays()
        {
            var str1 = "[{A}, {";
            var str2 = "B";
            var str3 = "}]";

            var bytes1 = Encoding.UTF8.GetBytes(str1);
            var bytes2 = Encoding.UTF8.GetBytes(str2);
            var bytes3 = Encoding.UTF8.GetBytes(str3);
            
            
            var chunkMem = new ChunkedStream();
            chunkMem.Write(bytes1);
            chunkMem.Write(bytes2);
            chunkMem.Write(bytes3);
            
            var result = chunkMem.SplitJsonArrayToObjects().ToArray();
            
            Assert.AreEqual("{A}", Encoding.UTF8.GetString(result[0].Span));
            Assert.AreEqual("{B}", Encoding.UTF8.GetString(result[1].Span));
        }
    }
}