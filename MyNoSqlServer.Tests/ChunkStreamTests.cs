using MyNoSqlServer.Common;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class ChunkStreamTests
    {

        [Test]
        public void TestReadOneByOneWholeStream()
        {

            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var chunkedStream = new ChunkedStream();

            chunkedStream.Write(array, 1, 3);
            chunkedStream.Write(array, 2, 4);
            chunkedStream.Write(array, 3, 3);


            var b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[1]);
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[2]);
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[3]);
            
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[2]);
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[3]);
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[4]);
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[5]);
            
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[3]);
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[4]);
            b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[5]);

        }
        
        
        [Test]
        public void TestCopyToNewArray()
        {

            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var chunkedStream = new ChunkedStream();

            chunkedStream.Write(array, 1, 3);
            chunkedStream.Write(array, 2, 4);
            chunkedStream.Write(array, 3, 3);


            var destArray = chunkedStream.AsArray();


            Assert.AreEqual(destArray[0], array[1]);
            Assert.AreEqual(destArray[1], array[2]);
            Assert.AreEqual(destArray[2], array[3]);
            
            Assert.AreEqual(destArray[3], array[2]);
            Assert.AreEqual(destArray[4], array[3]);
            Assert.AreEqual(destArray[5], array[4]);
            Assert.AreEqual(destArray[6], array[5]);
            
            Assert.AreEqual(destArray[7], array[3]);
            Assert.AreEqual(destArray[8], array[4]);
            Assert.AreEqual(destArray[9], array[5]);

        }
        
        [Test]
        public void TestMixCopy()
        {

            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var chunkedStream = new ChunkedStream();

            chunkedStream.Write(array, 1, 3);
            chunkedStream.Write(array, 2, 4);
            chunkedStream.Write(array, 3, 3);

            
            var b = chunkedStream.ReadByte();
            Assert.AreEqual(b, array[1]);


            var destArray = new byte[8];
            chunkedStream.Read(destArray, 0, destArray.Length);
            
            Assert.AreEqual(array[2], destArray[0]);
            Assert.AreEqual(array[3], destArray[1]);
            Assert.AreEqual(array[2], destArray[2]);            
            Assert.AreEqual(array[3], destArray[3]);
            Assert.AreEqual(array[4], destArray[4]);
            Assert.AreEqual(array[5], destArray[5]);            
            Assert.AreEqual(array[3], destArray[6]);
            Assert.AreEqual(array[4], destArray[7]);

        }
    }
}