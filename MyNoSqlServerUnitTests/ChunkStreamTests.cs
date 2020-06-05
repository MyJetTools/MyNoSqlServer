using MyNoSqlServer.Common;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    public class ChunkStreamTests
    {

        [Fact]
        public void TestReadOneByOneWholeStream()
        {

            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var chunkedStream = new ChunkedStream();

            chunkedStream.Write(array, 1, 3);
            chunkedStream.Write(array, 2, 4);
            chunkedStream.Write(array, 3, 3);


            var b = chunkedStream.ReadByte();
            Assert.Equal(b, array[1]);
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[2]);
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[3]);
            
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[2]);
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[3]);
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[4]);
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[5]);
            
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[3]);
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[4]);
            b = chunkedStream.ReadByte();
            Assert.Equal(b, array[5]);

        }
        
        
        [Fact]
        public void TestCopyToNewArray()
        {

            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var chunkedStream = new ChunkedStream();

            chunkedStream.Write(array, 1, 3);
            chunkedStream.Write(array, 2, 4);
            chunkedStream.Write(array, 3, 3);


            var destArray = chunkedStream.AsArray();


            Assert.Equal(destArray[0], array[1]);
            Assert.Equal(destArray[1], array[2]);
            Assert.Equal(destArray[2], array[3]);
            
            Assert.Equal(destArray[3], array[2]);
            Assert.Equal(destArray[4], array[3]);
            Assert.Equal(destArray[5], array[4]);
            Assert.Equal(destArray[6], array[5]);
            
            Assert.Equal(destArray[7], array[3]);
            Assert.Equal(destArray[8], array[4]);
            Assert.Equal(destArray[9], array[5]);

        }
        
        [Fact]
        public void TestMixCopy()
        {

            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var chunkedStream = new ChunkedStream();

            chunkedStream.Write(array, 1, 3);
            chunkedStream.Write(array, 2, 4);
            chunkedStream.Write(array, 3, 3);

            
            var b = chunkedStream.ReadByte();
            Assert.Equal(b, array[1]);


            var destArray = new byte[8];
            chunkedStream.Read(destArray, 0, destArray.Length);
            
            Assert.Equal(array[2], destArray[0]);
            Assert.Equal(array[3], destArray[1]);
            Assert.Equal(array[2], destArray[2]);            
            Assert.Equal(array[3], destArray[3]);
            Assert.Equal(array[4], destArray[4]);
            Assert.Equal(array[5], destArray[5]);            
            Assert.Equal(array[3], destArray[6]);
            Assert.Equal(array[4], destArray[7]);

        }
    }
}