using MyNoSqlServer.Common;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    
    
    /*
    public class SpanTests
    {

        [Fact]
        public static void TestInitializationByLength()
        {
            
            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var arraySpan = array.ToByteArraySpan(1, 2);

            var newArray = arraySpan.AsArray();

            Assert.Equal(1, newArray[0]);
            Assert.Equal(2, newArray[1]);
            Assert.Equal(2, newArray.Length);
        }
        
        [Fact]
        public static void TestCopyPartOfTheSpanToOtherArray()
        {
            
            var array = new byte[] {0, 1, 2, 3, 4, 5};
            
            var arraySpan = array.ToByteArraySpan(1, 4);

            var newArray = new byte[3];
            
            arraySpan.CopyToArray(1, newArray, 0,3);

            Assert.Equal(3, newArray.Length);
            Assert.Equal(2, newArray[0]);
            Assert.Equal(3, newArray[1]);
            Assert.Equal( 4, newArray[2]);
            
        }        
        
    }
    */
}