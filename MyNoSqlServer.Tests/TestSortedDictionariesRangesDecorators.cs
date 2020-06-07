using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Common;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestSortedDictionariesRangesDecorators
    {
        
        private static readonly SortedList<string, string> SmallAmount = new SortedList<string, string>
        {
            ["00"] = "00",
            ["02"] = "02",
            ["04"] = "04",
            ["06"] = "06",
            ["08"] = "08",
        };
        
        private static readonly SortedList<string, string> LargeAmount = new SortedList<string, string>
        {
            ["00"] = "00",
            ["02"] = "02",
            ["04"] = "04",
            ["06"] = "06",
            ["08"] = "08",
            ["10"] = "10",
            ["12"] = "12",
            ["14"] = "14",
            ["16"] = "16",
            ["18"] = "18",
            ["20"] = "20",
            ["22"] = "22",
            ["24"] = "24",
            ["26"] = "26",
            ["28"] = "28",
            ["30"] = "30",
            ["32"] = "32",
        };
        

        [Test]
        public void TestBetweenWithMinimumAmountToSearch()
        {

            var result = SmallAmount.GetRange("02", "06", RangeInclude.Both).ToArray();
            
            Assert.AreEqual(3,result.Length);
            
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            Assert.AreEqual("06", result[2].Value);
            
            result = SmallAmount.GetRange("01", "06", RangeInclude.Both).ToArray();
            
            Assert.AreEqual(3,result.Length);
            
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            Assert.AreEqual("06", result[2].Value);
            
            
            result = SmallAmount.GetRange("02", "06", RangeInclude.Lower).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            
            result = SmallAmount.GetRange("02", "06", RangeInclude.Higher).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("04", result[0].Value);
            Assert.AreEqual("06", result[1].Value);
        }
        
        [Test]
        public void TestBetweenWithLargeAmountToSearch()
        {

            var result = LargeAmount.GetRange("11", "15", RangeInclude.Both).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("12", result[0].Value);
            Assert.AreEqual("14", result[1].Value);
            
            
            
            result = LargeAmount.GetRange("02", "05", RangeInclude.Lower).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            
            
            result = LargeAmount.GetRange("02", "06", RangeInclude.Lower).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            
            result = LargeAmount.GetRange("01", "06", RangeInclude.Lower).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            
            
            result = LargeAmount.GetRange("10", "14", RangeInclude.Higher).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("12", result[0].Value);
            Assert.AreEqual("14", result[1].Value);

            result = LargeAmount.GetRange("11", "14", RangeInclude.Higher).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("12", result[0].Value);
            Assert.AreEqual("14", result[1].Value);
            
            result = LargeAmount.GetRange("11", "15", RangeInclude.Higher).ToArray();
            
            Assert.AreEqual(2,result.Length);
            
            Assert.AreEqual("12", result[0].Value);
            Assert.AreEqual("14", result[1].Value);
 
        }

        [Test]
        public void GetLowerRangeWithSmallAmountOfItems()
        {
            var result = SmallAmount.GetLowerRange( "06", true).ToArray();
            
            Assert.AreEqual(4,result.Length);
            
             
            Assert.AreEqual("00", result[0].Value);
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
            
            result = SmallAmount.GetLowerRange( "07", true).ToArray();
            
            Assert.AreEqual(4,result.Length);
            
             
            Assert.AreEqual("00", result[0].Value);
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
            
            
            result = SmallAmount.GetLowerRange( "06", false).ToArray();
            
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("00", result[0].Value);
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            
            result = SmallAmount.GetLowerRange( "05", false).ToArray();
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("00", result[0].Value);
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);            
        }
        
        [Test]
        public void GetLowerRangeWithLargeAmountOfItems()
        {
            var result = LargeAmount.GetLowerRange( "08", true).ToArray();
            
            Assert.AreEqual(5,result.Length);
            
            Assert.AreEqual("00", result[0].Value);             
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
            Assert.AreEqual("08", result[4].Value);
            
            result = LargeAmount.GetLowerRange( "09", true).ToArray();
            
            Assert.AreEqual(5,result.Length);
            
            Assert.AreEqual("00", result[0].Value);             
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
            Assert.AreEqual("08", result[4].Value);
            
            result = LargeAmount.GetLowerRange( "07", true).ToArray();
            
            Assert.AreEqual(4,result.Length);
            
            Assert.AreEqual("00", result[0].Value);             
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
            
            result = LargeAmount.GetLowerRange( "08", false).ToArray();

            Assert.AreEqual(4,result.Length);
             
            Assert.AreEqual("00", result[0].Value);             
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
            
            result = LargeAmount.GetLowerRange( "09", false).ToArray();

            Assert.AreEqual(5,result.Length);
             
            Assert.AreEqual("00", result[0].Value);             
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
            Assert.AreEqual("08", result[4].Value);
            
            result = LargeAmount.GetLowerRange( "07", false).ToArray();

            Assert.AreEqual(4,result.Length);
             
            Assert.AreEqual("00", result[0].Value);             
            Assert.AreEqual("02", result[1].Value);
            Assert.AreEqual("04", result[2].Value);
            Assert.AreEqual("06", result[3].Value);
        }
        
        [Test]
        public void GetHigherRangeWithSmallAmountOfItems()
        {
            var result = SmallAmount.GetGreaterRange( "02", true).ToArray();
            
            Assert.AreEqual(4,result.Length);
            
             
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            Assert.AreEqual("06", result[2].Value);
            Assert.AreEqual("08", result[3].Value);
            
            result = SmallAmount.GetGreaterRange( "01", true).ToArray();
            
            Assert.AreEqual(4,result.Length);
            
             
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            Assert.AreEqual("06", result[2].Value);
            Assert.AreEqual("08", result[3].Value);
            
            result = SmallAmount.GetGreaterRange( "03", true).ToArray();
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("04", result[0].Value);
            Assert.AreEqual("06", result[1].Value);
            Assert.AreEqual("08", result[2].Value);            
            
            result = SmallAmount.GetGreaterRange( "02", false).ToArray();
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("04", result[0].Value);
            Assert.AreEqual("06", result[1].Value);
            Assert.AreEqual("08", result[2].Value);
            
            result = SmallAmount.GetGreaterRange( "03", false).ToArray();
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("04", result[0].Value);
            Assert.AreEqual("06", result[1].Value);
            Assert.AreEqual("08", result[2].Value);
            
            result = SmallAmount.GetGreaterRange( "01", false).ToArray();
            
            Assert.AreEqual(4,result.Length);
             
            Assert.AreEqual("02", result[0].Value);
            Assert.AreEqual("04", result[1].Value);
            Assert.AreEqual("06", result[2].Value);
            Assert.AreEqual("08", result[3].Value);
            
            
        }

        [Test] 
        public void GetHigherRangeWithLargeAmountOfItems()
        {
            var result = LargeAmount.GetGreaterRange( "26", true).ToArray();
            
            Assert.AreEqual(4,result.Length);
             
            Assert.AreEqual("26", result[0].Value);
            Assert.AreEqual("28", result[1].Value);
            Assert.AreEqual("30", result[2].Value);
            Assert.AreEqual("32", result[3].Value);
            
            result = LargeAmount.GetGreaterRange( "25", true).ToArray();
            
            Assert.AreEqual(4,result.Length);
             
            Assert.AreEqual("26", result[0].Value);
            Assert.AreEqual("28", result[1].Value);
            Assert.AreEqual("30", result[2].Value);
            Assert.AreEqual("32", result[3].Value);

            result = LargeAmount.GetGreaterRange( "27", true).ToArray();
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("28", result[0].Value);
            Assert.AreEqual("30", result[1].Value);
            Assert.AreEqual("32", result[2].Value);
            
            
            result = LargeAmount.GetGreaterRange( "26", false).ToArray();
            
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("28", result[0].Value);
            Assert.AreEqual("30", result[1].Value);
            Assert.AreEqual("32", result[2].Value);
            
            result = LargeAmount.GetGreaterRange( "25", false).ToArray();
            
            
            Assert.AreEqual(4,result.Length);
             
            Assert.AreEqual("26", result[0].Value);
            Assert.AreEqual("28", result[1].Value);
            Assert.AreEqual("30", result[2].Value);
            Assert.AreEqual("32", result[3].Value);
            
            result = LargeAmount.GetGreaterRange( "27", false).ToArray();
            
            
            Assert.AreEqual(3,result.Length);
             
            Assert.AreEqual("28", result[0].Value);
            Assert.AreEqual("30", result[1].Value);
            Assert.AreEqual("32", result[2].Value);
        }

    }
    
}