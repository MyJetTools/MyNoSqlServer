using System;
using System.Collections.Generic;
using MyNoSqlServer.Common;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    public class TestStringComparator
    {
        
        [Fact]
        public void TestStringComparators()
        {
            Assert.True("01".LowerThen("02"));
            Assert.True("01".LowerOrEqualThen("02"));
            Assert.True("01".LowerOrEqualThen("01"));
            Assert.False("02".LowerOrEqualThen("01")); 
            
            Assert.True("02".GreaterThen("01"));
            Assert.True("02".GreaterOrEqualThen("01"));
            Assert.True("01".GreaterOrEqualThen("01"));
            Assert.False("01".GreaterOrEqualThen("02"));
                        
            Assert.True("05".RangeBetweenIncludingBoth("04","06"));
            Assert.True("04".RangeBetweenIncludingBoth("04","06"));
            Assert.True("06".RangeBetweenIncludingBoth("04","06"));

            Assert.False("04".RangeBetweenExcludingBoth("04","06"));
            Assert.True("05".RangeBetweenExcludingBoth("04","06"));
            Assert.False("06".RangeBetweenExcludingBoth("04","06"));
            
            Assert.True("04".RangeBetweenIncludingLower("04","06"));
            Assert.True("05".RangeBetweenIncludingLower("04","06"));
            Assert.False("06".RangeBetweenIncludingLower("04","06"));

            Assert.False("04".RangeBetweenIncludingHigher("04","06"));
            Assert.True("05".RangeBetweenIncludingHigher("04","06"));
            Assert.True("06".RangeBetweenIncludingHigher("04","06"));
        }
        
    }

}