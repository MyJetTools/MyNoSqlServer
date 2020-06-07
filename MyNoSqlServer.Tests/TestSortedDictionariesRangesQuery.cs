using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Query;
using NUnit.Framework;

namespace MyNoSqlServer.Tests
{
    public class TestSortedDictionariesRangesQuery
    {
        private static readonly SortedList<string, string> Data = new SortedList<string, string>
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
        public void TestEqualCondition()
        {
            const string query = "PartitionKey eq '08'";

            var conditions = query.ParseQueryConditions().ToArray();

            var result = Data.FilterByQueryConditions(conditions).ToArray();

            Assert.AreEqual(1, result.Length);
            Assert.AreEqual("08", result[0]);

        }
    }
}