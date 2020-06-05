using System;
using System.Collections.Generic;
using System.Text;

namespace MyNoSqlServer.Common
{
    public static class Utils
    {

        public static int ParseIntOrDefault(this string line, int @default = 0)
        {
            try
            {
                return int.Parse(line);
            }
            catch (Exception)
            {
                return @default;
            }
        }


        public static string AddLastSymbolIfOneNotExists(this string line, char theChar)
        {
            if (line == null)
                return line;

            if (line == string.Empty)
                return string.Empty+theChar;


            if (line[line.Length - 1] == theChar)
                return line;

            return line + theChar;

        }


        public static string ToBase64(this string src)
        {
            var bytes = Encoding.UTF8.GetBytes(src);
            return Convert.ToBase64String(bytes);
        }
        
        public static string Base64ToString(this string src)
        {            
            var bytes = Convert.FromBase64String(src);;
            return Encoding.UTF8.GetString(bytes);
        }

        public static T[] ToSingleArray<T>(this T value)
        {
            return new[] {value};
        }

        
        public static bool RangeBetweenIncludingBoth(this string value, string keyFrom, string keyTo)
        {
            return string.CompareOrdinal(value, keyFrom) >= 0 && string.CompareOrdinal(value, keyTo) <= 0;
        }
        
        public static bool RangeBetweenExcludingBoth(this string value, string keyFrom, string keyTo)
        {
            return string.CompareOrdinal(value, keyFrom) > 0 && string.CompareOrdinal(value, keyTo) < 0;
        }
        
        public static bool RangeBetweenIncludingLower(this string value, string keyFrom, string keyTo)
        {
            return string.CompareOrdinal(value, keyFrom) >= 0 && string.CompareOrdinal(value, keyTo) < 0;
        }
        
        public static bool RangeBetweenIncludingHigher(this string value, string keyFrom, string keyTo)
        {
            return string.CompareOrdinal(value, keyFrom) > 0 && string.CompareOrdinal(value, keyTo) <= 0;
        }


        public static bool GreaterThen(this string greaterThen, string thisOne)
        {
            return string.CompareOrdinal(greaterThen, thisOne) > 0;
        }
        
        public static bool GreaterOrEqualThen(this string greaterAndEqualThen, string thisOne)
        {
            return string.CompareOrdinal(greaterAndEqualThen, thisOne) >= 0;
        }
        
        public static bool LowerThen(this string lowerThen, string thisOne)
        {
            return string.CompareOrdinal(lowerThen, thisOne) < 0;
        }
        
        public static bool LowerOrEqualThen(this string lowerAndEqualThen, string thisOne)
        {
            return string.CompareOrdinal(lowerAndEqualThen, thisOne) <= 0;
        }


        public static Queue<T> ToQueue<T>(this IEnumerable<T> src)
        {
            var result = new Queue<T>();
            foreach (var itm in src)
            {
                result.Enqueue(itm);
            }

            return result;
        }


        public static string AsString(this in ReadOnlyMemory<byte> src)
        {
            return Encoding.UTF8.GetString(src.Span);
        }
        


        
    }
    
}