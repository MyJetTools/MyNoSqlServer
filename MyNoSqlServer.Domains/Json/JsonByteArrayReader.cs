using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Domains.Json
{

    public enum ExpectedToken
    {
        OpenBracket, OpenKey, CloseKey, DoubleColumn, OpenValue, CloseStringValue, CloseNumberOrBoolValue, CloseObject, CloseArray, Comma, EndOfFile
    }

    public static class JsonByteArrayReader
    {
        
        public const byte OpenBracket = (byte) '{';
        public const byte CloseBracket = (byte) '}';
        public const byte DoubleQuote = (byte) '"';
        public const byte DoubleColumn = (byte) ':';
        public const byte OpenArray = (byte) '[';
        public const byte CloseArray = (byte) ']';            
        public const byte Comma = (byte) ',';
        private const byte EscSymbol = (byte) '\\';
        
        private static readonly Dictionary<char, char> StartOfDigit = new Dictionary<char, char>
        {
            ['0']='0',
            ['1']='1',
            ['2']='2',
            ['3']='3',
            ['4']='4',
            ['5']='5',
            ['5']='5',
            ['6']='6',
            ['7']='7',
            ['8']='8',
            ['9']='9',
            ['-']='-'
        };

        private static bool IsSpace(this byte c)
        {
            return c <= 32;
        }

        private static bool IsStartOfBool(this byte c)
        {
            return c == (byte) 't' || c == (byte) 'f' || c == (byte) 'T' || c == (byte) 'F' || c == (byte)'n' || c == (byte)'N';
        }


        private static void ThrowException(this IMyMemory byteArray, int position)
        {
            var i = position - 10;
            if (i < 0)
                i = 0;

            var str = Encoding.UTF8.GetString(byteArray.Slice(i, position-i).Span);

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(str);
            Console.ResetColor();
            
            throw new Exception("Invalid Json at position: "+str);
        }


        public static Dictionary<string, IJsonFirstLine> ParseFirstLine(this IMyMemory inData)
        {
                        var result = new Dictionary<string, IJsonFirstLine>();
            
            var expectedToken = ExpectedToken.OpenBracket;


            var subObjectLevel = 0;
            var subObjectString = false;

            var keyStartIndex = 0;
            var keyEndIndex = 0;

            var valueStartIndex = 0;

            var skipItems = 0;

            
            foreach (var (b, index) in inData.Enumerate())
            {

                if (skipItems > 0)
                {
                    skipItems--;
                    continue;
                }
                
                if (expectedToken == ExpectedToken.EndOfFile)
                    break;

                switch (expectedToken)
                {
                    case ExpectedToken.OpenBracket:
                        if (b.IsSpace())
                            continue;
                        if (b != OpenBracket)
                            inData.ThrowException(index);

                        expectedToken = ExpectedToken.OpenKey;
                        break;

                    case ExpectedToken.OpenKey:
                        if (b == CloseBracket)
                        {
                            expectedToken = ExpectedToken.EndOfFile;
                            break;
                        }

                        if (b.IsSpace())
                            continue;

                        if (b != DoubleQuote)
                            inData.ThrowException(index);

                        keyStartIndex = index;
                        expectedToken = ExpectedToken.CloseKey;
                        break;

                    case ExpectedToken.CloseKey:
                        switch (b)
                        {
                            case EscSymbol:
                                skipItems++;
                                break;
                            case DoubleQuote:
                                keyEndIndex = index + 1;
                                expectedToken = ExpectedToken.DoubleColumn;
                                break;
                        }

                        break;

                    case ExpectedToken.DoubleColumn:
                        if (b.IsSpace())
                            continue;

                        if (b != DoubleColumn)
                            inData.ThrowException(index);

                        expectedToken = ExpectedToken.OpenValue;
                        break;

                    case ExpectedToken.OpenValue:
                        if (b.IsSpace())
                            continue;

                        valueStartIndex = index;

                        switch (b)
                        {
                            case OpenArray:
                                expectedToken = ExpectedToken.CloseArray;
                                break;
                            case DoubleQuote:
                                expectedToken = ExpectedToken.CloseStringValue;
                                break;
                            case OpenBracket:
                                subObjectLevel = 0;
                                subObjectString = false;
                                expectedToken = ExpectedToken.CloseObject;
                                break;
                            default:
                            {
                                if (StartOfDigit.ContainsKey((char) b) || b.IsStartOfBool())
                                    expectedToken = ExpectedToken.CloseNumberOrBoolValue;
                                else
                                    inData.ThrowException(index);

                                break;
                            }
                        }

                        break;

                    case ExpectedToken.CloseStringValue:
                        switch (b)
                        {
                            case EscSymbol:
                                skipItems++;
                                break;
                            case DoubleQuote:
                                var item =  new MyJsonFirstLevelFieldData (
                                    inData.Slice(keyStartIndex, keyEndIndex-keyStartIndex),
                                    inData.Slice(valueStartIndex, index + 1 - valueStartIndex));
                                
                                result.Add(item.Name, item);
                                
                                expectedToken = ExpectedToken.Comma;
                                break;
                        }

                        break;

                    case ExpectedToken.CloseNumberOrBoolValue:
                        if (b is Comma or CloseBracket || b.IsSpace())
                        {
                            var item = new MyJsonFirstLevelFieldData(
                                inData.Slice(keyStartIndex, keyEndIndex-keyStartIndex),
                                inData.Slice(valueStartIndex, index-valueStartIndex));
                            result.Add(item.Name, item);
                            if (b == CloseBracket)
                                expectedToken = ExpectedToken.EndOfFile;
                            else
                                expectedToken = b == Comma ? ExpectedToken.OpenKey : ExpectedToken.Comma;
                        }

                        break;

                    case ExpectedToken.Comma:
                        if (b.IsSpace())
                            continue;
                        if (b == CloseBracket)
                        {
                            expectedToken = ExpectedToken.EndOfFile;
                            continue;
                        }

                        if (b != Comma)
                            inData.ThrowException(index);

                        expectedToken = ExpectedToken.OpenKey;
                        continue;

                    case ExpectedToken.CloseObject:
                        if (subObjectString)
                        {
                            switch (b)
                            {
                                case EscSymbol:
                                    skipItems++;
                                    continue;
                                case DoubleQuote:
                                    subObjectString = false;
                                    break;
                            }
                        }
                        else
                        {
                            switch (b)
                            {
                                case DoubleQuote:
                                    subObjectString = true;
                                    continue;
                                case OpenBracket:
                                    subObjectLevel++;
                                    continue;
                                case CloseBracket when subObjectLevel == 0:
                                    var item = new MyJsonFirstLevelFieldData(
                                        inData.Slice(keyStartIndex, keyEndIndex-keyStartIndex),
                                        inData.Slice(valueStartIndex, index + 1 - valueStartIndex));
                                    result.Add(item.Name, item);
                                    expectedToken = ExpectedToken.Comma;
                                    break;
                                case CloseBracket:
                                    subObjectLevel--;
                                    break;
                            }
                        }

                        break;

                    case ExpectedToken.CloseArray:
                        if (subObjectString)
                        {
                            switch (b)
                            {
                                case EscSymbol:
                                    skipItems++;
                                    continue;
                                case DoubleQuote:
                                    subObjectString = false;
                                    break;
                            }
                        }
                        else
                        {
                            switch (b)
                            {
                                case DoubleQuote:
                                    subObjectString = true;
                                    continue;
                                case OpenArray:
                                    subObjectLevel++;
                                    continue;
                                case CloseArray when subObjectLevel == 0:
                                    var item = new MyJsonFirstLevelFieldData(
                                        inData.Slice(keyStartIndex, keyEndIndex-keyStartIndex),
                                        inData.Slice(valueStartIndex, index + 1 - valueStartIndex));
                                    result.Add(item.Name, item);
                                    expectedToken = ExpectedToken.Comma;
                                    break;
                                case CloseArray:
                                    subObjectLevel--;
                                    break;
                            }
                        }

                        break;

                }

            }

            if (expectedToken != ExpectedToken.EndOfFile)
                throw new Exception("Invalid Json");


            return result;
        }
        
        

        public static DynamicEntity ParseDynamicEntity(this byte[] rawData)
        {
            return new MyMemoryAsByteArray(rawData).ParseDynamicEntity();
        }

        public static DynamicEntity ParseDynamicEntity(
            this IMyMemory inData)
        {
            var result = inData.ParseFirstLine();
            return new DynamicEntity(result);
        }

        private static byte[] MergeToArray(this IReadOnlyList<ReadOnlyMemory<byte>> list, int startIndex, int endIndex)
        {
            var resultLen = list.Skip(1).Take(list.Count - 2).Sum(resItm => resItm.Length) 
                            + list[0].Length - startIndex
                                                         + endIndex+1;

            var result = new byte[resultLen];

            var i = 0;

            foreach (var b in list[0].Slice(startIndex, list[0].Length - startIndex).Span)
            {
                result[i] = b;
                i++;
            }

            for (var j = 1; j < list.Count - 1; j++)
                foreach (var b in list[j].Span)
                {
                    result[i] = b;
                    i++;
                }


            var last = list[^1];
            
            foreach (var b in last[..(endIndex+1)].Span)
            {
                result[i] = b;
                i++;
            }

            return result;
        }



        public static IEnumerable<IMyMemory> SplitJsonArrayToObjects(this IMyMemory seq)
        {
            var objectLevel = 0;
            

            var insideString = false;
            var escapeMode = false;

            var startIndex = -1;
            
    

            foreach (var (itm, i) in seq.Enumerate())
            {

                    if (escapeMode)
                    {
                        escapeMode = false;
                        continue;
                    }

                    switch (itm)
                    {

                        case (byte) '\\':
                            if (insideString)
                                escapeMode = true;
                            break;

                        case (byte) '"':
                            insideString = !insideString;
                            break;

                        case (byte) '{':
                            if (!insideString)
                            {
                                objectLevel++;
                                if (objectLevel == 1)
                                {
                                    startIndex = i;
                                }
                                    
                            }

                            break;

                        case (byte) '}':
                            if (!insideString)
                            {
                                objectLevel--;
                                if (objectLevel == 0)
                                {
                                    yield return seq.Slice(startIndex, i-startIndex+1).AsMyMemory();
                                }
                            }

                            break;
                    }

            }

        }

        public static byte[] AsDbRowJson(this DynamicEntity entity)
        {
            return entity.Raw.AsDbRowJson();
        }
        
        
        public static byte[] AsDbRowJson(this Dictionary<string, IJsonFirstLine> src)
        {

            var len = 2;
            var secondValue = false;

            foreach (var fieldData in src.Values)
            {
                if (secondValue)
                    len++;
                else
                    secondValue = true;

                len += fieldData.Name.Length + fieldData.Value.Length + 1;
            }

            var result = new byte[len];

            var i = 0;

            result[i++] = OpenBracket;
            secondValue = false;
            
            foreach (var fieldData in src.Values)
            {
                if (secondValue)
                    result[i++] = Comma;
                else
                    secondValue = true;
                
                foreach (var c in fieldData.Name.Span)
                    result[i++] = c;
                
                result[i++] = DoubleColumn;

                foreach (var c in fieldData.Value.Span)
                    result[i++] = c;    
                
            }

            result[i] = CloseBracket;
            return result.ToArray();

        }
        

        public static string AsJsonString(this in ReadOnlyMemory<byte> src)
        {
            if (src.IsNull())
                return null;



            var mySpan = src.Span[0] == DoubleQuote && src.Span[src.Length-1] == DoubleQuote
                ? src.Slice(1, src.Length - 2).Span
                : src.Span;

            var result = mySpan.Length == 0 
                ? string.Empty
                : Encoding.UTF8.GetString(mySpan);

            return result;
        }
    }
}