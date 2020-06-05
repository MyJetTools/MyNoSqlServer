using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Domains.Query
{

    public enum QueryOperation
    {
        Eq, Gt, Lt, Ge, Le, Ne, In, Bw
    }

    public class QueryCondition
    {
        public string FieldName { get; set; }
        public QueryOperation Operation { get; set; }
        internal string[] Values { get; set; }


        public string AsString(int indexOf)
        {
            var result = Values[indexOf];

            if (result[0] == '\'' && result[result.Length - 1] == '\'')
                return result.Substring(1, result.Length - 2);

            return result;
        }
    }

    public static class QueryBuilder
    {


        private static QueryOperation ParseQueryOperation(this string op)
        {
            op = op.ToLower();

            switch (op)
            {
                case "eq":
                case "==":
                case "=":
                    return QueryOperation.Eq;

                case "gt":
                case ">":
                    return QueryOperation.Gt;
                
                case "lt":
                case "<":
                    return QueryOperation.Lt;
                
                case "ge":
                case ">=":
                    return QueryOperation.Ge;
                
                case "le":
                case "<=":
                    return QueryOperation.Le;
                
                case "ne":
                case "!=":
                case "<>":
                    return QueryOperation.Ne;
                
                case "in":
                    return QueryOperation.In;
                
                case "between":
                    return QueryOperation.Bw;
                default:
                    throw new Exception("Invalid query Operation");
            }
        }

        private const string EscapeSequence = "''";
        private const char CharToEscape = '\'';
        private const string StringToEscape = "'";

        private static void MoveToTheNextStartOfString(this StringSpan stringSpan)
        {
            stringSpan.MoveStartPosition(EscapeSequence, c => c > ' ');
        }

        private static QueryOperation ReadOperation(this StringSpan src)
        {
            var opString = src.ReadNextString();
            return opString.ParseQueryOperation();
        }

        private static string ReadNextString(this StringSpan stringSpan)
        {
            stringSpan.MoveToTheNextStartOfString();

            var isString = stringSpan.CurrentChar == CharToEscape;

            if (isString){
                stringSpan.MoveEndPosition(EscapeSequence, c => c == CharToEscape, 1);
                stringSpan.MoveEndPosition(1);
                return stringSpan.GetCurrentValue(EscapeSequence, StringToEscape);
            }
            
            stringSpan.MoveEndPosition(EscapeSequence, c => c <= ' ');
            return stringSpan.GetCurrentValue();
        }
        
        private static string ReadNextStringFromArray(this StringSpan stringSpan)
        {
            stringSpan.MoveToTheNextStartOfString();

            var isString = stringSpan.CurrentChar == CharToEscape;

            if (isString){
                stringSpan.MoveEndPosition(EscapeSequence, c => c == CharToEscape, 1);
                stringSpan.MoveEndPosition(1);
                return stringSpan.GetCurrentValue(EscapeSequence, StringToEscape);
            }
            
            stringSpan.MoveEndPosition(EscapeSequence, c => c <= ' ' || c == ',' || c==']');
            return stringSpan.GetCurrentValue();
        }

        private static IEnumerable<string> ReadInValues(this StringSpan stringSpan)
        {
            
            stringSpan.MoveToTheNextStartOfString();

            var arrayIsOpened = stringSpan.CurrentChar == '[';
            
            if (!arrayIsOpened)
                throw new Exception("Invalid int operation at position: "+stringSpan.PositionStart);

            stringSpan.MoveStartPosition(1);
            stringSpan.SyncEndWithStart();
            
            while (!stringSpan.Eof)
            {
                var value = stringSpan.ReadNextStringFromArray();
                yield return value;

                stringSpan.SyncStartWithEnd();

                if (stringSpan.CurrentChar <= ' ')
                {
                    stringSpan.MoveToTheNextStartOfString();
                    stringSpan.SyncEndWithStart();
                }
                
                if (stringSpan.CurrentChar == ']')
                {
                    stringSpan.MoveStartPosition(1);
                    stringSpan.SyncEndWithStart();
                    break;
                }
                
                if (stringSpan.CurrentChar != ',')
                    stringSpan.MoveStartPosition(EscapeSequence, c => c == ',' || c==']');

                stringSpan.MoveStartPosition(1);
                stringSpan.SyncEndWithStart();
            }
            
            

        }



        public static IEnumerable<QueryCondition> ParseQueryConditions(this string query)
        {

            var stringSpan = new StringSpan(query);
            
            stringSpan.MoveToTheNextStartOfString();

            while (!stringSpan.Eof)
            {
               
                var fieldName = stringSpan.ReadNextString();
                

                var operation = stringSpan.ReadOperation();



                if (operation == QueryOperation.In)
                {
                    var value =  stringSpan.ReadInValues();
                    
                    yield return new QueryCondition
                    {
                        FieldName = fieldName,
                        Values = value.ToArray(),
                        Operation = operation
                    };
                    
                }
                else
                {
                    var value =  stringSpan.ReadNextString();
                
                    yield return new QueryCondition
                    {
                        FieldName = fieldName,
                        Values = value.ToSingleArray(),
                        Operation = operation
                    };
                    
                }

                stringSpan.MoveToTheNextStartOfString();
                
                if (stringSpan.Eof)
                    break;


                var logicalOperator = stringSpan.ReadNextString();
                
                if (logicalOperator.ToLower() != "and" )
                    throw new Exception("Only and logical operation is supported for a while");
            }

        }
        
        

    }
}