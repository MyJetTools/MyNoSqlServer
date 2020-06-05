using System;

namespace MyNoSqlServer.Common
{
    public class StringSpan
    {
        private readonly string _src;

        private readonly int _substringEnd;
        public StringSpan(string src)
        {
            _src = src;
            PositionStart = 0;
            PositionEnd = 0;
            _substringEnd = src.Length;
        }
        
        public int PositionStart { get; private set; }
        
        public int PositionEnd { get; private set; }

        public bool Eof => PositionStart >= _substringEnd && PositionEnd >= _substringEnd;
        
        /// <summary>
        ///  Move start position relative to End Position
        /// </summary>
        /// <param name="conditionToStop">condition - where do we stop movement</param>
        /// <param name="skipEscapeSequence">Escape sequence for string quotes</param> 
        /// <param name="offset">offset from End Position</param>
        /// <returns>EOF</returns>        
        public bool MoveStartPosition(string skipEscapeSequence, Func<char, bool> conditionToStop, int offset = 0)
        {
            for (var i = PositionEnd + offset; i < _substringEnd; i++)
            {
                if (conditionToStop(_src[i]))
                {
                    PositionStart = i;
                    return true;
                }
            }

            PositionStart = _substringEnd;
            return false;
        }


        /// <summary>
        ///  Move End position relative to start position
        /// </summary>
        /// <param name="conditionToStop">condition - where do we stop movement</param>
        /// <param name="skipEscapeSequence">Escape sequence for string quotes</param>
        /// <param name="offset">offset from Position Start</param>
        /// <returns>EOF</returns>
        public bool MoveEndPosition(string skipEscapeSequence, Func<char, bool> conditionToStop,  int offset = 0)
        {
            for (var i = PositionStart+offset; i < _substringEnd; i++)
            {
                if (conditionToStop(_src[i]))
                {
                    PositionEnd = i;
                    return true;
                }
            }

            PositionEnd = _substringEnd;
            return false;
        }
        
        /// <summary>
        ///  Move End position relative to start position
        /// </summary>
        /// <param name="conditionToStop">condition - where do we stop movement</param>
        /// <param name="offset">offset from Position Start</param>
        /// <returns>EOF</returns>
        public bool MoveEndPosition(Func<char, bool> conditionToStop,  int offset = 0)
        {
            for (var i = PositionStart+offset; i < _substringEnd; i++)
            {
                if (conditionToStop(_src[i]))
                {
                    PositionEnd = i;
                    return true;
                }
            }

            PositionEnd = _substringEnd;
            return false;
        }

        public void SyncStartWithEnd(int offset = 0)
        {
            PositionStart = PositionEnd + offset;
        }

        public void SyncEndWithStart(int offset = 0)
        {
            PositionEnd = PositionStart + offset;
        }

        public void MoveStartPosition(int offset)
        {
            PositionStart += offset;
        }

        public void MoveEndPosition(int offset)
        {
            PositionEnd += offset;
        }

        public string GetCurrentValue(string escapeSequence, string valueOfEscapeSequence)
        {
            return _src.Substring(PositionStart, PositionEnd - PositionStart).Replace(escapeSequence, valueOfEscapeSequence);
        }

        public char CurrentChar => _src[PositionStart];
        
        public string GetCurrentValue()
        {
            return _src.Substring(PositionStart, PositionEnd - PositionStart);
        }

        public override string ToString()
        {
            return _src.Substring(PositionStart, _substringEnd-PositionStart)+" | "+_src.Substring(PositionEnd, _substringEnd-PositionEnd);
        }
        
    }
}