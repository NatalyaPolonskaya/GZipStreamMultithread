using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GZipStreamMultithread
{
    public class DataBlock
    {
        public DataBlock(int id, byte[] data)
        {
            this.ID = id;
            this.Data = data;
        }
        public int ID { get; set; }
        public byte[] Data { get; set; }
    }
}
