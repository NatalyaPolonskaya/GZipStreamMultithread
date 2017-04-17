using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GZipStreamMultithread
{
    public class GZipMultithread
    {
        private ConcurrentStack<Thread> _compressors = new ConcurrentStack<Thread>();

        public ConcurrentStack<DataBlock> _tasks = new ConcurrentStack<DataBlock>();

        public ConcurrentStack<DataBlock> _results = new ConcurrentStack<DataBlock>();

        private Thread _writer;
        public GZipMultithread()
        {
            var maxThreadNumber = Environment.ProcessorCount;
            for (int i = 0; i < maxThreadNumber; i++)
            {
                var compressor = new Thread(Compress);
                _compressors.Push(compressor);
                compressor.Start();
            }
            _writer = new Thread(WriteToFile);
        }

        private void WriteToFile(object obj)
        {
            MemoryStream memory = obj as MemoryStream;
            FileStream file = File.Open("govno.gz", FileMode.OpenOrCreate, FileAccess.Write);
            memory.CopyTo(file);
        }

        public void Execute()
        {
            // delete idle threads?
        }

        public void AddTask(DataBlock task)
        {
            _tasks.Push(task);
        }

        private DataBlock GetTask()
        {
            DataBlock result;
            int tryNumber = 0;
            while (!_tasks.TryPop(out result))
            {
                tryNumber++;
                //*(new Random()).Next(10)
                Thread.Sleep(100*tryNumber);
            }
            return result;
        }

        public void AddTaskRange(DataBlock[] tasks)
        {
            _tasks.PushRange(tasks);
        }

        private void Compress()
        {
            var task = GetTask();
            while (task != null)
            {
                     //MemoryStream archiveFS = new MemoryStream();

                DataBlock tmpdb;// = new DataBlock();
                using (MemoryStream fileFS = new MemoryStream(task.Data))
                {
                    //using (FileStream archiveFS = File.Create("archive.gz"))

                    using (MemoryStream archiveFS = new MemoryStream())
                    {

                        using (GZipStream compressionStream = new GZipStream(archiveFS,
                           CompressionMode.Compress))
                        {
                            fileFS.CopyTo(compressionStream);
                            
                            byte[] buffer = archiveFS.ToArray();//GetBuffer();
                            //GZipStream cs = new GZipStream(file, CompressionMode.Compress);
                            //cs.Write(buffer, 0, buffer.Length);
                            ////byte[] buffer = new byte[archiveFS.Length];
                            ////archiveFS.Position = 0;
                            ////archiveFS.Read(buffer, 0, buffer.Length);
                            tmpdb = new DataBlock(task.ID, buffer);
                            this.AddResult(tmpdb);
                            
                        }
                        

                    }
                }
                using (FileStream fileFS = File.Open("govno.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite))
                {
                    //using (MemoryStream archiveFS = new MemoryStream())

                    using (FileStream archiveFS = File.Open("archive.gz", FileMode.Append, FileAccess.Write))
                    {

                        using (GZipStream compressionStream = new GZipStream(archiveFS,
                           CompressionMode.Compress))
                        {
                            fileFS.CopyTo(compressionStream);
                        }
                    }
                }
                //byte[] source = tmpdb.Data.ToArray();
                byte[] source = File.ReadAllBytes("archive.gz");
                byte[] test = Decompress(source);

                try
                {
                }
                catch (OutOfMemoryException)
                {
                    this.AddTask(task);
                    GC.Collect();
                    //memory = new MemoryStream();
                    //gzip = new GZipStream(memory, CompressionMode.Compress);

                    //archiveFS = File.Create("tmp.gz");
                    //gzip = new GZipStream(archiveFS, CompressionMode.Compress);
                }
                task = GetTask();
            }
        }

        static byte[] Decompress(byte[] gzip)
        {
            using (GZipStream stream = new GZipStream(new MemoryStream(gzip),
                CompressionMode.Decompress))
            {
                const int size = 4096;
                byte[] buffer = new byte[size];
                using (MemoryStream memory = new MemoryStream())
                {
                    int count = 0;
                    do
                    {
                        count = stream.Read(buffer, 0, size);
                        if (count > 0)
                        {
                            memory.Write(buffer, 0, count);
                        }
                    }
                    while (count > 0);
                    return memory.ToArray();
                }
            }
        }

        private void AddResult(DataBlock dataBlock)
        {
            _results.Push(dataBlock);
        }

        public void Dispose()
        {
            foreach (var item in this._compressors)
            {
                item.Abort();
            }
        }
    }
}
