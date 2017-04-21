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

        private ConcurrentQueue<DataBlock> _tasks = new ConcurrentQueue<DataBlock>();
        public int CurrentTaskCount { get { return this._tasks.Count; } }

        private ConcurrentDictionary<int, byte[]> _results = new ConcurrentDictionary<int, byte[]>();
        public int CurrentResultCount { get { return this._results.Count; } }

        public long TaskCount;
        public long ResultCount;

        public int MaxTaskNumber { get; set; }
        public int MaxResultNumber { get; set; }

        public ManualResetEvent StopRead = new ManualResetEvent(true);

        public GZipMultithread(CompressionMode mode)
        {
            var maxThreadNumber = Environment.ProcessorCount;
            var taskNumberPerThread = 32;
            this.MaxTaskNumber = taskNumberPerThread * maxThreadNumber;
            var resultNumberScale = 2;
            this.MaxResultNumber = this.MaxTaskNumber * resultNumberScale;
            for (int i = 0; i < maxThreadNumber; i++)
            {
                if (mode == CompressionMode.Compress)
                {
                    var compressor = new Thread(Compress);
                    compressor.Name = "compressor" + i.ToString();
                    _compressors.Push(compressor);
                    compressor.Start();
                }
                if (mode == CompressionMode.Decompress)
                {
                    var decompressor = new Thread(Decompress);
                    decompressor.Name = "decompressor" + i.ToString();
                    _compressors.Push(decompressor);
                    decompressor.Start();
                }
            }
            Thread exec = new Thread(Execute);
            exec.Start();
        }


        public void Execute()
        {
            if (this.TaskCount > 0 && this.ResultCount == this.TaskCount)
            {
                // delete all threads
                this.Dispose();
            }
            else
            {
                Thread.Sleep(1000);
            }
        }

        public void AddTask(DataBlock task)
        {
            if (this._tasks.Count > (this.MaxTaskNumber))
            {
                StopRead.Reset();
            }
            // TODO : free-lock Dequeue while Enqueue 
            lock (this._tasks)
            {

                _tasks.Enqueue(task);
            }
        }

        private DataBlock GetTask()
        {
            DataBlock result;
            int tryNumber = 0;
            var dequeueResult = _tasks.TryDequeue(out result);
            while (!dequeueResult)
            {
                tryNumber++;
                var sleepTime = 100 * tryNumber * (new Random()).Next(1, 10);

                Thread.Sleep(sleepTime);
                dequeueResult = _tasks.TryDequeue(out result);
            }
            return result;
        }

        private void Compress()
        {
            var task = GetTask();
            while (task != null)
            {
                try
                {
                    using (MemoryStream input = new MemoryStream(task.Data))
                    {
                        using (MemoryStream output = new MemoryStream(task.Data.Length))
                        {
                            using (GZipStream cs = new GZipStream(output, CompressionMode.Compress))
                            {
                                cs.Write(task.Data, 0, task.Data.Length);
                            }
                            if (!this.AddResult(new DataBlock(task.ID, output.ToArray())))
                            {
                                this.AddTask(task);
                                this.MaxTaskNumber++;
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    GC.Collect();
                    this.AddTask(task);
                    this.MaxTaskNumber++;
                }
                task = GetTask();
            }
        }

        private void Decompress()
        {
            var task = GetTask();
            while (task != null)
            {
                try
                {
                    using (MemoryStream input = new MemoryStream(task.Data))
                    {
                        using (MemoryStream output = new MemoryStream())
                        {
                            using (GZipStream cs = new GZipStream(input, CompressionMode.Decompress))
                            {
                                cs.CopyTo(output);
                            }
                            if (!this.AddResult(new DataBlock(task.ID, output.ToArray())))
                            {
                                this.AddTask(task);
                                this.MaxTaskNumber++;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    GC.Collect();
                    this.AddTask(task);
                    this.MaxTaskNumber++;
                }
                task = GetTask();
            }
        }

        private bool AddResult(DataBlock dataBlock)
        {
            if (this._results.Count > (this.MaxResultNumber / 2))
            {
                StopRead.Reset();
            }

            while (_results.Count > this.MaxResultNumber)
            {
                if (dataBlock.ID < this._results.Keys.Max())
                {
                    //Console.WriteLine("current results count " + this._results.Count);
                    break;
                }
                else
                {
                    return false;
                }

            }
            if (_results.TryAdd(dataBlock.ID, dataBlock.Data))
            {
                this.ResultCount++;
                return true;
            }
            else
            {
                return false;
            }
        }

        public int GetResultsToWrite(long resultSize, int currentDataBlock, out MemoryStream results)
        {
            if ((this._tasks.Count < (this.MaxTaskNumber / 2)))
            {
                StopRead.Set();
            }
            results = new MemoryStream();
            byte[] db;
            var start = 0;
            while ((currentDataBlock < this.TaskCount))
            {
                if (resultSize > results.Length && this._results.TryGetValue(currentDataBlock, out db))
                {
                    results.Seek(start, SeekOrigin.Begin);
                    results.Write(db, 0, db.Length);
                    start += db.Length;
                    while (!this._results.TryRemove(currentDataBlock, out db))
                    {
                        Thread.Sleep(10);
                    }
                    currentDataBlock++;
                }
                else
                    break;
            }

            return currentDataBlock;
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
