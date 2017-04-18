﻿using System;
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
        public ConcurrentStack<Thread> _compressors = new ConcurrentStack<Thread>();


        public ConcurrentQueue<DataBlock> _tasks = new ConcurrentQueue<DataBlock>();
        //private object _tasksQueueLock = new Object();

        public ConcurrentDictionary<int, byte[]> _results = new ConcurrentDictionary<int, byte[]>();

        public bool InputStreamComplete { get; set; }
        public long TaskCount;
        public long ResultCount;

        public int MaxTaskNumber { get; set; }

        public GZipMultithread()
        {
            var maxThreadNumber = Environment.ProcessorCount;
            var taskNumberPerThread = 32;
            this.MaxTaskNumber = taskNumberPerThread * maxThreadNumber;
            this.MaxResultsNumber = this.MaxTaskNumber*2;
            for (int i = 0; i < maxThreadNumber; i++)
            {
                var compressor = new Thread(Compress);
                compressor.Name = "compressor" + i.ToString();
                //compressor.IsBackground = true;
                _compressors.Push(compressor);
                compressor.Start();
            }
            Execute();
            // _writer = new Thread(WriteToFile);
        }

        public void Execute()
        {
            //while (!InputStreamComplete && ResultCount != TaskCount)
            //{
            //    Thread.Sleep(1000);
            //}
            //this.Dispose();

            // delete idle threads?
        }

        public void AddTask(DataBlock task)
        {
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
                //*(new Random()).Next(10)
                var sleepTime = 100 * tryNumber;// *(new Random()).Next(1, 10);
                Console.WriteLine(Thread.CurrentThread.Name + " Task queue wait " + sleepTime);

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
                    //memory = new MemoryStream();
                    //gzip = new GZipStream(memory, CompressionMode.Compress);

                    //archiveFS = File.Create("tmp.gz");
                    //gzip = new GZipStream(archiveFS, CompressionMode.Compress);
                }
                task = GetTask();
            }
        }

        //static byte[] Decompress(byte[] gzip)
        //{
        //    using (GZipStream stream = new GZipStream(new MemoryStream(gzip),
        //        CompressionMode.Decompress))
        //    {
        //        const int size = 4096;
        //        byte[] buffer = new byte[size];
        //        using (MemoryStream memory = new MemoryStream())
        //        {
        //            int count = 0;
        //            do
        //            {
        //                count = stream.Read(buffer, 0, size);
        //                if (count > 0)
        //                {
        //                    memory.Write(buffer, 0, count);
        //                }
        //            }
        //            while (count > 0);
        //            return memory.ToArray();
        //        }
        //    }
        //}

        private bool AddResult(DataBlock dataBlock)
        {
            this.ResultCount++;
            var tryNumber = 0;
            while (_results.Count > this.MaxResultsNumber)
            {
                if (dataBlock.ID < this._results.Keys.Max())
                {
                    Console.WriteLine(Thread.CurrentThread.Name + " Add result overhead ");

                    break;
                }
                else
                {
                    return false;
                }
                //tryNumber++;
                //var sleepTime = 100 * tryNumber * (new Random()).Next(1, 10);
                //Console.WriteLine(Thread.CurrentThread.Name + " Add result wait " + sleepTime);

                //Thread.Sleep(sleepTime);
            }
            Console.WriteLine(Thread.CurrentThread.Name + " Add result normal ");

            return _results.TryAdd(dataBlock.ID, dataBlock.Data);
        }

        public void Dispose()
        {
            foreach (var item in this._compressors)
            {
                item.Abort();
            }
        }

        public int MaxResultsNumber { get; set; }
    }
}
