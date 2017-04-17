using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GZipStreamMultithread
{
    class Program
    {
        static string FileName = string.Empty;

        static void Main(string[] args)
        {
            GC.Collect();
            Thread.CurrentThread.Priority = ThreadPriority.Lowest;
            //leaveOpen in GZipStream is true for MemoryStream output and false for File output
            Console.CancelKeyPress += new ConsoleCancelEventHandler(ConsoleCancelHandler);
            args[0] = "compress";
            args[1] = "dd3.zip";
            //args[1] = "test6.tar";
           
            //args[1] = "D:\\gzipExperiment\\dd2.zip";
            //args[1] = "dd.txt";
            args[2] = "log";
            FileName = args[1];

            DateTime start = DateTime.Now;
            Console.WriteLine("Timestamp start " + start);
            //args[1] = "C:\\tmp\\MyTest.txt";
            //args[2] = "C:\\tmp\\MyTest";
            //Console.WriteLine(Environment.ProcessorCount);//4
            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "compress":
                        {
                            Console.WriteLine("compress");

                            Compress(args);
                        }
                        break;
                    case "decompress":
                        {
                            Console.WriteLine("decompress");
                            Decompress(args);
                        }
                        break;
                    default:
                        Console.WriteLine("No such parameter");
                        break;
                }
            }
            else
            {
                Console.WriteLine("Not enough parameters");
            }
            DateTime end = DateTime.Now;
            Console.WriteLine("Timestamp end " + end);
            Console.WriteLine("Timelapse " + (end-start).Duration());
            Console.ReadLine();
        }

        private static void Decompress(string[] args)
        {
            try
            {
                var fileName = args[1];
                var archiveName = args[2];
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
            }
        }

        private static void Compress(string[] args)
        {
            try
            {
                var fileName = args[1];
                var archiveName = args[2];
                //using (FileStream fileFS = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                using (FileStream fileFS = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
                    var maxBufferSize = 128 * 1024;//1024*1024;// 4 * 1024 * 1024;
                    var buffer = new byte[Math.Min(fileFS.Length, maxBufferSize)];           
                    var compressor = new GZipMultithread();
                    int i = 0;

                    long start = 0;
                    int count = fileFS.Read(buffer, 0, buffer.Length);
                    Thread writer = new Thread(WriteToFile);
                    writer.Start(compressor);
                    var maxTaskNumber = 64;
                    while (count > 0)
                    {
                        if (compressor._tasks.Count > maxTaskNumber)
                        {
                            Thread.Sleep(10);
                        }
                        else
                        {
                            compressor.AddTask(new DataBlock(i, buffer));
                            compressor.TaskCount++;
                            start += count;
                            fileFS.Seek(start, SeekOrigin.Begin);
                            buffer = new byte[Math.Min(fileFS.Length - start, maxBufferSize)];
                            count = fileFS.Read(buffer, 0, buffer.Length);
                            i++;
                        }
                    }
                    while (writer.ThreadState!=ThreadState.Stopped)
                    {
                        Thread.Sleep(1000);
                    }
                    compressor.Dispose();
                }

                GC.Collect();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
            }
        }

        private static void WriteToFile( object compressorObj)
        {
            GZipMultithread compressor = compressorObj as GZipMultithread;
            string fileName = FileName;
            using (FileStream archiveFS = File.Create(fileName + ".gz"))
            {
                long start =0;
                var currentSegment = 0;
                var tryNumber = 0;
                //compressor.InputStreamComplete && compressor.ResultCount != compressor.TaskCount
                while (currentSegment != compressor.TaskCount && compressor.TaskCount!=0)
                {
                    byte[] db;

                    if (compressor._results.TryRemove(currentSegment, out db))
                    {
                        archiveFS.Seek(start, SeekOrigin.Begin);
                        archiveFS.Write(db, 0, db.Length);
                        start += db.Length;
                        currentSegment++;
                    }
                    else
                    {
                        tryNumber++;
                        Thread.Sleep(100 * tryNumber);
                    }
                }
            }
            Thread.CurrentThread.Abort();
        }

        protected static void ConsoleCancelHandler(object sender, ConsoleCancelEventArgs args)
        {
            Console.WriteLine("CTRL+C pressed. Process aborted.");
            Environment.Exit(0);
        }
    }

}
