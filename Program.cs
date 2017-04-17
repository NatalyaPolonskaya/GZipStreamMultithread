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
        static void Main(string[] args)
        {
            GC.Collect();
            //leaveOpen in GZipStream is true for MemoryStream output and false for File output
            Console.CancelKeyPress += new ConsoleCancelEventHandler(ConsoleCancelHandler);
            args[0] = "compress";
            args[1] = "dd4.txt";
            args[2] = "log";
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
                    var maxBufferSize = 4 * 1024 * 1024;
                    var buffer = new byte[Math.Min(fileFS.Length, maxBufferSize)];           
                    var compressor = new GZipMultithread();
                    int i = 0;

                    long start = 0;
                    int count = fileFS.Read(buffer, 0, buffer.Length);

                    while (count > 0)
                    {
                        compressor.AddTask(new DataBlock(i, buffer));
                        start += count;  
                        fileFS.Seek(start, SeekOrigin.Begin);
                        count = fileFS.Read(buffer, 0, buffer.Length);
                        i++;
                    }
                    Thread.Sleep(10000);

                    while (compressor._tasks.Count > 0)
                    {
                        Thread.Sleep(1000);

                    }
                    fileFS.Seek(0, SeekOrigin.Begin);
                    using (FileStream archiveFS = File.Create(fileName + ".gz"))
                    {
                        var results = compressor._results.OrderBy(o => o.ID);
                        start = 0;
                        foreach (var item in results)
                        {
                            archiveFS.Seek(start, SeekOrigin.Begin);
                            archiveFS.Write(item.Data, 0, item.Data.Length);
                            start += item.Data.Length;
                        }
                        //using (GZipStream compressionStream = new GZipStream(archiveFS,
                        //   CompressionMode.Compress))
                        //{
                        //    //compressionStream.FileName = fileName;
                        //    fileFS.CopyTo(compressionStream);
                        //    archiveFS.Read(buffer, 0, buffer.Length);

                        //}
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

        protected static void ConsoleCancelHandler(object sender, ConsoleCancelEventArgs args)
        {
            Console.WriteLine("CTRL+C pressed. Process aborted.");
            Environment.Exit(0);
        }
    }

}
