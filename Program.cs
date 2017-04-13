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
            //leaveOpen in GZipStream is true for MemoryStream output and false for File output
            args[0] = "compress";
            args[1] = "@C:\\tmp\\opencv-2.4.9.exe";
            args[2] = "@C:\\tmp\\opencv-2.4.9";
            //args[1] = "C:\\tmp\\MyTest.txt";
            //args[2] = "C:\\tmp\\MyTest";
            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "compress":
                        {
                            Console.WriteLine("compress");

                            try
                            {
                                var fileName = args[1];
                                var archiveName = args[2];
                                using (FileStream fileFS = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                                {
                                    using (FileStream archiveFS = File.Create(archiveName + ".gz"))
                                    {
                                        using (GZipStream compressionStream = new GZipStream(archiveFS,
                                           CompressionMode.Compress))
                                        {
                                            fileFS.CopyToAsync(compressionStream);
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Exception code: " + ex.HResult + " with message: " + ex.Message);
                            }
                        }
                        break;
                    case "decompress":
                        {
                            Console.WriteLine("decompress");
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
            Console.ReadLine();
        }
    }
}
