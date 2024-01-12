﻿using System;
using System.Threading.Tasks;
using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace SQSConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddSource("MyCompany.MyProduct.MyLibrary")
            .AddAWSInstrumentation()
            .Build();

            SQSMessageConsumer sqsConsumer = new SQSMessageConsumer();
                       
            await sqsConsumer.Listen();           
                       
        }
    }
}